from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify

import json
import os
import sys
from time import time
import pickle
import numpy as np
from util import elapsed

from app import app
from app import get_db_cursor
from app import logger
from app import mycache
from util import jsonify_fast
from util import str2bool
from journal import Journal
from journal_list import JournalList
from assumptions import Assumptions



def json_dumper(obj):
    """
    if the obj has a to_dict() function we've implemented, uses it to get dict.
    from http://stackoverflow.com/a/28174796
    """
    try:
        return obj.to_dict()
    except AttributeError:
        return obj.__dict__


def json_resp(thing):
    json_str = json.dumps(thing, sort_keys=True, default=json_dumper, indent=4)

    if request.path.endswith(".json") and (os.getenv("FLASK_DEBUG", False) == "True"):
        logger.info(u"rendering output through debug_api.html template")
        resp = make_response(render_template(
            'debug_api.html',
            data=json_str))
        resp.mimetype = "text/html"
    else:
        resp = make_response(json_str, 200)
        resp.mimetype = "application/json"
    return resp


def abort_json(status_code, msg):
    body_dict = {
        "HTTP_status_code": status_code,
        "message": msg,
        "error": True
    }
    resp_string = json.dumps(body_dict, sort_keys=True, indent=4)
    resp = make_response(resp_string, status_code)
    resp.mimetype = "application/json"
    abort(resp)



@app.after_request
def after_request_stuff(resp):

    #support CORS
    resp.headers['Access-Control-Allow-Origin'] = "*"
    resp.headers['Access-Control-Allow-Methods'] = "POST, GET, OPTIONS, PUT, DELETE, PATCH"
    resp.headers['Access-Control-Allow-Headers'] = "origin, content-type, accept, x-requested-with"

    # # remove session
    # db.session.remove()

    # without this jason's heroku local buffers forever
    sys.stdout.flush()

    return resp



@app.route('/', methods=["GET", "POST"])
def base_endpoint():
    return jsonify_fast({
        "version": "0.0.1",
        "msg": "Don't panic"
    })

@app.route("/jump/temp/package/<package>", methods=["GET"])
def jump_package_get(package):
    command = """select issn_l, journal_name from unpaywall_journals_package_issnl_view where package='{}'""".format(package)

    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()

    return jsonify({"list": rows, "count": len(rows)})


@app.route("/jump/temp/issn/<issn_l>", methods=["GET"])
def jump_issn_get(issn_l):
    package = request.args.get("package", "demo")
    if package == "demo":
        package = "uva_elsevier"

    jump_response = get_jump_response(package)

    journal_dicts = jump_response["list"]
    issnl_dict = filter(lambda my_dict: my_dict['issn_l'] == issn_l, journal_dicts)[0]

    command = """select year, oa_status, count(*) as num_articles from unpaywall 
    where journal_issn_l = '{}'
    and year > 2015
    group by year, oa_status""".format(issn_l)

    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    for row in rows:
        row["year"] = int(row["year"])

    issnl_dict["oa_status"] = rows

    return jsonify(issnl_dict)


def get_issn_ls_for_package(package):
    command = "select issn_l from unpaywall_journals_package_issnl_view"
    if package:
        command += " where package='{}'".format(package)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    package_issn_ls = [row["issn_l"] for row in rows]
    return package_issn_ls

def get_settings():
    settings = Assumptions()

    for key in request.args:
        try:
            settings.set_assumption(key, float(request.args.get(key)))
        except ValueError:
            settings.set_assumption(key, request.args.get(key))

    settings.package = request.args.get("package", None)  # so get demo if that's what was used
    return settings

def do_wizardly_things(jump_details, spend):
    journal_list = JournalList(jump_details["journals_list"], get_settings())

    my_max = spend/100.0 * journal_list.settings.bigdeal_cost
    my_spend_so_far = 0
    for journal in journal_list.journals_by_value:
        my_spend_so_far += journal.subscription_price_average
        if my_spend_so_far > my_max:
            return journal_list
        journal.set_subscribe()

    return journal_list


@app.route("/jump/wizard", methods=["GET"])
def jump_wizard_get():
    package = request.args.get("package", "demo")
    if package == "demo":
        package = "uva_elsevier"
    spend = int(request.args.get("spend", 0))
    jump_details = get_jump_response(package, max_journals=100000000)
    journal_list = do_wizardly_things(jump_details, spend)
    response = {
        "_summary": {
            "cost": journal_list.cost,
            "num_journals_subscribed": len(journal_list.subscribed),
            "num_journals_total": len(journal_list.journals)
        },
        "journals": [j.to_dict() for j in journal_list.journals],
        "_settings": journal_list.settings.to_dict()
    }
    return jsonify_fast(response)


@app.route("/jump/temp", methods=["GET"])
def jump_get():
    package = request.args.get("package", "demo")
    if package == "demo":
        package = "uva_elsevier"

    return jsonify_fast(get_jump_response(package))


# observation_year 	total views 	total views percent of 2018 	total oa views 	total oa views percent of 2018
# 2018 	25,565,054.38 	1.00 	12,664,693.62 	1.00
# 2019 	28,162,423.76 	1.10 	14,731,000.96 	1.16
# 2020 	30,944,070.68 	1.21 	17,033,520.59 	1.34
# 2021 	34,222,756.60 	1.34 	19,830,049.25 	1.57
# 2022 	38,000,898.80 	1.49 	23,092,284.75 	1.82
# 2023 	42,304,671.82 	1.65 	26,895,794.03 	2.12


@mycache.cache
def get_data_from_db(package):


    timing = []
    section_time = time()

    package_issn_ls = get_issn_ls_for_package(package)

    command = "select issn_l, total from jump_counter where package='{}'".format(package)
    counter_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        counter_rows = cursor.fetchall()
    counter_dict = dict((a["issn_l"], a["total"]) for a in counter_rows)

    timing.append(("time from db: counter", elapsed(section_time, 2)))
    section_time = time()

    command = "select issn_l, embargo from journal_delayed_oa_active"
    embargo_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        embargo_rows = cursor.fetchall()
    embargo_dict = dict((a["issn_l"], int(a["embargo"])) for a in embargo_rows)

    timing.append(("time from db: journal_delayed_oa_active", elapsed(section_time, 2)))
    section_time = time()

    command = """select issn_l, num_citations
        from jump_citing_2018
        where citing_org = 'University of Virginia'""".format(package)
    citation_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        citation_rows = cursor.fetchall()
    citation_dict = dict((a["issn_l"], a["num_citations"]) for a in citation_rows)

    timing.append(("time from db: citation_rows", elapsed(section_time, 2)))
    section_time = time()

    command = """select issn_l as journal_issn_l, num_authorships
        from jump_authorship_2018
        where org = 'University of Virginia'""".format(package)
    authorship_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        authorship_rows = cursor.fetchall()
    authorship_dict = dict((a["journal_issn_l"], a["num_authorships"]) for a in authorship_rows)

    timing.append(("time from db: authorship_rows", elapsed(section_time, 2)))
    section_time = time()


    command = "select * from jump_elsevier_unpaywall_downloads"
    jump_elsevier_unpaywall_downloads_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        jump_elsevier_unpaywall_downloads_rows = cursor.fetchall()

    timing.append(("time from db: download_rows", elapsed(section_time, 2)))
    section_time = time()

    response = {
        "timing": timing,
        "package_issn_ls": package_issn_ls,
        "counter_dict": counter_dict,
        "embargo_dict": embargo_dict,
        "citation_dict": citation_dict,
        "authorship_dict": authorship_dict,
        "jump_elsevier_unpaywall_downloads_rows": jump_elsevier_unpaywall_downloads_rows
    }

    return response



def get_jump_response(package="mit_elsevier", max_journals=100):
    timing = []

    start_time = time()
    section_time = time()

    settings = get_settings()

    data = get_data_from_db(package)
    # timing += data["timing"]

    timing.append(("total db/cache time", elapsed(section_time, 2)))
    section_time = time()

    rows_to_export = []
    summary_unweighted_usage = {}
    summary_unweighted_usage["year"] = [2020 + projected_year for projected_year in range(0, 5)]
    for field in ["total", "oa", "social_networks", "backfile", "notfreenotinstant", "ill", "other_delayed"]:
        summary_unweighted_usage[field] = [0 for projected_year in range(0, 5)]
    summary_weighted_usage = summary_unweighted_usage

    timing.append(("calc summary", elapsed(section_time, 2)))
    section_time = time()

    for row in data["jump_elsevier_unpaywall_downloads_rows"]:
        if package and row["issn_l"] not in data["package_issn_ls"]:
            continue

        my_dict = {}
        for field in row.keys():
            if not row[field]:
                row[field] = 0

        for field in ["issn_l", "title", "subject", "publisher"]:
            my_dict[field] = row[field]
        my_dict["papers_2018"] = row["num_papers_2018"]
        my_dict["num_citations"] = data["citation_dict"].get(my_dict["issn_l"], 0)
        my_dict["num_authorships"] = data["authorship_dict"].get(my_dict["issn_l"], 0)
        my_dict["oa_embargo_months"] = data["embargo_dict"].get(my_dict["issn_l"], None)

        my_dict["unweighted_usage"] = {}
        my_dict["unweighted_usage"]["year"] = [2020 + projected_year for projected_year in range(0, 5)]

        my_dict["weighted_usage"] = {}
        my_dict["weighted_usage"]["year"] = [2020 + projected_year for projected_year in range(0, 5)]

        oa_recall_scaling_factor = 1.3
        social_networks_proportion_of_downloads = 0.1
        growth_scaling = {}
        growth_scaling["downloads"] =   [1.10, 1.21, 1.34, 1.49, 1.65]
        growth_scaling["oa"] =          [1.16, 1.24, 1.57, 1.83, 2.12]
        my_dict["unweighted_usage"]["total"] = [row["downloads_total"]*growth_scaling["downloads"][year] for year in range(0, 5)]
        my_dict["unweighted_usage"]["oa"] = [int(oa_recall_scaling_factor * row["downloads_total_oa"] * growth_scaling["oa"][year]) for year in range(0, 5)]

        my_dict["unweighted_usage"]["oa"] = [min(a, b) for a, b in zip(my_dict["unweighted_usage"]["total"], my_dict["unweighted_usage"]["oa"])]

        my_dict["unweighted_usage"]["social_networks"] = [int(social_networks_proportion_of_downloads * my_dict["unweighted_usage"]["total"][projected_year]) for projected_year in range(0, 5)]

        total_downloads_by_age = [row["downloads_{}y".format(age)] for age in range(0, 5)]
        oa_downloads_by_age = [row["downloads_{}y_oa".format(age)] for age in range(0, 5)]

        my_dict["unweighted_usage"]["notfreenotinstant"] = [0 for year in range(0, 5)]
        for year in range(0,5):
            my_dict["unweighted_usage"]["notfreenotinstant"][year] = (1 - social_networks_proportion_of_downloads) *\
                sum([(total_downloads_by_age[age]*growth_scaling["downloads"][year] - oa_downloads_by_age[age]*growth_scaling["oa"][year])
                     for age in range(0, year+1)])
        my_dict["unweighted_usage"]["notfreenotinstant"] = [max(0, num) for num in my_dict["unweighted_usage"]["notfreenotinstant"]]

        my_dict["unweighted_usage"]["oa"] = [min(my_dict["unweighted_usage"]["total"][year] - my_dict["unweighted_usage"]["notfreenotinstant"][year], my_dict["unweighted_usage"]["oa"][year]) for year in range(0,5)]

        my_dict["unweighted_usage"]["backfile"] = [my_dict["unweighted_usage"]["total"][projected_year]\
                                                        - (my_dict["unweighted_usage"]["notfreenotinstant"][projected_year]
                                                           + my_dict["unweighted_usage"]["oa"][projected_year]
                                                           + my_dict["unweighted_usage"]["social_networks"][projected_year])\
                                                        for projected_year in range(0, 5)]
        my_dict["unweighted_usage"]["backfile"] = [max(0, num) for num in my_dict["unweighted_usage"]["backfile"]]

        my_dict["unweighted_usage"]["ill"] = [int(notfreenotinstant*settings.ill_request_percent) for notfreenotinstant in my_dict["unweighted_usage"]["notfreenotinstant"]]
        my_dict["unweighted_usage"]["other_delayed"] = [my_dict["unweighted_usage"]["notfreenotinstant"][year] - my_dict["unweighted_usage"]["ill"][year] for year in range(0, 5)]

        # now scale for the org
        try:
            total_org_downloads = data["counter_dict"][row["issn_l"]]
            total_org_downloads_multiple = total_org_downloads / row["downloads_total"]
        except:
            total_org_downloads_multiple = 0

        for field in ["total", "oa", "social_networks", "backfile", "notfreenotinstant", "ill", "other_delayed"]:
            for projected_year in range(0, 5):
                my_dict["unweighted_usage"][field][projected_year] *= float(total_org_downloads_multiple)
                my_dict["unweighted_usage"][field][projected_year] = int(my_dict["unweighted_usage"][field][projected_year])


        for field in ["total", "oa", "social_networks", "backfile", "notfreenotinstant", "ill", "other_delayed"]:
            for projected_year in range(0, 5):
                summary_unweighted_usage[field][projected_year] += my_dict["unweighted_usage"][field][projected_year]
                summary_weighted_usage[field] = summary_unweighted_usage[field]

        my_dict["dollars_2018_subscription"] = float(row["usa_usd"])

        rows_to_export.append(my_dict)

    annual_unweighted_usage = {}
    annual_weighted_usage = {}
    for field in ["total", "oa", "social_networks", "backfile", "ill", "other_delayed"]:
        annual_unweighted_usage[field] = int(np.mean(summary_unweighted_usage[field]))
        annual_weighted_usage[field] = int(np.mean(summary_weighted_usage[field]))  # + my_dict["num_citations"] * settings.weight_citations

    annual_price = {}
    for field in ["total", "oa", "social_networks", "backfile", "other_delayed"]:
        annual_price[field] = 0
    annual_price["ill"] = int(annual_unweighted_usage["ill"] * settings.ill_cost)

    timing.append(("loop", elapsed(section_time, 2)))
    section_time = time()

    sorted_rows = sorted(rows_to_export, key=lambda x: x["unweighted_usage"]["total"][0], reverse=True)
    timing.append(("after sort", elapsed(section_time, 2)))
    section_time = time()

    timing.append(("total time", elapsed(start_time, 2)))
    section_time = time()

    timing_messages = ["{}: {}s".format(*item) for item in timing]
    return {"_timing": timing_messages,
            "_settings": settings.to_dict(),
            "journals_list": sorted_rows[0:max_journals],
            "by_year": {"unweighted_usage": summary_unweighted_usage, "weighted_usage": summary_weighted_usage},
            "annual": {"unweighted_usage": annual_unweighted_usage, "weighted_usage": annual_weighted_usage, "price": annual_price},
            "journals_count": len(sorted_rows)}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5004))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)

















