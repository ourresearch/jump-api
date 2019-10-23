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
from util import elapsed

from app import app
from app import get_db_cursor
from app import logger
from util import jsonify_fast
from util import str2bool




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
    use_cache = str2bool(request.args.get("use_cache", "false"))
    package = request.args.get("package", "demo")
    if package == "demo":
        package = "uva_elsevier"
    min_arg = request.args.get("min", None)

    if use_cache:
        jump_response = jump_cache[package]
    else:
        jump_response = get_jump_response(package, min_arg)

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


@app.route("/jump/temp", methods=["GET"])
def jump_get():
    use_cache = str2bool(request.args.get("use_cache", "false"))
    package = request.args.get("package", "demo")
    if package == "demo":
        package = "uva_elsevier"
    min_arg = request.args.get("min", None)

    if use_cache:
        global jump_cache
        return jsonify_fast(jump_cache[package])
    else:
        return jsonify_fast(get_jump_response(package, min_arg))

# observation_year 	total views 	total views percent of 2018 	total oa views 	total oa views percent of 2018
# 2018 	25,565,054.38 	1.00 	12,664,693.62 	1.00
# 2019 	28,162,423.76 	1.10 	14,731,000.96 	1.16
# 2020 	30,944,070.68 	1.21 	17,033,520.59 	1.34
# 2021 	34,222,756.60 	1.34 	19,830,049.25 	1.57
# 2022 	38,000,898.80 	1.49 	23,092,284.75 	1.82
# 2023 	42,304,671.82 	1.65 	26,895,794.03 	2.12


def get_jump_response(package="mit_elsevier", min_arg=None):
    timing = []

    start_time = time()
    section_time = time()

    package_issn_ls = get_issn_ls_for_package(package)

    command = "select * from counter where package='{}'".format(package)
    counter_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        counter_rows = cursor.fetchall()
    counter_dict = dict((a["issn_l"], a["total"]) for a in counter_rows)

    command = "select * from journal_delayed_oa_active"
    embargo_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        embargo_rows = cursor.fetchall()
    embargo_dict = dict((a["issn_l"], int(a["embargo"])) for a in embargo_rows)

    command = """select cites.journal_issn_l, sum(num_citations) as num_citations_2018
        from ricks_temp_num_cites_by_uva cites
        join unpaywall u on u.doi=cites.doi
        join unpaywall_journals_package_issnl_view package on package.issn_l=cites.journal_issn_l
        where year = 2018
        and u.publisher ilike 'elsevier%'
        and package = '{}'
        group by cites.journal_issn_l""".format(package)
    citation_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        citation_rows = cursor.fetchall()
    citation_dict = dict((a["journal_issn_l"], a["num_citations_2018"]) for a in citation_rows)

    command = """select u.journal_issn_l as journal_issn_l, count(u.doi) as num_authorships
        from unpaywall u 
        join ricks_affiliation affil on u.doi = affil.doi
        where affil.org = 'University of Virginia'
        and u.year = 2018
        group by u.journal_issn_l""".format(package)
    authorship_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        authorship_rows = cursor.fetchall()
    authorship_dict = dict((a["journal_issn_l"], a["num_authorships"]) for a in authorship_rows)


    command = "select * from jump_elsevier_unpaywall_downloads"
    jump_elsevier_unpaywall_downloads_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        jump_elsevier_unpaywall_downloads_rows = cursor.fetchall()

    timing.append(("time from db", elapsed(section_time, 2)))
    section_time = time()

    rows_to_export = []
    summary_dict = {}
    summary_dict["year"] = [2020 + projected_year for projected_year in range(0, 5)]
    for field in ["total", "oa", "researchgate", "back_catalog", "turnaways"]:
        summary_dict[field] = [0 for projected_year in range(0, 5)]

    timing.append(("summary", elapsed(section_time, 2)))
    section_time = time()

    for row in jump_elsevier_unpaywall_downloads_rows:
        if package and row["issn_l"] not in package_issn_ls:
            continue

        my_dict = {}
        for field in row.keys():
            if not row[field]:
                row[field] = 0

        for field in ["issn_l", "title", "subject", "publisher"]:
            my_dict[field] = row[field]
        my_dict["papers_2018"] = row["num_papers_2018"]
        my_dict["citations_from_mit_in_2018"] = citation_dict.get(my_dict["issn_l"], 0)
        my_dict["num_citations"] = citation_dict.get(my_dict["issn_l"], 0)
        my_dict["num_authorships"] = authorship_dict.get(my_dict["issn_l"], 0)
        my_dict["oa_embargo_months"] = embargo_dict.get(my_dict["issn_l"], None)

        my_dict["downloads_by_year"] = {}
        my_dict["downloads_by_year"]["year"] = [2020 + projected_year for projected_year in range(0, 5)]

        oa_recall_scaling_factor = 1.3
        researchgate_proportion_of_downloads = 0.1
        growth_scaling = {}
        growth_scaling["downloads"] =   [1.10, 1.21, 1.34, 1.49, 1.65]
        growth_scaling["oa"] =          [1.16, 1.24, 1.57, 1.83, 2.12]
        my_dict["downloads_by_year"]["total"] = [row["downloads_total"]*growth_scaling["downloads"][year] for year in range(0, 5)]
        my_dict["downloads_by_year"]["oa"] = [int(oa_recall_scaling_factor * row["downloads_total_oa"] * growth_scaling["oa"][year]) for year in range(0, 5)]

        my_dict["downloads_by_year"]["oa"] = [min(a, b) for a, b in zip(my_dict["downloads_by_year"]["total"], my_dict["downloads_by_year"]["oa"])]

        my_dict["downloads_by_year"]["researchgate"] = [int(researchgate_proportion_of_downloads * my_dict["downloads_by_year"]["total"][projected_year]) for projected_year in range(0, 5)]
        my_dict["downloads_by_year"]["researchgate_orig"] = my_dict["downloads_by_year"]["researchgate"]

        total_downloads_by_age = [row["downloads_{}y".format(age)] for age in range(0, 5)]
        oa_downloads_by_age = [row["downloads_{}y_oa".format(age)] for age in range(0, 5)]

        my_dict["downloads_by_year"]["turnaways"] = [0 for year in range(0, 5)]
        for year in range(0,5):
            my_dict["downloads_by_year"]["turnaways"][year] = (1 - researchgate_proportion_of_downloads) *\
                sum([(total_downloads_by_age[age]*growth_scaling["downloads"][year] - oa_downloads_by_age[age]*growth_scaling["oa"][year])
                     for age in range(0, year+1)])
        my_dict["downloads_by_year"]["turnaways"] = [max(0, num) for num in my_dict["downloads_by_year"]["turnaways"]]

        my_dict["downloads_by_year"]["oa"] = [min(my_dict["downloads_by_year"]["total"][year] - my_dict["downloads_by_year"]["turnaways"][year], my_dict["downloads_by_year"]["oa"][year]) for year in range(0,5)]

        my_dict["downloads_by_year"]["back_catalog"] = [my_dict["downloads_by_year"]["total"][projected_year]\
                                                        - (my_dict["downloads_by_year"]["turnaways"][projected_year]
                                                           + my_dict["downloads_by_year"]["oa"][projected_year]
                                                           + my_dict["downloads_by_year"]["researchgate"][projected_year])\
                                                        for projected_year in range(0, 5)]
        my_dict["downloads_by_year"]["back_catalog"] = [max(0, num) for num in my_dict["downloads_by_year"]["back_catalog"]]


        # now scale for the org
        try:
            total_org_downloads = counter_dict[row["issn_l"]]
            total_org_downloads_multiple = total_org_downloads / row["downloads_total"]
        except:
            total_org_downloads_multiple = 0

        for field in ["total", "oa", "researchgate", "back_catalog", "turnaways"]:
            for projected_year in range(0, 5):
                my_dict["downloads_by_year"][field][projected_year] *= float(total_org_downloads_multiple)
                my_dict["downloads_by_year"][field][projected_year] = int(my_dict["downloads_by_year"][field][projected_year])


        for field in ["total", "oa", "researchgate", "back_catalog", "turnaways"]:
            for projected_year in range(0, 5):
                summary_dict[field][projected_year] += my_dict["downloads_by_year"][field][projected_year]

        if min_arg:
            del my_dict["downloads_by_year"]

        my_dict["dollars_2018_subscription"] = float(row["usa_usd"])
        rows_to_export.append(my_dict)

    timing.append(("loop", elapsed(section_time, 2)))
    section_time = time()

    sorted_rows = sorted(rows_to_export, key=lambda x: x["downloads_by_year"]["total"][0], reverse=True)
    timing.append(("after sort", elapsed(section_time, 2)))

    timing_messages = ["{}: {}s".format(*item) for item in timing]
    return {"_timing": timing_messages, "list": sorted_rows, "total": summary_dict, "count": len(sorted_rows)}

# jump_cache = {}
# store_cache = True
# if store_cache:
#     print "building cache"
#     for package in ["cdl_elsevier", "mit_elsevier", "uva_elsevier"]:
#         print package
#         jump_cache[package] = get_jump_response(package)
#         pickle.dump(jump_cache, open( "data/jump_cache.pkl", "wb" ), -1)
#     print "done"
# else:
#     print "loading cache"
#     jump_cache = pickle.load(open( "data/jump_cache.pkl", "rb" ))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5004))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)

















