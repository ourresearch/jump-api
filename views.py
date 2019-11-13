# coding: utf-8

from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify
from flask import url_for
from flask import Response
from flask import send_file
from flask_jwt_extended import jwt_required, jwt_optional, create_access_token, get_jwt_identity

import simplejson as json
import os
import sys
from time import time
import unicodecsv as csv
from app import app
from app import logger
from app import jwt
from scenario import Scenario
from util import jsonify_fast
from util import jsonify_fast_no_sort
from util import str2bool
from util import elapsed

def get_clean_package(http_request_args):
    return "uva_elsevier"
    # package = http_request_args.get("package", "demo")
    # if package == "demo":
    #     package = "uva_elsevier"
    # return package

# warm the cache
print "warming the cache"
start_time = time()
Scenario(get_clean_package(None))
print "done, took {} seconds".format(elapsed(start_time, 2))

@app.after_request
def after_request_stuff(resp):
    sys.stdout.flush()  # without this jason's heroku local buffers forever
    #support CORS
    resp.headers['Access-Control-Allow-Origin'] = "*"
    resp.headers['Access-Control-Allow-Methods'] = "POST, GET, OPTIONS, PUT, DELETE, PATCH"
    resp.headers['Access-Control-Allow-Headers'] = "origin, content-type, accept, x-requested-with"
    return resp


@app.route('/', methods=["GET", "POST", "POST"])
def base_endpoint():
    return jsonify_fast({
        "version": "0.0.1",
        "msg": "Don't panic"
    })

# @app.route('/favicon.ico')
# def favicon():
#     return redirect(url_for("static", filename="img/favicon.ico", _external=True, _scheme='https'))

@app.route("/scenario/wizard", methods=["GET", "POST"])
def jump_wizard_get():
    pagesize = int(request.args.get("pagesize", 100))
    spend = round(request.args.get("spend", 0))
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    scenario.do_wizardly_things(spend)
    return jsonify_fast(scenario.to_dict(pagesize))


@app.route("/scenario/summary", methods=["GET", "POST"])
def jump_summary_get():
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast(scenario.to_dict_summary())

@app.route("/scenario/journals", methods=["GET", "POST"])
@app.route("/scenario/overview", methods=["GET", "POST"])
def jump_overview_get():
    pagesize = int(request.args.get("pagesize", 4000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_overview(pagesize))

@app.route("/scenario/table", methods=["GET", "POST"])
def jump_table_get():
    pagesize = int(request.args.get("pagesize", 4000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_table(pagesize))

@app.route("/scenario", methods=["GET", "POST"])
@app.route("/scenario/slider", methods=["GET", "POST"])
def jump_slider_get():
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_slider())

@app.route("/scenario/timeline", methods=["GET", "POST"])
def jump_timeline_get():
    pagesize = int(request.args.get("pagesize", 4000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_timeline(pagesize))

@app.route("/scenario/apc", methods=["GET", "POST"])
def jump_apc_get():
    pagesize = int(request.args.get("pagesize", 4000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_apc(pagesize))

@app.route("/scenario/costs", methods=["GET", "POST"])
def jump_costs_get():
    pagesize = int(request.args.get("pagesize", 4000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_cost(pagesize))

@app.route("/scenario/oa", methods=["GET", "POST"])
def jump_oa_get():
    pagesize = int(request.args.get("pagesize", 4000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_oa(pagesize))


@app.route("/scenario/fulfillment", methods=["GET", "POST"])
def jump_fulfillment_get():
    pagesize = int(request.args.get("pagesize", 4000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_fulfillment(pagesize))

@app.route("/scenario/report", methods=["GET", "POST"])
def jump_report_get():
    pagesize = int(request.args.get("pagesize", 4000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_report(pagesize))

@app.route("/scenario/impact", methods=["GET", "POST"])
def jump_impact_get():
    pagesize = int(request.args.get("pagesize", 4000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_impact(pagesize))


@app.route("/journal/issn_l/<issn_l>", methods=["GET", "POST"])
def jump_issn_get(issn_l):
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    my_journal = scenario.get_journal(issn_l)
    return jsonify_fast_no_sort({"_settings": scenario.settings.to_dict(), "journal": my_journal.to_dict_details()})


@app.route("/scenario/export.csv", methods=["GET"])
def jump_export_csv():
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)

    filename = "export.csv"
    with open(filename, "w") as file:
        csv_file = csv.writer(file, encoding='utf-8')
        keys = ["issn_l", "title", "subscribed"]
        csv_file.writerow(keys)
        for journal in scenario.journals:
            # doing this hacky thing so excel doesn't format the issn as a date :(
            csv_file.writerow(["issn:{}".format(journal.issn_l), journal.title, journal.subscribed])

    with open(filename, "r") as file:
        contents = file.readlines()

    # return Response(contents, mimetype="text/text")
    return Response(contents, mimetype="text/csv")



# Provide a method to create access tokens. The create_access_token()
# function is used to actually generate the token, and you can return
# it to the caller however you choose.
@app.route('/login', methods=['POST'])
def login():
    if not request.is_json:
        return jsonify({"msg": "Missing JSON in request"}), 400

    username = request.json.get('username', None)
    password = request.json.get('password', None)

    print "in login"
    print "username", username
    print "password", password

    if not username:
        return jsonify({"msg": "Missing username parameter"}), 400
    if not password:
        return jsonify({"msg": "Missing password parameter"}), 400

    if password != 'password':
        return jsonify({"msg": "Bad username or password"}), 401

    my_identity = {
        "id": username,
        "name": "User {}".format(username.title()),
        "package": get_clean_package(username)
    }
    # Identity can be any data that is json serializable
    access_token = create_access_token(identity=my_identity)
    print "access_token", access_token

    return jsonify(access_token=access_token), 200


# TOKEN=$(curl -s -X POST -H 'Accept: application/json' -H 'Content-Type: application/json' --data '{"username":"test","password":"test","rememberMe":false}' http://localhost:5004/login | jq -r '.id_token')
#curl -H 'Accept: application/json' -H "Authorization: Bearer ${TOKEN}" http://localhost:5004/protected

# Protect a view with jwt_required, which requires a valid access token
# in the request to access.
@app.route('/protected', methods=['GET'])
@jwt_required
def protected():
    # Access the identity of the current user with get_jwt_identity
    current_user = get_jwt_identity()
    return jsonify(logged_in_as=current_user["id"]), 200

@app.route('/account', methods=['GET'])
@jwt_optional
def account_get():
    demo_package = get_clean_package(None)
    current_user = get_jwt_identity()
    if not current_user:
        current_user = {
            "id": "demo",
            "name": "Demo User",
            "package": get_clean_package(None)
        }

    scenario = Scenario(current_user["package"])

    account_dict = {
        "id": current_user["id"],
        "name": current_user["name"],
        "packages": [{
                "id": "demo-pkg-123",
                "name": "my Elsevier Freedom Package",
                "hasCounterData": len(scenario.journals) > 0,
                "numJournals": len(scenario.journals),
                "numPerpAccessJournals": len(scenario.journals)
            }],
        "scenarios": [{
            "id": "1",
            "name": "My First Scenario",
            "pkgId": "demo-pkg-123",
            "summary": {
                "cost_percent": scenario.cost_spent_percent,
                "use_instant_percent": scenario.use_instant_percent,
                "num_journals_subscribed": len(scenario.subscribed),
            },
            "subrs": [],
            "customSubrs": [],
            "configs": scenario.settings.to_dict()
        }]
    }
    return jsonify_fast(account_dict)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5004))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)

















