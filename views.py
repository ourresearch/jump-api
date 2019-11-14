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
from werkzeug.security import safe_str_cmp
from werkzeug.security import generate_password_hash, check_password_hash

import simplejson as json
import os
import sys
from time import time
import unicodecsv as csv
from app import app
from app import logger
from app import jwt
from app import db
from scenario import Scenario
from account import Account
from package import Package
from saved_scenario import SavedScenario
from util import jsonify_fast
from util import jsonify_fast_no_sort
from util import str2bool
from util import elapsed
from util import abort_json
from util import safe_commit
from util import TimingMessages

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
    resp.headers['Access-Control-Allow-Headers'] = "Origin, X-Requested-With, Content-Type, Accept, Authorization"
    resp.headers['Access-Control-Expose-Headers'] = "Authorization"
    resp.headers['Access-Control-Allow-Credentials'] = "true"
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
    pagesize = int(request.args.get("pagesize", 5000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_overview(pagesize))

@app.route("/scenario/table", methods=["GET", "POST"])
def jump_table_get():
    pagesize = int(request.args.get("pagesize", 5000))
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
    pagesize = int(request.args.get("pagesize", 5000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_timeline(pagesize))

@app.route("/scenario/apc", methods=["GET", "POST"])
def jump_apc_get():
    pagesize = int(request.args.get("pagesize", 5000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_apc(pagesize))

@app.route("/scenario/costs", methods=["GET", "POST"])
def jump_costs_get():
    pagesize = int(request.args.get("pagesize", 5000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_cost(pagesize))

@app.route("/scenario/oa", methods=["GET", "POST"])
def jump_oa_get():
    pagesize = int(request.args.get("pagesize", 5000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_oa(pagesize))


@app.route("/scenario/fulfillment", methods=["GET", "POST"])
def jump_fulfillment_get():
    pagesize = int(request.args.get("pagesize", 5000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_fulfillment(pagesize))

@app.route("/scenario/report", methods=["GET", "POST"])
def jump_report_get():
    pagesize = int(request.args.get("pagesize", 5000))
    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args
    package = get_clean_package(scenario_input)
    scenario = Scenario(package, scenario_input)
    return jsonify_fast_no_sort(scenario.to_dict_report(pagesize))

@app.route("/scenario/impact", methods=["GET", "POST"])
def jump_impact_get():
    pagesize = int(request.args.get("pagesize", 5000))
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
    my_timing = TimingMessages()

    if not request.is_json:
        return abort_json(400, "Missing JSON in request")

    username = request.json.get('username', None)
    password = request.json.get('password', None)

    if not username:
        return abort_json(400, "Missing username parameter")
    if not password:
        return abort_json(400, "Missing password parameter")

    my_account = Account.query.filter(Account.username == username).first()
    my_timing.log_timing("after db get for account")

    if not my_account or not check_password_hash(my_account.password_hash, password):
        return abort_json(401, "Bad username or password")

    # Identity can be any data that is json serializable
    access_token = create_access_token(identity=my_account.id)

    my_timing.log_timing("after create_access_token")

    return jsonify({"access_token": access_token, "_timing": my_timing.to_dict()})


# curl -s -X POST -H 'Accept: application/json' -H 'Content-Type: application/json' --data '{"username":"test","password":"password","rememberMe":false}' http://localhost:5004/login
#curl -H 'Accept: application/json' -H "Authorization: Bearer ${TOKEN}" http://localhost:5004/protected

# Protect a view with jwt_required, which requires a valid access token
# in the request to access.
@app.route('/protected', methods=['GET'])
@jwt_required
def protected():
    # Access the identity of the current user with get_jwt_identity
    current_user = get_jwt_identity()
    return jsonify({"logged_in_as": current_user["id"]})

@app.route('/account', methods=['GET'])
@jwt_required
def account_get():
    my_timing = TimingMessages()

    jwt_account_id = get_jwt_identity()
    my_account = Account.query.get(jwt_account_id)
    my_timing.log_timing("after getting account")

    # scenario = Scenario(my_account.active_package)
    # my_timing.log_timing("after creating scenario")

    account_dict = {
        "id": my_account.id,
        "name": my_account.display_name,
        "packages": [package.to_dict_summary() for package in my_account.packages],
        # "scenarios": [{
        #     "id": my_account.default_scenario_id,
        #     "name": my_account.default_scenario_name,
        #     "pkgId": my_account.default_package_id,
        #     "summary": {
        #         "cost_percent": scenario.cost_spent_percent,
        #         "use_instant_percent": scenario.use_instant_percent,
        #         "num_journals_subscribed": len(scenario.subscribed),
        #     },
        #     "subrs": [],
        #     "customSubrs": [],
        #     "configs": scenario.settings.to_dict()
        # }]
    }
    my_timing.log_timing("after to_dict()")
    account_dict["_timing"] = my_timing.to_dict()

    return jsonify_fast(account_dict)


@app.route('/package/<package_id>', methods=['GET'])
@jwt_required
def package_id_get(package_id):
    my_timing = TimingMessages()

    jwt_account_id = get_jwt_identity()
    my_package = Package.query.get(package_id)
    if not my_package:
        abort_json(404, "Package not found")

    if my_package.account_id != jwt_account_id:
        abort_json(401, "Not authorized to view this package")

    my_timing.log_timing("after getting package")

    package_dict = my_package.to_dict_summary()
    package_dict["scenarios"] = [scenario.to_dict_definition() for scenario in my_package.scenarios]
    my_timing.log_timing("after to_dict()")
    package_dict["_timing"] = my_timing.to_dict()

    return jsonify_fast(package_dict)

@app.route('/scenario/<scenario_id>', methods=['GET'])
@jwt_required
def scenario_id_get(scenario_id):
    my_timing = TimingMessages()

    jwt_account_id = get_jwt_identity()
    my_saved_scenario = SavedScenario.query.get(scenario_id)
    if not my_saved_scenario:
        abort_json(404, "Scenario not found")

    if my_saved_scenario.package.account_id != jwt_account_id:
        abort_json(401, "Not authorized to view this package")

    my_timing.log_timing("after getting scenario")

    response = my_saved_scenario.to_dict_definition()

    my_timing.log_timing("after to_dict()")
    response["_timing"] = my_timing.to_dict()

    return jsonify_fast(response)

@app.route('/scenario/<scenario_id>/summary', methods=['GET', 'POST'])
def scenario_id_summary_get(scenario_id):
    return jump_summary_get()

@app.route('/scenario/<scenario_id>/journals', methods=['GET', 'POST'])
@app.route('/scenario/<scenario_id>/overview', methods=['GET', 'POST'])
def scenario_id_overview_get(scenario_id):
    return jump_overview_get()

@app.route('/scenario/<scenario_id>/table', methods=['GET', 'POST'])
def scenario_id_table_get(scenario_id):
    return jump_table_get()

@app.route('/scenario/<scenario_id>/slider', methods=['GET', 'POST'])
def scenario_id_slider_get(scenario_id):
    return jump_slider_get()

@app.route('/scenario/<scenario_id>/timeline', methods=['GET', 'POST'])
def scenario_id_timeline_get(scenario_id):
    return jump_timeline_get()

@app.route('/scenario/<scenario_id>/apc', methods=['GET', 'POST'])
def scenario_id_apc_get(scenario_id):
    return jump_apc_get()

@app.route('/scenario/<scenario_id>/costs', methods=['GET', 'POST'])
def scenario_id_costs_get(scenario_id):
    return jump_costs_get()

@app.route('/scenario/<scenario_id>/oa', methods=['GET', 'POST'])
def scenario_id_oa_get(scenario_id):
    return jump_oa_get()

@app.route('/scenario/<scenario_id>/fulfillment', methods=['GET', 'POST'])
def scenario_id_fulfillment_get(scenario_id):
    return jump_fulfillment_get()

@app.route('/scenario/<scenario_id>/report', methods=['GET', 'POST'])
def scenario_id_report_get(scenario_id):
    return jump_report_get()

@app.route('/scenario/<scenario_id>/impact', methods=['GET', 'POST'])
def scenario_id_impact_get(scenario_id):
    return jump_impact_get()

@app.route('/scenario/<scenario_id>/export.csv', methods=['GET', 'POST'])
def scenario_id_export_csv_get(scenario_id):
    return jump_export_csv()

@app.route('/register', methods=['GET'])
def register_user():
    secret = request.args.get('secret', None)
    if not safe_str_cmp(secret, os.getenv("JWT_SECRET_KEY")):
        abort_json(500, "Secret doesn't match, not saving user in database")

    new_account = Account()
    new_account.username = request.args.get('username')
    new_account.password_hash = generate_password_hash(request.args.get('password'))
    new_account.display_name = request.args.get('name',  None)
    db.session.add(new_account)
    safe_commit(db)

    return jsonify({'message': 'User registered successfully',
                    "username": request.args.get('username')})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5004))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)

















