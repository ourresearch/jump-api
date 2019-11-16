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
import shortuuid
import datetime

from app import app
from app import logger
from app import jwt
from app import db
from scenario import Scenario
from account import Account
from package import Package
from saved_scenario import SavedScenario
from saved_scenario import get_latest_scenario
from util import jsonify_fast
from util import jsonify_fast_no_sort
from util import str2bool
from util import elapsed
from util import abort_json
from util import safe_commit
from util import TimingMessages
from util import get_ip

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
@jwt_optional
def jump_wizard_get():
    identity_dict = get_jwt_identity()

    pagesize = int(request.args.get("pagesize", 100))
    spend = int(request.args.get("spend"))
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    scenario.do_wizardly_things(spend)

    my_saved_scenario = SavedScenario.query.get("demo")
    my_saved_scenario.live_scenario = scenario
    unique_id = shortuuid.uuid()[0:20]
    if identity_dict:
        unique_id = identity_dict.get("login_uuid")
    my_saved_scenario.set_unique_id(unique_id)
    my_saved_scenario.save_live_scenario_to_db(get_ip(request))

    response = scenario.to_dict(pagesize)
    response["_scenario_id"] = my_saved_scenario.scenario_id

    return jsonify_fast(response)


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
@app.route('/login', methods=["GET", 'POST'])
def login():
    my_timing = TimingMessages()

    request_source = request.args
    if request.is_json:
        request_source = request.json
    username = request_source.get('username', None)
    password = request_source.get('password', None)

    if not username:
        return abort_json(400, "Missing username parameter")
    if not password:
        return abort_json(400, "Missing password parameter")

    my_account = Account.query.filter(Account.username == username).first()
    my_timing.log_timing("after db get for account")

    if not my_account or not check_password_hash(my_account.password_hash, password):
        return abort_json(401, "Bad username or password")

    # Identity can be any data that is json serializable.  Include timestamp so is unique for each demo start.
    identity_dict = {
        "account_id": my_account.id,
        "login_uuid": shortuuid.uuid()[0:10],
        "created": datetime.datetime.utcnow().isoformat(),
        "is_demo_account": my_account.is_demo_account
    }
    print u"login with {}".format(identity_dict)
    access_token = create_access_token(identity=identity_dict)

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
    identity_dict = get_jwt_identity()
    return jsonify({"logged_in_as": identity_dict["account_id"]})

@app.route('/account', methods=['GET'])
@jwt_required
def account_get():
    my_timing = TimingMessages()

    identity_dict = get_jwt_identity()
    my_account = Account.query.get(identity_dict["account_id"])
    if identity_dict["is_demo_account"]:
        my_account.make_unique_demo_packages(identity_dict["login_uuid"])
    my_timing.log_timing("after getting account")

    account_dict = {
        "id": my_account.id,
        "name": my_account.display_name,
        "packages": [package.to_dict_summary() for package in my_account.unique_packages],
    }
    my_timing.log_timing("after to_dict()")
    account_dict["_timing"] = my_timing.to_dict()

    return jsonify_fast(account_dict)


@app.route('/package/<package_id>', methods=['GET'])
@jwt_required
def package_id_get(package_id):
    my_timing = TimingMessages()

    identity_dict = get_jwt_identity()

    if package_id.startswith("demo"):
        my_package = Package.query.get("demo")
        my_package.package_id = package_id
    else:
        my_package = Package.query.get(package_id)

    if not my_package:
        abort_json(404, "Package not found")

    if my_package.account_id != identity_dict["account_id"]:
        abort_json(401, "Not authorized to view this package")

    my_timing.log_timing("after getting package")

    package_dict = my_package.to_dict_summary()
    package_dict["scenarios"] = [scenario.to_dict_definition() for scenario in my_package.unique_saved_scenarios]
    my_timing.log_timing("after to_dict()")
    package_dict["_timing"] = my_timing.to_dict()

    return jsonify_fast(package_dict)

def get_saved_scenario(scenario_id):
    identity_dict = get_jwt_identity()
    is_demo_account = (identity_dict["account_id"] == "demo")
    if is_demo_account:
        my_saved_scenario = SavedScenario.query.get(scenario_id)
        if not my_saved_scenario:
            my_saved_scenario = SavedScenario.query.get("demo")
            my_saved_scenario.scenario_id = scenario_id
    else:
        my_saved_scenario = SavedScenario.query.get(scenario_id)
    if not my_saved_scenario:
        abort_json(404, "Scenario not found")

    if my_saved_scenario.package_real.account_id != identity_dict["account_id"]:
        abort_json(401, "Not authorized to view this package")

    my_saved_scenario.set_live_scenario()
    return my_saved_scenario


@app.route('/scenario/<scenario_id>', methods=['GET'])
@jwt_required
def scenario_id_get(scenario_id):
    my_timing = TimingMessages()
    my_saved_scenario = get_saved_scenario(scenario_id)
    my_timing.log_timing("after setting live scenario")
    response = my_saved_scenario.to_dict_definition()
    my_timing.log_timing("after to_dict()")
    response["_timing"] = my_timing.to_dict()
    return jsonify_fast(response)


@app.route('/scenario/<scenario_id>', methods=['POST'])
@app.route('/scenario/<scenario_id>/post', methods=['GET'])  # just for debugging
@jwt_required
def scenario_id_post(scenario_id):
    my_timing = TimingMessages()

    identity_dict = get_jwt_identity()

    scenario_input = request.get_json()
    if not scenario_input:
        scenario_input = request.args

    my_saved_scenario = SavedScenario.query.get(scenario_id)

    # check if demo account is ok
    if identity_dict["is_demo_account"]:
        if not scenario_id.startswith("demo"):
            abort_json(401, "Not authorized to view this package")
        if not my_saved_scenario:
            my_saved_scenario = SavedScenario(True, scenario_id, scenario_input)
            my_saved_scenario.scenario_id = scenario_id
    else:
        if not my_saved_scenario:
            abort_json(404, "Package not found")
        if my_saved_scenario.package.account_id != identity_dict["account_id"]:
            abort_json(401, "Not authorized to view this package")

    package_id = get_clean_package(my_saved_scenario.package_id)
    my_live_scenario = Scenario(package_id, scenario_input)  # don't care about old one, just write new one
    my_saved_scenario.live_scenario = my_live_scenario

    my_saved_scenario.save_live_scenario_to_db(get_ip(request))

    response = my_saved_scenario.to_dict_definition()

    my_timing.log_timing("after to_dict()")
    response["_timing"] = my_timing.to_dict()

    return jsonify_fast(response)



@app.route('/scenario/<scenario_id>/summary', methods=['GET', 'POST'])
@jwt_optional
def scenario_id_summary_get(scenario_id):
    pagesize = int(request.args.get("pagesize", 5000))
    my_timing = TimingMessages()
    my_saved_scenario = get_saved_scenario(scenario_id)
    my_timing.log_timing("after setting live scenario")
    response = my_saved_scenario.to_dict_definition()
    my_timing.log_timing("after to_dict()")
    response["_timing"] = my_timing.to_dict()
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_summary())

@app.route('/scenario/<scenario_id>/journals', methods=['GET', 'POST'])
@app.route('/scenario/<scenario_id>/overview', methods=['GET', 'POST'])
@jwt_optional
def scenario_id_overview_get(scenario_id):
    pagesize = int(request.args.get("pagesize", 5000))
    my_timing = TimingMessages()
    my_saved_scenario = get_saved_scenario(scenario_id)
    my_timing.log_timing("after setting live scenario")
    response = my_saved_scenario.to_dict_definition()
    my_timing.log_timing("after to_dict()")
    response["_timing"] = my_timing.to_dict()
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_overview(pagesize))

@app.route('/scenario/<scenario_id>/table', methods=['GET', 'POST'])
@jwt_optional
def scenario_id_table_get(scenario_id):
    pagesize = int(request.args.get("pagesize", 5000))
    my_timing = TimingMessages()
    my_saved_scenario = get_saved_scenario(scenario_id)
    my_timing.log_timing("after setting live scenario")
    response = my_saved_scenario.to_dict_definition()
    my_timing.log_timing("after to_dict()")
    response["_timing"] = my_timing.to_dict()
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_table(pagesize))

@app.route('/scenario/<scenario_id>/slider', methods=['GET', 'POST'])
@jwt_optional
def scenario_id_slider_get(scenario_id):
    pagesize = int(request.args.get("pagesize", 5000))
    my_timing = TimingMessages()
    my_saved_scenario = get_saved_scenario(scenario_id)
    my_timing.log_timing("after setting live scenario")
    response = my_saved_scenario.to_dict_definition()
    my_timing.log_timing("after to_dict()")
    response["_timing"] = my_timing.to_dict()
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_slider(pagesize))

@app.route('/scenario/<scenario_id>/apc', methods=['GET', 'POST'])
@jwt_optional
def scenario_id_apc_get(scenario_id):
    pagesize = int(request.args.get("pagesize", 5000))
    my_timing = TimingMessages()
    my_saved_scenario = get_saved_scenario(scenario_id)
    my_timing.log_timing("after setting live scenario")
    response = my_saved_scenario.to_dict_definition()
    my_timing.log_timing("after to_dict()")
    response["_timing"] = my_timing.to_dict()
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_apc(pagesize))


@app.route('/scenario/<scenario_id>/report', methods=['GET', 'POST'])
def scenario_id_report_get(scenario_id):
    pagesize = int(request.args.get("pagesize", 5000))
    my_timing = TimingMessages()
    my_saved_scenario = get_saved_scenario(scenario_id)
    my_timing.log_timing("after setting live scenario")
    response = my_saved_scenario.to_dict_definition()
    my_timing.log_timing("after to_dict()")
    response["_timing"] = my_timing.to_dict()
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_report(pagesize))

@app.route('/scenario/<scenario_id>/export.csv', methods=['GET', 'POST'])
@jwt_optional
def scenario_id_export_csv_get(scenario_id):
    # TODO
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

















