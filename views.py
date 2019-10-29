# coding: utf-8

from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify
from flask import url_for

import json
import os
import sys
from time import time
from app import app
from app import logger
from scenario import Scenario
from util import jsonify_fast
from util import str2bool
from util import elapsed

# warm the cache
print "warming the cache"
start_time = time()
Scenario("demo")
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

@app.route('/favicon.ico')
def favicon():
    return redirect(url_for("static", filename="img/favicon.ico", _external=True, _scheme='https'))

def get_clean_package(http_request_args):
    package = http_request_args.get("package", "demo")
    if package == "demo":
        package = "uva_elsevier"
    return package

@app.route("/scenario/wizard", methods=["GET", "POST"])
def jump_wizard_get():
    pagesize = int(request.args.get("pagesize", 100))
    package = get_clean_package(request.args)
    spend = int(request.args.get("spend", 0))
    scenario = Scenario(package, request.args)
    scenario.do_wizardly_things(spend)
    return jsonify_fast(scenario.to_dict(pagesize))


@app.route("/scenario/summary", methods=["GET", "POST"])
def jump_summary_get():
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    return jsonify_fast(scenario.to_dict_summary())

@app.route("/scenario", methods=["GET", "POST"])
@app.route("/scenario/slider", methods=["GET", "POST"])
def jump_slider_get():
    pagesize = int(request.args.get("pagesize", 100))
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    return jsonify_fast(scenario.to_dict(pagesize))

@app.route("/scenario/timeline", methods=["GET", "POST"])
def jump_timeline_get():
    pagesize = int(request.args.get("pagesize", 100))
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    return jsonify_fast(scenario.to_dict_timeline(pagesize))

@app.route("/scenario/apc", methods=["GET", "POST"])
def jump_apc_get():
    pagesize = int(request.args.get("pagesize", 100))
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    return jsonify_fast(scenario.to_dict_apc(pagesize))

@app.route("/scenario/cost", methods=["GET", "POST"])
def jump_cost_get():
    pagesize = int(request.args.get("pagesize", 100))
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    return jsonify_fast(scenario.to_dict_cost(pagesize))

@app.route("/scenario/journals", methods=["GET", "POST"])
def jump_journals_get():
    pagesize = int(request.args.get("pagesize", 100))
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    return jsonify_fast(scenario.to_dict_journals(pagesize))

@app.route("/scenario/fulfillment", methods=["GET", "POST"])
def jump_fulfillment_get():
    pagesize = int(request.args.get("pagesize", 100))
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    return jsonify_fast(scenario.to_dict_fulfillment(pagesize))

@app.route("/scenario/report", methods=["GET", "POST"])
def jump_report_get():
    pagesize = int(request.args.get("pagesize", 100))
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    return jsonify_fast(scenario.to_dict_report(pagesize))

@app.route("/scenario/impact", methods=["GET", "POST"])
def jump_impact_get():
    pagesize = int(request.args.get("pagesize", 100))
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    return jsonify_fast(scenario.to_dict_impact(pagesize))


@app.route("/journal/issn_l/<issn_l>", methods=["GET", "POST"])
def jump_issn_get(issn_l):
    package = request.args.get("package", "demo")
    if package == "demo":
        package = "uva_elsevier"

    scenario = Scenario(package, request.args)
    my_journal = scenario.get_journal(issn_l)
    return jsonify(my_journal.to_dict_details())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5004))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)

















