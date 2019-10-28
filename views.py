# coding: utf-8

from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify
from flask import send_from_directory

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


@app.after_request
def after_request_stuff(resp):
    sys.stdout.flush()  # without this jason's heroku local buffers forever
    #support CORS
    resp.headers['Access-Control-Allow-Origin'] = "*"
    resp.headers['Access-Control-Allow-Methods'] = "POST, GET, OPTIONS, PUT, DELETE, PATCH"
    resp.headers['Access-Control-Allow-Headers'] = "origin, content-type, accept, x-requested-with"
    return resp


@app.route('/', methods=["GET", "POST"])
def base_endpoint():
    return jsonify_fast({
        "version": "0.0.1",
        "msg": "Don't panic"
    })

import os
from flask import send_from_directory

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, '.'),
                          'favicon.ico', mimetype='image/vnd.microsoft.icon')

def get_clean_package(http_request_args):
    package = http_request_args.get("package", "demo")
    if package == "demo":
        package = "uva_elsevier"
    return package

@app.route("/scenario/subscription_wizard", methods=["GET"])
def jump_wizard_get():
    package = get_clean_package(request.args)
    spend = int(request.args.get("spend", 0))
    pagesize = int(request.args.get("pagesize", 100))
    scenario = Scenario(package, request.args)
    scenario.do_wizardly_things(spend)
    return jsonify_fast(scenario.to_dict(pagesize))


@app.route("/scenario", methods=["GET"])
def jump_get():
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    pagesize = int(request.args.get("pagesize", 100))
    return jsonify_fast(scenario.to_dict(pagesize))

@app.route("/scenario/timeline", methods=["GET"])
def jump_timeline_get():
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    pagesize = int(request.args.get("pagesize", 100))
    return jsonify_fast(scenario.to_dict_timeline(pagesize))

@app.route("/scenario/report", methods=["GET"])
def jump_report_get():
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    pagesize = int(request.args.get("pagesize", 100))
    return jsonify_fast(scenario.to_dict_report(pagesize))

@app.route("/scenario/impact", methods=["GET"])
def jump_impact_get():
    package = get_clean_package(request.args)
    scenario = Scenario(package, request.args)
    pagesize = int(request.args.get("pagesize", 100))
    return jsonify_fast(scenario.to_dict_impact(pagesize))


@app.route("/journal/issn_l/<issn_l>", methods=["GET"])
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

















