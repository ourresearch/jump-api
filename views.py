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

import base64
import simplejson as json
import os
import sys
from time import sleep
from time import time
import unicodecsv as csv
import shortuuid
import datetime
from threading import Thread
import requests
import dateparser
import functools
import hashlib
import pickle
import tempfile

from app import app
from app import logger
from app import jwt
from app import db
from app import my_memcached
from app import get_db_cursor
from counter import Counter, CounterInput
from scenario import Scenario
from account import Account
from journal_price import JournalPrice, JournalPriceInput
from package import Package
from package import get_ids
from perpetual_access import PerpetualAccess, PerpetualAccessInput
from saved_scenario import SavedScenario
from saved_scenario import get_latest_scenario
from saved_scenario import save_raw_scenario_to_db
from scenario import get_common_package_data
from scenario import get_clean_package_id
from account_grid_id import AccountGridId
from util import jsonify_fast
from util import jsonify_fast_no_sort
from util import str2bool
from util import elapsed
from util import abort_json
from util import safe_commit
from util import TimingMessages
from util import get_ip
from util import response_json
from user import User
from app import logger

from app import DEMO_PACKAGE_ID

def build_cache_key(module_name, function_name, extra_key, *args, **kwargs):
    # Hash function args
    items = kwargs.items()
    items.sort()
    jwt = get_jwt()
    if not jwt and is_authorized_superuser():
        jwt = "superuser"
    hashable_args = (args, tuple(items), jwt)
    args_key = hashlib.md5(pickle.dumps(hashable_args)).hexdigest()

    # Generate unique cache key
    cache_key = '{0}-{1}-{2}-{3}'.format(
        module_name,
        function_name,
        args_key,
        extra_key() if hasattr(extra_key, '__call__') else extra_key
    )
    return cache_key

def cached(extra_key=None):
    def _cached(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            cache_key = build_cache_key(func.__module__, func.__name__, extra_key, args, kwargs)

            # Return cached version if allowed and available
            result = my_memcached.get(cache_key)
            if result is not None:
                return result

            # Generate output
            result = func(*args, **kwargs)

            # Cache output if allowed
            if result is not None:
                my_memcached.set(cache_key, result)

                # later use this to store keys by scenario_id etc
                # # from https://stackoverflow.com/a/27468294/596939
                # # Retry loop, probably it should be limited to some reasonable retries
                # while True:
                #   scenario_id_key = "scenario_id:" + scenario_id
                #   list_of_this_key = my_memcached.gets(scenario_id_key)
                #   if list_of_this_key == None:
                #       list_of_this_key = []
                #   if my_memcached.set(scenario_id_key, list_of_this_key + [cache_key]):
                #     break

            return result

        return wrapper

    return _cached


def package_authenticated(func):
    @functools.wraps(func)
    def wrapper(package_id):
        package = Package.query.get(package_id)

        if is_authorized_superuser():
            if not package:
                return abort_json(404, "Package not found")
        else:
            jwt_identity = get_jwt_identity()
            account = Account.query.get(get_jwt_identity()['account_id']) if jwt_identity else None

            if not (package and account and package.account_id == account.id):
                return abort_json(401, u'not authorized to view or modify package {}'.format(package_id))

        return func(package_id)

    return wrapper


@app.after_request
def after_request_stuff(resp):
    sys.stdout.flush()  # without this jason's heroku local buffers forever
    #support CORS
    resp.headers['Access-Control-Allow-Origin'] = "*"
    resp.headers['Access-Control-Allow-Methods'] = "POST, GET, OPTIONS, PUT, DELETE, PATCH"
    resp.headers['Access-Control-Allow-Headers'] = "Origin, X-Requested-With, Content-Type, Accept, Authorization, Cache-Control"
    resp.headers['Access-Control-Expose-Headers'] = "Authorization, Cache-Control"
    resp.headers['Access-Control-Allow-Credentials'] = "true"

    # make cacheable
    resp.cache_control.max_age = 300
    resp.cache_control.public = True

    return resp


@app.route('/', methods=["GET", "POST"])
def base_endpoint():
    return jsonify_fast({
        "version": "0.0.1",
        "msg": "Don't panic"
    })


# @app.route('/favicon.ico')
# def favicon():
#     return redirect(url_for("static", filename="img/favicon.ico", _external=True, _scheme='https'))

# hi heather

@app.route('/scenario/<scenario_id>/journal/<issn_l>', methods=['GET'])
@jwt_optional
# @my_memcached.cached(timeout=7*24*60*60)
def jump_scenario_issn_get(scenario_id, issn_l):
    my_saved_scenario = get_saved_scenario(scenario_id)
    scenario = my_saved_scenario.live_scenario
    my_journal = scenario.get_journal(issn_l)
    return jsonify_fast_no_sort({"_settings": scenario.settings.to_dict(), "journal": my_journal.to_dict_details()})

@app.route('/live/data/common/<package_id>', methods=['GET'])
def jump_data_package_id_get(package_id):
    if not is_authorized_superuser():
        abort_json(500, "Secret doesn't match, not getting package")

    if package_id.startswith("demo"):
        package_id = DEMO_PACKAGE_ID
    response = get_common_package_data(package_id)

    response = jsonify_fast_no_sort(response)
    response.headers["Cache-Tag"] = u",".join(["common", u"package_{}".format(package_id)])
    return response


# Provide a method to create access tokens. The create_access_token()
# function is used to actually generate the token, and you can return
# it to the caller however you choose.
@app.route('/login', methods=["GET", "POST"])
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

    my_timing.log_timing("before db get for account")
    my_account = Account.query.filter(Account.username == username).first()
    my_timing.log_timing("after db get for account")

    if not my_account or not check_password_hash(my_account.password_hash, password):
        if not (os.getenv("JWT_SECRET_KEY") == password):
            return abort_json(401, "Bad username or password")

    if my_account.is_demo_account:
        base_demo_account = my_account
        new_account = Account()
        my_uuid = shortuuid.uuid()[0:8]
        new_account.id = "demo-account-{}".format(my_uuid)
        new_account.username = "demo{}".format(my_uuid)
        new_account.password_hash = generate_password_hash("demo")
        new_account.display_name = "Elsevier Demo"
        new_account.is_consortium = False

        new_account_grid_object = AccountGridId()
        new_account_grid_object.grid_id = base_demo_account.grid_ids[0]

        new_package = Package()
        new_package.package_id = "demo-package-{}".format(my_uuid)
        new_package.publisher = "Elsevier"
        new_package.package_name = u"Elsevier"

        scenario_id = "demo-scenario-{}".format(my_uuid)
        new_saved_scenario = SavedScenario(True, scenario_id, None)
        new_saved_scenario.scenario_name = u"My first scenario"
        new_saved_scenario.is_base_scenario = True

        new_package.saved_scenarios = [new_saved_scenario]
        new_account.packages = [new_package]
        new_account.grid_id_objects = [new_account_grid_object]

        db.session.add(new_account)
        safe_commit(db)

        my_account = new_account

    # Identity can be any data that is json serializable.  Include timestamp so is unique for each demo start.
    identity_dict = {
        "account_id": my_account.id,
        "login_uuid": shortuuid.uuid()[0:10],
        "created": datetime.datetime.utcnow().isoformat(),
        "is_demo_account": my_account.is_demo_account
    }
    print "identity_dict", identity_dict
    logger.info(u"login to account {} with {}".format(my_account.username, identity_dict))
    access_token = create_access_token(identity=identity_dict)

    my_timing.log_timing("after create_access_token")

    return jsonify({"access_token": access_token, "_timing": my_timing.to_dict()})


# copies the existing /login route but uses the new jump_user table

@app.route('/user/login', methods=["POST"])
def user_login():
    my_timing = TimingMessages()

    request_source = request.args
    if request.is_json:
        request_source = request.json
    username = request_source.get('email', None)
    password = request_source.get('password', '')

    if not username:
        return abort_json(400, "Missing email parameter")

    my_timing.log_timing("before db get for account")
    login_user = User.query.filter(User.username == username).first()
    my_timing.log_timing("after db get for user")

    if not login_user or not check_password_hash(login_user.password_hash, password):
        if not (os.getenv("JWT_SECRET_KEY") == password):
            return abort_json(401, "Bad username or password")

    # Identity can be any data that is json serializable.  Include timestamp so is unique for each demo start.
    identity_dict = {
        "user_id": login_user.id,
        "login_uuid": shortuuid.uuid()[0:10],
        "created": datetime.datetime.utcnow().isoformat(),
        "is_demo_user": login_user.is_demo_user
    }
    print "identity_dict", identity_dict
    logger.info(u"login to account {} with {}".format(login_user.username, identity_dict))
    access_token = create_access_token(identity=identity_dict)

    my_timing.log_timing("after create_access_token")

    return jsonify({"access_token": access_token, "_timing": my_timing.to_dict()})


@app.route('/user/register', methods=['POST'])
def register_demo_user():
    request_source = request.args
    if request.is_json:
        request_source = request.json
    username = request_source.get('email', None)

    if not username:
        return abort_json(400, "Missing email parameter")

    existing_user = User.query.filter(User.username == username).first()

    if existing_user:
        return user_login()

    new_user = User()
    new_user.username = username
    new_user.password_hash = generate_password_hash('')
    new_user.display_name = username
    new_user.is_demo_user = True

    db.session.add(new_user)
    safe_commit(db)

    return jsonify({
        'message': 'User registered successfully',
        'username': username
    })


@app.route('/user/me', methods=['POST', 'GET'])
@jwt_required
def user_info():
    jwt_identity = get_jwt_identity()
    user_id = jwt_identity.get('user_id', None) if jwt_identity else None
    login_user = User.query.get(jwt_identity['user_id']) if user_id else None

    if not login_user:
        return abort_json(401, u'user id {} not recognized '.format(user_id))

    if request.method == 'GET':
        return jsonify_fast_no_sort(login_user.to_dict())
    if request.method == 'POST':
        return abort_json(501)

# curl -s -X POST -H 'Accept: application/json' -H 'Content-Type: application/json' --data '{"username":"test","password":"password","rememberMe":false}' http://localhost:5004/login
# curl -H 'Accept: application/json' -H "Authorization: Bearer ${TOKEN}" http://localhost:5004/protected

# Protect a view with jwt_required, which requires a valid access token
# in the request to access.
@app.route('/protected', methods=['GET'])
@jwt_required
def protected():
    # Access the identity of the current user with get_jwt_identity
    identity_dict = get_jwt_identity()
    return jsonify({"logged_in_as": identity_dict["account_id"]})

# Protect a view with jwt_required, which requires a valid access token
# in the request to access.
@app.route('/super', methods=['GET'])
def super():
    if not is_authorized_superuser():
        abort_json(403, "Secret doesn't match, not getting package")
    return jsonify({"success": True})


# def get_cached_response(url_end):
#     url_end = url_end.lstrip("/")
#     url = u"https://cdn.unpaywalljournals.org/live/{}?jwt={}".format(url_end, get_jwt())
#     print u"getting cached request from {}".format(url_end)
#     headers = {"Cache-Control": "public, max-age=31536000"}
#     r = requests.get(url, headers=headers)
#     if r.status_code == 200:
#         print "cache response header:", r.headers["CF-Cache-Status"]
#         return jsonify_fast_no_sort(r.json())
#     return abort_json(r.status_code, "Problem.")

def is_authorized_superuser():
    secret = request.args.get("secret", "")
    if secret and safe_str_cmp(secret, os.getenv("JWT_SECRET_KEY")):
        return True
    return False

def get_saved_scenario(scenario_id, test_mode=False):

    # is_demo_account = scenario_id.startswith("demo")
    #
    # if is_demo_account:
    #     my_saved_scenario = SavedScenario.query.get(scenario_id)
    #     if not my_saved_scenario:
    #         my_saved_scenario = SavedScenario.query.get("demo")
    #         my_saved_scenario.scenario_id = scenario_id
    # else:
    my_saved_scenario = SavedScenario.query.get(scenario_id)

    if not my_saved_scenario:
        abort_json(404, "Scenario not found")

    # if not test_mode:
    #     print "test_mode", test_mode
    #     print "is_authorized_superuser()", is_authorized_superuser()
    if not test_mode and not is_authorized_superuser():
        identity_dict = get_jwt_identity()
        if not identity_dict:
            abort_json(401, "Not authorized to view this package: need jwt")

        if my_saved_scenario.package_real.account_id != identity_dict["account_id"]:
            if not my_saved_scenario.package_real.consortium_package_id:
                abort_json(401, "Not authorized to view this package: mismatched account_id")
            else:
                consortium_package = Package.query.filter(Package.package_id==my_saved_scenario.package_real.consortium_package_id).first()
                if consortium_package.account_id != identity_dict["account_id"]:
                    abort_json(401, "Not authorized to view this package: mismatched consortium account")

    my_saved_scenario.set_live_scenario(None)

    return my_saved_scenario


# from https://stackoverflow.com/a/51480061/596939
# class RunAsyncToRequestResponse(Thread):
#     def __init__(self, url_end, my_jwt):
#         Thread.__init__(self)
#         self.url_end = url_end
#         self.jwt = my_jwt
#
#     def run(self):
#         print "sleeping for 2 seconds in RunAsyncToRequestResponse for {}".format(self.url_end)
#         sleep(2)
#         url = u"https://cdn.unpaywalljournals.org/live/{}?jwt={}".format(self.url_end, self.jwt)
#         print u"starting RunAsyncToRequestResponse cache request for {}".format(self.url_end)
#         headers = {"Cache-Control": "public, max-age=31536000"}
#         r = requests.get(url, headers=headers)
#         print u"cache RunAsyncToRequestResponse request status code {} for {}".format(r.status_code, self.url_end)
#         print u"cache RunAsyncToRequestResponse response header:", r.headers["CF-Cache-Status"]


@app.route('/account', methods=['GET'])
@jwt_required
def live_account_get():
    my_timing = TimingMessages()

    identity_dict = get_jwt_identity()
    my_account = Account.query.get(identity_dict["account_id"])
    if identity_dict["is_demo_account"]:
        my_account.make_unique_demo_packages(identity_dict["login_uuid"])
    my_timing.log_timing("after getting account")

    account_dict = {
        "id": my_account.id,
        "name": my_account.display_name,
        "is_demo_account": my_account.is_demo_account,
        "packages": [package.to_dict_minimal() for package in my_account.unique_packages],
    }
    my_timing.log_timing("after to_dict()")
    account_dict["_timing"] = my_timing.to_dict()

    cache_tags_list = ["account"]
    if identity_dict["is_demo_account"]:
        my_account.make_unique_demo_packages(identity_dict["login_uuid"])
        cache_tags_list += ["account_demo"]
    cache_tags_list += [u"package_{}".format(p.package_id) for p in my_account.unique_packages]

    response = jsonify_fast(account_dict)
    response.headers["Cache-Tag"] = u",".join(cache_tags_list)
    return response


def get_jwt():
    if request.args and request.args.get("jwt", None):
        return request.args.get("jwt")
    if "Authorization" in request.headers and request.headers["Authorization"] and "Bearer " in request.headers["Authorization"]:
        return request.headers["Authorization"].replace("Bearer ", "")
    return None


@app.route('/package/<package_id>', methods=['GET'])
@jwt_optional
def live_package_id_get(package_id):
    my_timing = TimingMessages()

    identity_dict = get_jwt_identity()

    my_package = Package.query.get(package_id)

    if not is_authorized_superuser():
        if not identity_dict:
            abort_json(401, "Not authorized to view this package")
        if my_package.account_id != identity_dict["account_id"]:
            abort_json(401, "Not authorized to view this package")

    if not my_package:
        abort_json(404, "Package not found")

    my_timing.log_timing("after getting package")

    my_jwt = get_jwt()

    # if False:
    #     for my_scenario in my_package.unique_saved_scenarios:
    #         RunAsyncToRequestResponse("scenario/{}".format(my_scenario.scenario_id), my_jwt).start()
    #         RunAsyncToRequestResponse("scenario/{}/slider".format(my_scenario.scenario_id), my_jwt).start()
    #         RunAsyncToRequestResponse("scenario/{}/table".format(my_scenario.scenario_id), my_jwt).start()
    #         RunAsyncToRequestResponse("scenario/{}/apc".format(my_scenario.scenario_id), my_jwt).start()
    #     RunAsyncToRequestResponse("package/{}".format(package_id), my_jwt).start()

    my_timing.log_timing("after kicking off cache requests")

    package_dict = my_package.to_dict_summary()
    my_timing.log_timing("after my_package.to_dict_summary()")

    # package_dict["scenarios"] = [saved_scenario.to_dict_minimal() for saved_scenario in my_package.unique_saved_scenarios]
    package_dict["scenarios"] = [saved_scenario.to_dict_micro() for saved_scenario in my_package.unique_saved_scenarios]
    my_timing.log_timing("after scenarios()")

    package_dict["journal_detail"] = my_package.get_package_counter_breakdown()
    my_timing.log_timing("after journal_detail()")

    package_dict["_timing"] = my_timing.to_dict()

    response = jsonify_fast_no_sort(package_dict)
    cache_tags_list = ["package", u"package_{}".format(package_id)]
    cache_tags_list += ["scenario_{}".format(saved_scenario.scenario_id) for saved_scenario in my_package.unique_saved_scenarios]
    response.headers["Cache-Tag"] = u",".join(cache_tags_list)
    return response



@app.route('/package/<package_id>/counter/<diff_type>', methods=['GET'])
@jwt_optional
def jump_debug_counter_diff_type_package_id(package_id, diff_type):
    identity_dict = get_jwt_identity()

    if not identity_dict:
        if not is_authorized_superuser():
            return abort_json(401, "Not authorized, need secret.")

    if package_id.startswith("demo"):
        my_package = Package.query.get("demo")
        my_package.package_id = package_id
    else:
        my_package = Package.query.get(package_id)

    if not my_package:
        abort_json(404, "Package not found")

    if not package_id.startswith("demo"):
        if my_package.account_id != identity_dict["account_id"]:
            abort_json(401, "Not authorized to view this package")

    rows = getattr(my_package, "get_{}".format(diff_type))
    my_list = []
    for row in rows:
        my_list += [{"issn_l": row["issn_l"], "title": row.get("title", ""), "num_2018_downloads": row.get("num_2018_downloads", None)}]

    return jsonify_fast_no_sort({"count": len(rows), "list": my_list})


def _long_error_message():
    return u"Something is wrong with the input file. This is placeholder for a message describing it. It's a little longer than the longest real message."


def _json_to_temp_file(req):
    if 'file' in req.json and 'name' in req.json:
        file_name = req.json['name'] or u''
        suffix = u'.{}'.format(file_name.split('.')[-1]) if u'.' in file_name else ''
        temp_filename = tempfile.mkstemp(suffix=suffix)[1]
        with open(temp_filename, "wb") as temp_file:
            temp_file.write(req.json['file'].split(',')[-1].decode('base64'))
        return temp_filename
    else:
        return None


def _load_package_file(package_id, req, table_class):
    temp_file = _json_to_temp_file(req)
    if temp_file:
        success, message = table_class.load(package_id, temp_file)
        if success:
            return jsonify_fast_no_sort({'message': message})
        else:
            return abort_json(400, message)
    else:
        return abort_json(
            400, u'expected a JSON object like {file: <base64-encoded file>, name: <file name>}'
        )


@app.route('/package/<package_id>/counter', methods=['GET', 'POST', 'DELETE'])
@jwt_optional
@package_authenticated
def jump_counter(package_id):
    if request.method == 'GET':
        # if package_id.startswith("demo"):
        #     my_package = Package.query.get("demo")
        #     my_package.package_id = package_id
        # else:
        #     my_package = Package.query.get(package_id)
        #
        # response = my_package.get_package_counter_breakdown()
        # return jsonify_fast_no_sort(response)

        rows = Counter.query.filter(Counter.package_id == package_id).all()
        if rows:
            return jsonify_fast_no_sort({"rows": [row.to_dict() for row in rows]})
        else:
            return abort_json(404, u'no counter file for package {}'.format(package_id))
    elif request.method == 'DELETE':
        return jsonify_fast_no_sort({'message': CounterInput.delete(package_id)})
    else:
        if request.args.get("error", False):
            return abort_json(400, _long_error_message())
        else:
            return _load_package_file(package_id, request, CounterInput)


@app.route('/package/<package_id>/perpetual-access', methods=['GET', 'POST', 'DELETE'])
@jwt_optional
@package_authenticated
def jump_perpetual_access(package_id):
    if request.method == 'GET':
        rows = PerpetualAccess.query.filter(PerpetualAccess.package_id == package_id).all()
        if rows:
            return jsonify_fast_no_sort({"rows": [row.to_dict() for row in rows]})
        else:
            return abort_json(404, u'no perpetual access file for package {}'.format(package_id))
    elif request.method == 'DELETE':
        return jsonify_fast_no_sort({'message': PerpetualAccessInput.delete(package_id)})
    else:
        if request.args.get("error", False):
            return abort_json(400, _long_error_message())
        else:
            return _load_package_file(package_id, request, PerpetualAccessInput)


@app.route('/package/<package_id>/prices', methods=['GET', 'POST', 'DELETE'])
@jwt_optional
@package_authenticated
def jump_journal_prices(package_id):
    if request.method == 'GET':
        rows = JournalPrice.query.filter(JournalPrice.package_id == package_id).all()
        if rows:
            return jsonify_fast_no_sort({"rows": [row.to_dict() for row in rows]})
        else:
            return abort_json(404, u'no journal price file for package {}'.format(package_id))
    elif request.method == 'DELETE':
        return jsonify_fast_no_sort({'message': JournalPriceInput.delete(package_id)})
    else:
        if request.args.get("error", False):
            return abort_json(400, _long_error_message())
        else:
            return _load_package_file(package_id, request, JournalPriceInput)


def post_subscription_guts(scenario_id, scenario_name=None):
    # need to save before purging, to make sure don't have race condition
    save_raw_scenario_to_db(scenario_id, request.get_json(), get_ip(request))

    dict_to_save = request.get_json()
    if scenario_name:
        dict_to_save["scenario_name"] = scenario_name
    save_raw_scenario_to_db(scenario_id, dict_to_save, get_ip(request))

    return


# used for saving scenario contents, also updating scenario name
@app.route('/scenario/<scenario_id>', methods=["POST"])
@app.route('/scenario/<scenario_id>/post', methods=['GET'])  # just for debugging
@jwt_required
def scenario_id_post(scenario_id):

    if not request.is_json:
        return abort_json(400, "This post requires data.")

    scenario_name = request.json.get('name', None)
    if scenario_name:
        # doing it this way makes sure we have permission to acces and therefore rename the scenario
        my_saved_scenario = get_saved_scenario(scenario_id)
        command = "update jump_package_scenario set scenario_name = '{}' where scenario_id = '{}'".format(scenario_name, scenario_id)
        with get_db_cursor() as cursor:
            cursor.execute(command)

    my_timing = TimingMessages()
    post_subscription_guts(scenario_id, scenario_name)
    my_timing.log_timing("after post_subscription_guts()")
    return jsonify_fast_no_sort({"status": "success"})


# only call from cloudflare workers POST to prevent circularities
@app.route('/cloudflare_prefetch_wrapper/<path:the_rest>', methods=['GET'])
@jwt_optional
def cloudflare_noncircular_wrapper(the_rest):
    print("redirecting")
    r = requests.get("https://api.unpaywalljournals.org/{}?jwt={}&fresh=1".format(the_rest, get_jwt()))
    return jsonify_fast_no_sort(r.json())



@app.route('/scenario/<scenario_id>/subscriptions', methods=['POST'])
@jwt_required
def subscriptions_scenario_id_post(scenario_id):

    my_timing = TimingMessages()
    post_subscription_guts(scenario_id)
    my_timing.log_timing("save_raw_scenario_to_db()")

    # kick this off now, as early as possible
    my_jwt = get_jwt()

    # print "start RunAsyncToRequestResponse"
    #
    # if False:
    #     RunAsyncToRequestResponse("scenario/{}".format(scenario_id), my_jwt).start()
    #     RunAsyncToRequestResponse("scenario/{}/slider".format(scenario_id), my_jwt).start()
    #     RunAsyncToRequestResponse("scenario/{}/table".format(scenario_id), my_jwt).start()
    #     my_timing.log_timing("start RunAsyncToRequestResponse")
    #
    # print "all done"

    my_timing.log_timing("after to_dict()")
    response = {"status": "success"}
    response["_timing"] = my_timing.to_dict()

    return jsonify_fast_no_sort(response)




@app.route('/scenario/<scenario_id>', methods=['GET'])
@jwt_optional
# @my_memcached.cached(timeout=7*24*60*60)
def live_scenario_id_get(scenario_id):


    my_timing = TimingMessages()
    my_saved_scenario = get_saved_scenario(scenario_id)
    my_timing.log_timing("after setting live scenario")
    response = my_saved_scenario.to_dict_definition()
    my_timing.log_timing("after to_dict()")
    response["_timing"] = my_timing.to_dict()
    response = jsonify_fast(response)
    cache_tags_list = ["scenario", u"package_{}".format(my_saved_scenario.package_id), u"scenario_{}".format(scenario_id)]
    # print "cache_tags for /scenario", cache_tags_list
    response.headers["Cache-Tag"] = u",".join(cache_tags_list)
    return response



@app.route('/scenario/<scenario_id>/summary', methods=['GET'])
@jwt_optional
# @my_memcached.cached(timeout=7*24*60*60)
def scenario_id_summary_get(scenario_id):
    my_timing = TimingMessages()
    my_saved_scenario = get_saved_scenario(scenario_id)
    my_timing.log_timing("after setting live scenario")
    my_timing.log_timing("after to_dict()")
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_summary())

@app.route('/scenario/<scenario_id>/journals', methods=['GET'])
@jwt_optional
def scenario_id_journals_get(scenario_id):
    my_saved_scenario = get_saved_scenario(scenario_id)
    response = jsonify_fast_no_sort(my_saved_scenario.to_dict_journals())
    cache_tags_list = ["scenario", u"package_{}".format(my_saved_scenario.package_id), u"scenario_{}".format(scenario_id)]
    response.headers["Cache-Tag"] = u",".join(cache_tags_list)
    return response



@app.route('/scenario/<scenario_id>/raw', methods=['GET'])
@jwt_optional
@cached()
def scenario_id_raw_get(scenario_id):
    my_saved_scenario = get_saved_scenario(scenario_id)
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_raw())

def check_authorized():
    return True

@app.route('/scenario/<scenario_id>/details', methods=['GET'])
@jwt_optional
# @my_memcached.cached(timeout=7*24*60*60)
def scenario_id_details_get(scenario_id):
    my_saved_scenario = get_saved_scenario(scenario_id)
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_details())



@app.route('/scenario/<scenario_id>/table', methods=['GET'])
@jwt_optional
# @my_memcached.cached(timeout=7*24*60*60)
def live_scenario_id_table_get(scenario_id):
    my_saved_scenario = get_saved_scenario(scenario_id)
    response = jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_table())
    cache_tags_list = ["scenario", u"package_{}".format(my_saved_scenario.package_id), u"scenario_{}".format(scenario_id)]
    response.headers["Cache-Tag"] = u",".join(cache_tags_list)
    return response



@app.route('/scenario/<scenario_id>/slider', methods=['GET'])
@jwt_optional
# @my_memcached.cached(timeout=7*24*60*60)
def live_scenario_id_slider_get(scenario_id):

    my_saved_scenario = get_saved_scenario(scenario_id)
    response = jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_slider())
    cache_tags_list = ["scenario", u"package_{}".format(my_saved_scenario.package_id), u"scenario_{}".format(scenario_id)]
    response.headers["Cache-Tag"] = u",".join(cache_tags_list)
    return response


@app.route('/package/<package_id>/apc', methods=['GET'])
@jwt_optional
def live_package_id_apc_get(package_id):
    identity_dict = get_jwt_identity()

    if not identity_dict:
        if not is_authorized_superuser():
            return abort_json(401, "Not authorized, need secret.")

    if package_id.startswith("demo"):
        my_package = Package.query.get("demo")
        my_package.package_id = package_id
    else:
        my_package = Package.query.get(package_id)

    if not my_package:
        abort_json(404, "Package not found")

    if not package_id.startswith("demo"):
        if my_package.account_id != identity_dict["account_id"]:
            abort_json(401, "Not authorized to view this package")

    my_scenario = my_package.unique_saved_scenarios[0]
    scenario_id = my_scenario.scenario_id
    return live_scenario_id_apc_get(scenario_id)


@app.route('/scenario/<scenario_id>/apc', methods=['GET'])
@jwt_optional
def live_scenario_id_apc_get(scenario_id):
    my_saved_scenario = get_saved_scenario(scenario_id)
    response = jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_apc())
    cache_tags_list = ["apc", u"package_{}".format(my_saved_scenario.package_id)]
    response.headers["Cache-Tag"] = u",".join(cache_tags_list)
    return response

@app.route('/scenario/<scenario_id>/report', methods=['GET'])
@jwt_required
def scenario_id_report_get(scenario_id):
    my_saved_scenario = get_saved_scenario(scenario_id)
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_report())


def export_get(my_saved_scenario):

    table_dicts = my_saved_scenario.live_scenario.to_dict_export()["journals"]

    filename = "export.csv"
    with open(filename, "w") as file:
        csv_writer = csv.writer(file, encoding='utf-8')
        meta_keys = table_dicts[0]["meta"].keys()
        keys = meta_keys + table_dicts[0]["table_row"].keys()
        csv_writer.writerow(keys)
        for table_dict in table_dicts:
            row = []
            for my_key in keys:
                if my_key in meta_keys:
                    if my_key in "issn_l":
                        # doing this hacky thing so excel doesn't format the issn as a date :(
                        row.append(u"issn:{}".format(table_dict["meta"][my_key]))
                    else:
                        row.append(table_dict["meta"][my_key])
                else:
                    row.append(table_dict["table_row"][my_key])
            csv_writer.writerow(row)

    with open(filename, "r") as file:
        contents = file.readlines()

    return contents


@app.route('/scenario/<scenario_id>/export.csv', methods=['GET'])
@jwt_required
def scenario_id_export_csv_get(scenario_id):
    my_saved_scenario = get_saved_scenario(scenario_id)
    contents = export_get(my_saved_scenario)
    # return Response(contents, mimetype="text/text")
    return Response(contents, mimetype="text/csv")


@app.route('/scenario/<scenario_id>/export', methods=['GET'])
@jwt_required
def scenario_id_export_get(scenario_id):
    my_saved_scenario = get_saved_scenario(scenario_id)
    contents = export_get(my_saved_scenario)
    return Response(contents, mimetype="text/text")


@app.route('/package/<package_id>/scenario', methods=["POST"])
@jwt_optional
def scenario_post(package_id):
    new_scenario_id = request.json.get('id', shortuuid.uuid()[0:8])
    new_scenario_name = request.json.get('name', "New Scenario")

    if package_id.startswith("demo-package") and not new_scenario_id.startswith("demo-scenario-"):
        new_scenario_id = "demo-scenario-" + new_scenario_id

    my_saved_scenario_to_copy_from = None

    copy_scenario_id = request.args.get('copy', None)
    if copy_scenario_id:
        my_saved_scenario_to_copy_from = get_saved_scenario(copy_scenario_id)

    new_saved_scenario = SavedScenario(False, new_scenario_id, None)
    new_saved_scenario.package_id = package_id
    new_saved_scenario.scenario_name = new_scenario_name
    new_saved_scenario.is_base_scenario = False
    db.session.add(new_saved_scenario)
    safe_commit(db)


    if my_saved_scenario_to_copy_from:
        dict_to_save = my_saved_scenario_to_copy_from.to_dict_saved()
    else:
        dict_to_save = new_saved_scenario.to_dict_saved()

    save_raw_scenario_to_db(new_scenario_id, dict_to_save, get_ip(request))

    my_new_scenario = get_saved_scenario(new_scenario_id)

    return jsonify_fast_no_sort(my_new_scenario.to_dict_meta())


@app.route('/scenario/<scenario_id>', methods=['DELETE'])
@jwt_optional
def scenario_delete(scenario_id):
    # just delete it out of the table, leave the saves
    # doing it this way makes sure we have permission to acces and therefore delete the scenario
    my_saved_scenario = get_saved_scenario(scenario_id)
    command = "delete from jump_package_scenario where scenario_id = '{}'".format(scenario_id)

    with get_db_cursor() as cursor:
        cursor.execute(command)

    return jsonify_fast_no_sort({"response": "success"})


@app.route('/debug/export', methods=['GET'])
def debug_export_get():
    scenario_id = "demo-debug"
    my_saved_scenario = get_saved_scenario(scenario_id)
    contents = export_get(my_saved_scenario)
    return Response(contents, mimetype="text/text")



@app.route('/admin/change_password', methods=['GET'])
@app.route('/admin/change-password', methods=['GET'])
def admin_change_password():
    username = request.args.get('username')
    old_password = request.args.get('old_password')
    if not old_password:
        old_password = request.args.get('old-password')
    new_password = request.args.get('new_password')
    if not new_password:
        new_password = request.args.get('new-password')
    if not username or not old_password or not new_password:
        return abort_json(400, "Missing parameters:  need username, old-password, new-password")

    my_account = Account.query.filter(Account.username == username).first()

    if not my_account:
        return abort_json(401, "Bad username or bad old password")
    if not check_password_hash(my_account.password_hash, old_password)\
        and (os.getenv("JWT_SECRET_KEY") != old_password):
        return abort_json(401, "Bad username or bad old password")
    my_account.password_hash = generate_password_hash(new_password)
    safe_commit(db)

    return jsonify({'message': "Password updated successfully",
                    "username": username,
                    "display_name": my_account.display_name})


@app.route('/admin/register', methods=['GET'])
def admin_register_user():
    if not is_authorized_superuser():
        abort_json(500, "Secret doesn't match, not saving user in database")

    new_account = Account()
    new_account.username = request.args.get('username')
    new_account.password_hash = generate_password_hash(request.args.get('password'))
    new_account.display_name = request.args.get('name',  None)
    db.session.add(new_account)
    safe_commit(db)

    return jsonify({'message': 'User registered successfully',
                    "username": request.args.get('username')})


@app.route('/debug/journal/<issn_l>', methods=['GET'])
def jump_debug_issn_get(issn_l):
    subscribe = str2bool(request.args.get('subscribe', "false"))
    scenario_id = "demo-debug"
    my_saved_scenario = get_saved_scenario(scenario_id)
    scenario = my_saved_scenario.live_scenario
    my_journal = scenario.get_journal(issn_l)
    if subscribe:
        my_journal.set_subscribe_custom()
    if not my_journal:
        abort_json(404, "journal not found")
    return jsonify_fast_no_sort({"_settings": scenario.settings.to_dict(), "journal": my_journal.to_dict_details()})


@app.route('/debug/scenario/journals', methods=['GET'])
def jump_debug_journals_get():
    scenario_id = "demo-debug"
    my_saved_scenario = get_saved_scenario(scenario_id)
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_journals())

@app.route('/debug/scenario/table', methods=['GET'])
def jump_debug_table_get():
    scenario_id = "demo-debug"
    my_saved_scenario = get_saved_scenario(scenario_id)
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_table(5000))

@app.route('/debug/scenario/slider', methods=['GET'])
def jump_debug_slider_get():
    scenario_id = "demo-debug"
    my_saved_scenario = get_saved_scenario(scenario_id)
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_slider())

@app.route('/debug/scenario/apc', methods=['GET'])
def jump_debug_apc_get():
    scenario_id = "demo-debug"
    my_saved_scenario = get_saved_scenario(scenario_id)
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_apc(5000))

@app.route('/debug/counter/<package_id>', methods=['GET'])
def jump_debug_counter_package_id(package_id):
    if not is_authorized_superuser():
        return abort_json(401, "Not authorized, need secret.")

    if package_id.startswith("demo"):
        my_package = Package.query.get("demo")
        my_package.package_id = package_id
    else:
        my_package = Package.query.get(package_id)
    response = my_package.get_package_counter_breakdown()
    return jsonify_fast_no_sort(response)



@app.route('/debug/ids', methods=['GET'])
def jump_debug_ids():
    if not is_authorized_superuser():
        return abort_json(401, "Not authorized, need secret.")

    response = get_ids()
    return jsonify_fast(response)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5004))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True, use_reloader=True)








