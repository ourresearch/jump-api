# coding: utf-8

import timeout_decorator
from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify
from flask import url_for
from flask import Response
from flask import send_file
from flask import g
from flask_jwt_extended import jwt_required, jwt_optional, create_access_token, get_jwt_identity
from pyinstrument import Profiler
from sqlalchemy import or_
from sqlalchemy import func as sql_func
from werkzeug.security import safe_str_cmp
from werkzeug.security import generate_password_hash, check_password_hash

import os
import sys
import simplejson as json
from collections import defaultdict
from time import sleep
from time import time
import unicodecsv as csv
import shortuuid
import datetime
import requests
import tempfile
import random
from collections import OrderedDict
import boto3

# from app import my_memorycache_dict
from app import app
from app import logger
from app import jwt
from app import db
# from app import my_memcached  # disable memcached
from app import get_db_cursor
from app import reset_cache

import ror_search
import password_reset
import prepared_demo_publisher
from emailer import create_email, send
from counter import Counter, CounterInput
from grid_id import GridId
from scenario import Scenario
from institution import Institution
from journal_price import JournalPrice, JournalPriceInput
from package import Package
from package import get_ids
from permission import Permission, UserInstitutionPermission
from perpetual_access import PerpetualAccess, PerpetualAccessInput
from ror_id import RorId, RorGridCrosswalk
from saved_scenario import SavedScenario, default_scenario
from saved_scenario import get_latest_scenario
from saved_scenario import save_raw_scenario_to_db
from saved_scenario import save_raw_member_institutions_included_to_db
from saved_scenario import get_latest_scenario_raw
from scenario import get_common_package_data, refresh_cached_prices_from_db, refresh_perpetual_access_from_db
from scenario import get_clean_package_id
from consortium import get_consortium_ids
from consortium import Consortium

from util import jsonify_fast
from util import jsonify_fast_no_sort
from util import str2bool
from util import elapsed
from util import abort_json
from util import safe_commit
from util import TimingMessages
from util import get_ip
from util import response_json
from user import User, default_password
from app import logger

from app import DEMO_PACKAGE_ID

s3_client = boto3.client("s3")
print "made s3_client"

def s3_cache_get(url):
    print u"in cache_get with", url

    filename = u"{}.json".format(url.replace("/", "~"))
    s3_clientobj = s3_client.get_object(Bucket="unsub-cache", Key=filename)
    contents_string = s3_clientobj["Body"].read().decode("utf-8")
    contents_json = json.loads(contents_string)
    return contents_json


def authenticate_for_publisher(publisher_id, required_permission):
    package = Package.query.get(publisher_id)

    if not package:
        abort_json(404, "Publisher not found")

    if not is_authorized_superuser():
        auth_user = authenticated_user()

        if not auth_user:
            abort_json(401, u"Must be logged in.")

        if not package.institution:
            abort_json(400, u"Publisher is not owned by any institution.")

        if not auth_user.has_permission(package.institution.id, required_permission):
            consortium_package = None
            if package.consortium_package_id:
                consortium_package = Package.query.get(package.consortium_package_id)

            if not consortium_package:
                abort_json(403, u"Missing required permission '{}' for institution {}.".format(
                    required_permission.name,
                    package.institution.id)
                )

            if not auth_user.has_permission(consortium_package.institution.id, required_permission):
                abort_json(403, u"Missing required permission '{}' for institution {}.".format(
                    required_permission.name,
                    consortium_package.institution.id)
                )

    return package


def authenticated_user():
    jwt_identity = get_jwt_identity()
    user_id = jwt_identity.get("user_id", None) if jwt_identity else None
    return User.query.get(user_id) if user_id else None


def lookup_user(user_id=None, email=None, username=None):
    id_user = User.query.filter(User.id == user_id).scalar() if user_id is not None else None
    email_user = User.query.filter(sql_func.lower(User.email) == email.lower()).scalar() if email is not None else None
    username_user = User.query.filter(User.username == username).scalar() if username is not None else None

    user_ids = set([user.id for user in [id_user, email_user, username_user] if user])
    if len(user_ids) > 1:
        return abort_json(400, u"Email, username, and user id are in use by different users.")

    return id_user or email_user or username_user


@app.before_request
def before_request_stuff():
    if app.config["PROFILE_REQUESTS"]:
        g.profiler = Profiler()
        g.profiler.start()

@app.after_request
def after_request_stuff(resp):
    sys.stdout.flush()  # without this jason's heroku local buffers forever
    #support CORS
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS, PUT, DELETE, PATCH"
    resp.headers["Access-Control-Allow-Headers"] = "Origin, X-Requested-With, Content-Type, Accept, Authorization, Cache-Control"
    resp.headers["Access-Control-Expose-Headers"] = "Authorization, Cache-Control"
    resp.headers["Access-Control-Allow-Credentials"] = "true"

    # make not cacheable because the GETs change after parameter change posts!
    resp.cache_control.max_age = 0
    resp.cache_control.no_cache = True

    if app.config["PROFILE_REQUESTS"]:
        g.profiler.stop()
        print(g.profiler.output_text(unicode=True, color=True, show_all=True))

    return resp


class TimeoutError(Exception):
    pass


@app.errorhandler(500)
def error_500(e):
    response = jsonify({"message": "Internal Server Error"})
    response.status_code = 500
    return response


@app.errorhandler(TimeoutError)
def error_timeout(e):
    response = jsonify({"message": "Timeout"})
    response.status_code = 500
    return response


@app.route("/", methods=["GET", "POST"])
def base_endpoint():
    return jsonify_fast({
        "version": "0.0.1",
        "msg": "Don't panic"
    })


# @app.route("/favicon.ico")
# def favicon():
#     return redirect(url_for("static", filename="img/favicon.ico", _external=True, _scheme="https"))


@app.route("/scenario/<scenario_id>/journal/<issn_l>", methods=["GET"])
@jwt_optional
def jump_scenario_issn_get(scenario_id, issn_l):
    my_saved_scenario = get_saved_scenario(scenario_id, required_permission=Permission.view())
    scenario = my_saved_scenario.live_scenario

    consortium_ids = get_consortium_ids()
    if scenario_id in [d["scenario_id"] for d in consortium_ids]:
        my_consortium = Consortium(scenario_id)
        response = {"journal": {"member_institutions": my_consortium.to_dict_journal_zoom(issn_l)}}
    else:
        my_journal = scenario.get_journal(issn_l)
        response = {"_settings": scenario.settings.to_dict(), "journal": my_journal.to_dict_details()}
    response = jsonify_fast_no_sort(response)
    return response


@app.route("/live/data/common/<package_id>", methods=["GET"])
def jump_data_package_id_get(package_id):
    if not is_authorized_superuser():
        abort_json(500, "Secret doesn't match, not getting package")

    if package_id.startswith("demo"):
        package_id = DEMO_PACKAGE_ID
    response = get_common_package_data(package_id)

    response = jsonify_fast_no_sort(response)
    return response



@app.route("/login", methods=["GET", "POST"])
def login():
    return user_login()


def make_identity_dict(user):
    # Identity can be any data that is json serializable.  Include timestamp so is unique for each demo start.
    return {
        "user_id": user.id,
        "login_uuid": shortuuid.uuid()[0:10],
        "created": datetime.datetime.utcnow().isoformat(),
        "is_demo_user": user.is_demo_user
    }


# copies the existing /login route but uses the new jump_user table

@app.route("/user/login", methods=["POST"])
def user_login():
    print u"in user_login"
    request_args = request.args
    if request.is_json:
        request_args = request.json

    username = request_args.get("username", None)
    email = request_args.get("email", None)
    password = request_args.get("password", u"")

    if username is None and email is None:
        return abort_json(400, "Username or email parameter is required.")

    print u"before lookup_user"
    login_user = lookup_user(email=email, username=username)

    # maybe the username was passed as an email
    if not login_user and email and not username:
        login_user = lookup_user(username=email)

    print u"after lookup_user"

    if not login_user:
        return abort_json(404, u"User does not exist.")

    if not check_password_hash(login_user.password_hash, password) and os.getenv("JWT_SECRET_KEY") != password:
        return abort_json(403, u"Bad password.")

    identity_dict = make_identity_dict(login_user)
    print "identity_dict", identity_dict
    logger.info(u"login to account {} with {}".format(login_user.username, identity_dict))
    access_token = create_access_token(identity=identity_dict)

    login_user_permissions =  db.session.query(UserInstitutionPermission).filter(
        UserInstitutionPermission.user_id == login_user.id,
    ).first()

    print u"done query"

    if not login_user_permissions:
        assign_demo_institution(login_user)
        safe_commit(db)

    print u"done assign_demo_institution"

    return jsonify({"access_token": access_token})


@app.route("/user/demo", methods=["POST"])
def register_demo_user():
    request_args = request.args
    if request.is_json:
        request_args = request.json

    email = request_args.get("email", None)
    username = request_args.get("username", None)
    display_name = request_args.get("name", u"Anonymous User")
    password = request_args.get("password", default_password())

    if not email:
        return abort_json(400, u"Email parameter is required.")

    existing_user = lookup_user(email=email, username=username)

    if existing_user:
        if check_password_hash(existing_user.password_hash, password):
            return user_login()
        else:
            if lookup_user(email=email):
                return abort_json(409, u"A user with email {} already exists.".format(email))
            else:
                return abort_json(409, u"A user with username {} already exists.".format(username))

    demo_user = User()
    demo_user.username = username
    demo_user.email = email
    demo_user.password_hash = generate_password_hash(password)
    demo_user.display_name = display_name
    demo_user.is_demo_user = True

    db.session.add(demo_user)

    if u"@" in email and email.split(u"@")[-2].lower().endswith(u"+nocache"):
        use_prepared_publisher = False
    else:
        use_prepared_publisher = True

    assign_demo_institution(demo_user, use_prepared_publisher=use_prepared_publisher)

    if safe_commit(db):
        welcome_email = create_email(email, u"Welcome to Unsub", "demo_user", {})
        send(welcome_email, for_real=True)

        identity_dict = make_identity_dict(demo_user)
        logger.info(u"login to account {} with {}".format(demo_user.username, identity_dict))
        access_token = create_access_token(identity=identity_dict)

        return jsonify({"access_token": access_token})
    else:
        return abort_json(500, u"Database error.")


def assign_demo_institution(user, use_prepared_publisher=True):
    demo_institution = Institution()
    demo_institution.display_name = "Demo University"
    demo_institution.is_demo_institution = True

    db.session.add(demo_institution)
    db.session.add(GridId(institution_id=demo_institution.id, grid_id="grid.433631.0"))
    db.session.add(RorId(institution_id=demo_institution.id, ror_id="00xbe3815"))

    for permission in [Permission.view(), Permission.modify(), Permission.admin()]:
        user_perm = UserInstitutionPermission()
        user_perm.permission_id = permission.id,
        user_perm.user_id = user.id,
        user_perm.institution_id = demo_institution.id
        db.session.add(user_perm)

    demo_publisher = prepared_demo_publisher.get_demo_publisher(demo_institution, use_prepared=use_prepared_publisher)
    db.session.add(demo_publisher)


def notify_changed_permissions(user, admin, old_permissions, new_permissions):
    if old_permissions != new_permissions and user.id != admin.id and user.email:
        diff_lines = []

        for institution_id in set(old_permissions.keys() + new_permissions.keys()):
            old_names = set(old_permissions.get(institution_id, {}).get("permissions", []))
            new_names = set(new_permissions.get(institution_id, {}).get("permissions", []))
            if old_names != new_names:
                institution_name = old_permissions.get(institution_id, new_permissions.get(institution_id))[
                    "institution_name"]
                diff_lines.append(u"{} ({}):".format(institution_name, institution_id))
                diff_lines.append(u"old: {}".format(u",".join(old_names) if old_names else "[None]"))
                diff_lines.append(u"new: {}".format(u",".join(new_names) if new_names else "[None]"))
                diff_lines.append(u"")

        email = create_email(user.email, u"Your Unsub permissions were changed.", "changed_permissions",
                             {"data": {
                                 "display_name": user.display_name,
                                 "admin_name": admin.display_name,
                                 "admin_email": admin.email,
                                 "diff": u"\n".join(diff_lines)
                             }})

        send(email, for_real=True)


@app.route("/user/new", methods=["POST"])
@jwt_required
def register_new_user():
    if not request.is_json:
        return abort_json(400, u"This post requires data.")

    auth_user = authenticated_user()

    if not auth_user:
        return abort_json(401, u"Must be logged in.")

    new_email = request.json.get("email", None)
    new_username = request.json.get("username", None)
    display_name = request.json.get("name", u"Anonymous User")
    password = request.json.get("password", default_password())

    if not new_email:
        return abort_json(400, u"Email parameter is required.")

    req_user = lookup_user(email=new_email, username=new_username)

    new_user_created = False

    if not req_user:
        new_user_created = True
        req_user = User()
        req_user.username = new_username
        req_user.email = new_email
        req_user.password_hash = generate_password_hash(password)
        req_user.display_name = display_name
        req_user.is_demo_user = auth_user.is_demo_user

        db.session.add(req_user)

    permissions_by_institution = defaultdict(set)
    for permission_request in request.json.get("user_permissions", []):
        try:
            for permission_name in permission_request["permissions"]:
                permissions_by_institution[permission_request["institution_id"]].add(permission_name)
        except KeyError as e:
            return abort_json(400, u"Missing key in user_permissions object: {}".format(e.message))

    safe_commit(db)

    old_permissions = req_user.to_dict_permissions()

    for institution_id, permission_names in permissions_by_institution.items():
        if auth_user.has_permission(institution_id, Permission.admin()):

            command = "delete from jump_user_institution_permission where user_id = '{}' and institution_id = '{}'".format(
                req_user.id, institution_id)
            with get_db_cursor() as cursor:
                cursor.execute(command)

            safe_commit(db)

            for permission_name in permission_names:
                permission = Permission.get(permission_name)
                if permission:
                    user_perm = UserInstitutionPermission()
                    user_perm.permission_id = permission.id,
                    user_perm.user_id = req_user.id,
                    user_perm.institution_id = institution_id
                    db.session.add(user_perm)
                else:
                    return abort_json(400, u"Unknown permission: {}.".format(permission_name))
        else:
            return abort_json(403, u"Not authorized to create users for institution {}".format(institution_id))

    safe_commit(db)

    db.session.refresh(req_user)
    new_permissions = req_user.to_dict_permissions()

    if new_user_created:
        email_institution = Institution.query.get(
            permissions_by_institution.keys()[0]
        ) if permissions_by_institution else None

        email = create_email(req_user.email, u"Welcome to Unsub", "new_user", {"data": {
            "email": new_email,
            "password": password,
            "institution_name": email_institution and email_institution.display_name
        }})

        send(email, for_real=True)
    else:
        notify_changed_permissions(req_user, auth_user, old_permissions, new_permissions)

    return jsonify_fast_no_sort(req_user.to_dict())


@app.route("/user/me", methods=["POST", "GET"])
@jwt_required
def my_user_info():
    login_user = authenticated_user()

    if not login_user:
        return abort_json(401, u"Must be logged in.")

    if request.method == "POST":
        if not request.is_json:
            return abort_json(400, u"Post a User object to change properties.")

        if "email" in request.json:
            email = request.json["email"]
            if not email:
                return abort_json(400, u"Can't remove your email address.")
            email_user = lookup_user(email=email)
            if email_user and email_user.id != login_user.id:
                return abort_json(409, u"A user with email {} already exists.".format(email))
            login_user.email = email
        if "username" in request.json:
            username = request.json["username"]
            username_user = lookup_user(username=username)
            if username_user and username_user.id != login_user.id:
                return abort_json(409, u"A user with username {} already exists.".format(username))
            login_user.username = username
        if "name" in request.json:
            login_user.display_name = request.json["name"]
        if "password" in request.json:
            login_user.password_hash = generate_password_hash(request.json["password"] or u"")

        db.session.merge(login_user)
        safe_commit(db)

    return jsonify_fast_no_sort(login_user.to_dict())


@app.route("/user/id/<user_id>", methods=["GET"], defaults={"email": None, "username": None})
@app.route("/user/email/<email>", methods=["GET"], defaults={"user_id": None, "username": None})
@app.route("/user/username/<username>", methods=["GET"], defaults={"email": None, "user_id": None})
def user_info(user_id, email, username):
    user = lookup_user(user_id=user_id, email=email, username=username)

    if user:
        return jsonify_fast_no_sort(user.to_dict())
    else:
        return abort_json(404, u"User does not exist.")


@app.route("/user-permissions", methods=["GET", "POST"])
@jwt_optional
def user_permissions():
    request_args = dict(request.args)
    request_args.update(request.form)

    if request.is_json:
        request_args.update(request.json)

    user_id = request_args.get("user_id", None)
    email = request_args.get("user_email", None)
    username = request_args.get("username", None)
    institution_id = request_args.get("institution_id", None)

    if not (user_id or email or username):
        return abort_json(400, u"A user_id, user_email, or username parameter is required.")

    if isinstance(user_id, list):
        user_id = user_id[0]

    if isinstance(email, list):
        email = email[0]

    if isinstance(username, list):
        username = username[0]

    if not institution_id:
        return abort_json(400, u"Missing institution_id parameter.")
    elif isinstance(institution_id, list):
        institution_id = institution_id[0]

    query_user = lookup_user(user_id=user_id, email=email, username=username)

    if not query_user:
        return abort_json(404, u"User does not exist.")

    if request.method == "POST":
        old_permissions = query_user.to_dict_permissions()

        auth_user = authenticated_user()
        if not auth_user:
            return abort_json(401, u"Must be logged in.")

        inst = Institution.query.get(institution_id)
        if not inst:
            return abort_json(404, u"Institution does not exist.")

        if not auth_user.has_permission(institution_id, Permission.admin()):
            return abort_json(403, u"Must have Admin permission to modify user permissions.")

        permission_names = request_args.get("permissions", request_args.get("data", None))

        if permission_names is None:
            return abort_json(400, u"Missing permissions list.")

        if not isinstance(permission_names, list):
            permission_names = [permission_names]

        if query_user.id == auth_user.id and Permission.admin().name not in permission_names:
            return abort_json(400, u"Cannot revoke own admin permission.")

        command = "delete from jump_user_institution_permission where user_id = '{}' and institution_id = '{}'".format(
            query_user.id, institution_id)
        with get_db_cursor() as cursor:
            cursor.execute(command)

        for permission_name in permission_names:
            permission = Permission.get(permission_name)
            if permission:
                user_perm = UserInstitutionPermission()
                user_perm.permission_id = permission.id,
                user_perm.user_id = query_user.id
                user_perm.institution_id = institution_id
                db.session.add(user_perm)
            else:
                return abort_json(400, u"Unknown permission: {}.".format(permission_name))

        safe_commit(db)

        db.session.refresh(query_user)
        new_permissions = query_user.to_dict_permissions()

        notify_changed_permissions(query_user, auth_user, old_permissions, new_permissions)

    return jsonify_fast_no_sort(query_user.to_dict_permissions().get(institution_id, {}))

# @app.route("/institution/institution-Afxc4mAYXoJH", methods=["GET"])
# @jwt_optional
# def institution_jisc(institution_id="institution-Afxc4mAYXoJH"):
#     print u"in institution_jisc"
#
#     inst = Institution.query.get(institution_id)
#     if not inst:
#         return abort_json(404, u"Institution does not exist.")
#
#     if not authorize_institution(inst, Permission.view()):
#         return abort_json(403, u"Must have read permission to get institution properties.")
#
#     print "authorized"
#     response_dict = s3_cache_get("institution/institution-Afxc4mAYXoJH")
#
#     return jsonify_fast_no_sort(response_dict)



@app.route("/institution/<institution_id>", methods=["POST", "GET"])
@jwt_optional
def institution(institution_id):

    inst = Institution.query.get(institution_id)
    if not inst:
        return abort_json(404, u"Institution does not exist.")

    if request.method == "POST":
        if not authorize_institution(inst, Permission.modify()):
            return abort_json(403, u"Must have Write permission to modify institution properties.")

        request_args = request.args
        if request.is_json:
            request_args = request.json

        display_name = request_args.get("name", None)
        if display_name:
            inst.display_name = display_name

        db.session.add(inst)
        safe_commit(db)

    if not authorize_institution(inst, Permission.view()):
        return abort_json(403, u"Must have read permission to get institution properties.")

    return jsonify_fast_no_sort(inst.to_dict())


@app.route("/institution/<institution_id>/ror/<ror_id>", methods=["POST", "DELETE"])
@jwt_optional
def institution_ror_id(institution_id, ror_id):
    inst = Institution.query.get(institution_id)
    if not inst:
        return abort_json(404, u"Institution does not exist.")

    if not authorize_institution(inst, Permission.modify()):
        return abort_json(403, u"Must have Write permission to modify institution properties.")

    grid_ids = [x.grid_id for x in RorGridCrosswalk.query.filter(RorGridCrosswalk.ror_id == ror_id).all()]

    if request.method == "POST":
        if not grid_ids:
            return abort_json(404, u"Unknown ROR '{}'.".format(ror_id))

        db.session.merge(RorId(institution_id=inst.id, ror_id=ror_id))
        for grid_id in grid_ids:
            db.session.merge(GridId(institution_id=inst.id, grid_id=grid_id))
    elif request.method == "DELETE":

        command = "delete from jump_ror_id where ror_id = '{}' and institution_id = '{}'".format(
            ror_id, institution_id)
        with get_db_cursor() as cursor:
            cursor.execute(command)

        for grid_id in grid_ids:
            command = "delete from jump_grid_id where grid_id = '{}' and institution_id = '{}'".format(
                grid_id, institution_id)
            with get_db_cursor() as cursor:
                cursor.execute(command)

    db.session.commit()

    return jsonify_fast_no_sort(inst.to_dict())


# curl -s -X POST -H 'Accept: application/json' -H "Content-Type: application/json' --data '{"username":"test","password":"password","rememberMe":false}' http://localhost:5004/login
# curl -H 'Accept: application/json' -H "Authorization: Bearer ${TOKEN}" http://localhost:5004/protected

# Protect a view with jwt_required, which requires a valid access token
# in the request to access.
@app.route("/protected", methods=["GET"])
@jwt_required
def protected():
    # Access the identity of the current user with get_jwt_identity
    identity_dict = get_jwt_identity()
    return jsonify({"logged_in_as": identity_dict["user_id"]})

# Protect a view with jwt_required, which requires a valid access token
# in the request to access.
@app.route("/super", methods=["GET"])
def super():
    if not is_authorized_superuser():
        abort_json(403, "Secret doesn't match, not getting package")
    return jsonify({"success": True})


def is_authorized_superuser():
    secret = request.args.get("secret", "")
    if secret and safe_str_cmp(secret, os.getenv("JWT_SECRET_KEY")):
        return True
    return False


def authorize_institution(auth_institution, required_permission):
    if is_authorized_superuser():
        return True

    auth_user = authenticated_user()

    if not auth_user:
        return abort_json(401, u"Authentication required.")

    return auth_user.has_permission(auth_institution.id, required_permission)


def get_saved_scenario(scenario_id, test_mode=False, required_permission=None):
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
        abort_json(404, "Scenario {} not found.".format(scenario_id))

    # if not test_mode:
    #     print "test_mode", test_mode
    #     print "is_authorized_superuser()", is_authorized_superuser()
    if required_permission:
        if my_saved_scenario.package.institution_id:
            authenticate_for_publisher(my_saved_scenario.package.package_id, required_permission)
        else:
            abort_json(
                400,
                u"Scenario package {} has no institution_id. Can't decide how to authenticate.".format(
                    my_saved_scenario.package.package_id
                )
            )

    my_saved_scenario.set_live_scenario(None)

    return my_saved_scenario


@app.route("/account", methods=["GET"])
@jwt_required
def live_account_get():
    return abort_json(404, "Removed. Use /user/me or /institution/<institution_id>.")


def get_jwt():
    if request.args and request.args.get("jwt", None):
        return request.args.get("jwt")
    if "Authorization" in request.headers and request.headers["Authorization"] and "Bearer " in request.headers["Authorization"]:
        return request.headers["Authorization"].replace("Bearer ", "")
    return None


# @app.route("/publisher/package-3WkCDEZTqo6S", methods=["GET"])
# @jwt_optional
# def get_package_jisc_package_3WkCDEZTqo6S(package_id="package-3WkCDEZTqo6S"):
#     authenticate_for_publisher(package_id, Permission.view())
#     print u"in get_package_package_3WkCDEZTqo6S"
#     response_dict = s3_cache_get("publisher/package-3WkCDEZTqo6S")
#     return jsonify_fast_no_sort(response_dict)


@app.route("/publisher/<package_id>", methods=["GET"])
@jwt_optional
def get_package(package_id):
    authenticate_for_publisher(package_id, Permission.view())

    package = Package.query.filter(Package.package_id == package_id).scalar()
    package_dict = package.to_package_dict()
    response = jsonify_fast_no_sort(package_dict)
    return response


@app.route("/publisher/<publisher_id>", methods=["POST"])
@jwt_required
# @timeout_decorator.timeout(25, timeout_exception=TimeoutError)
def update_publisher(publisher_id):
    authenticate_for_publisher(publisher_id, required_permission=Permission.modify())

    publisher = Package.query.filter(Package.package_id == publisher_id).scalar()

    if not request.is_json:
        return abort_json(400, u"Post an object to change properties.")

    if "name" in request.json:
        publisher.package_name = request.json["name"]

    if "cost_bigdeal" in request.json:
        try:
            cost = float(request.json["cost_bigdeal"]) if request.json["cost_bigdeal"] is not None else None
        except (ValueError, TypeError):
            return abort_json(400, u"Couln't parse cost_bigdeal '{}' as a number.".format(request.json["cost_bigdeal"]))

        publisher.big_deal_cost = cost

    if "is_deleted" in request.json:
        if not isinstance(request.json["is_deleted"], bool):
            return abort_json(400, u"is_deleted must be a boolean. got {}.".format(request.json["is_deleted"]))

        publisher.is_deleted = request.json["is_deleted"]

    db.session.merge(publisher)
    safe_commit(db)

    package_dict = publisher.to_package_dict()
    return jsonify_fast_no_sort(package_dict)


@app.route("/publisher/new", methods=["POST"])
@jwt_required
# @timeout_decorator.timeout(25, timeout_exception=TimeoutError)
def new_publisher():
    auth_user = authenticated_user()

    if not request.is_json:
        return abort_json(400, u"Post an object to change properties.")

    if "institution_id" not in request.json:
        return abort_json(400, u"institution_id is required")

    pub_institution = Institution.query.get(request.json["institution_id"])

    if not pub_institution:
        abort_json(404, u"institution not found")

    if not auth_user.has_permission(pub_institution.id, Permission.modify()):
        abort_json(401, u"must have Modify permission for institution {}".format(pub_institution.id))

    if "name" not in request.json:
        return abort_json(400, u"name is required")

    if "publisher" not in request.json:
        return abort_json(400, u"publisher is required")

    publisher = request.json["publisher"].lower()

    if "elsevier" in publisher:
        publisher = "Elsevier"
    elif "wiley" in publisher:
        publisher = "Wiley"
    elif "springer" in publisher:
        publisher = "SpringerNature"
    elif "taylor" in publisher:
        publisher = "TaylorFrancis"
    elif "sage" in publisher:
        publisher = "Sage"
    else:
        return abort_json(400, u"not a supported publisher")

    now = datetime.datetime.utcnow().isoformat()

    new_package = Package()
    new_package.package_id = "package-{}".format(shortuuid.uuid()[0:12])
    new_package.institution_id = pub_institution.id
    new_package.package_name = request.json["name"]
    new_package.publisher = publisher
    new_package.is_demo = pub_institution.is_demo_institution
    new_package.created = now

    db.session.add(new_package)
    safe_commit(db)

    q = """
            insert into jump_apc_authorships (
                select * from jump_apc_authorships_view
                where package_id = '{}' and issn_l in (select issn_l from ricks_journal rj where {}))
        """.format(new_package.package_id, new_package.publisher_where)
    # print "q", q
    with get_db_cursor() as cursor:
        cursor.execute(q)

    package_dict = new_package.to_package_dict()
    return jsonify_fast_no_sort(package_dict)


## examples
# /counter/diff_no_price
# /counter/diff_changed_publisher
# /counter/
@app.route("/package/<package_id>/counter/<diff_type>", methods=["GET"])
@jwt_optional
def jump_debug_counter_diff_type_package_id(package_id, diff_type):
    authenticate_for_publisher(package_id, Permission.view())

    if package_id.startswith("demo"):
        my_package = Package.query.get("demo")
        my_package.package_id = package_id
    else:
        my_package = Package.query.get(package_id)

    if not my_package:
        abort_json(404, "Package not found")

    rows = getattr(my_package, "get_{}".format(diff_type))
    my_list = []
    for row in rows:
        my_list += [{"issn_l": row["issn_l"], "title": row.get("title", ""), "num_2018_downloads": row.get("num_2018_downloads", None)}]

    return jsonify_fast_no_sort({"count": len(rows), "list": my_list})


def _long_error_message():
    return u"Something is wrong with the input file. This is placeholder for a message describing it. It's a little longer than the longest real message."


def _json_to_temp_file(req):
    if "file" in req.json and "name" in req.json:
        file_name = req.json["name"] or u""
        suffix = u".{}".format(file_name.split(".")[-1]) if u"." in file_name else ""
        temp_filename = tempfile.mkstemp(suffix=suffix)[1]
        with open(temp_filename, "wb") as temp_file:
            temp_file.write(req.json["file"].split(",")[-1].decode("base64"))
        return temp_filename
    else:
        return None


def _load_package_file(package_id, req, table_class):
    temp_file = _json_to_temp_file(req)
    if temp_file:
        load_result = table_class.load(package_id, temp_file, commit=True)
        if load_result["success"]:
            return load_result
        else:
            return abort_json(400, load_result)
    else:
        return abort_json(
            400, u"expected a JSON object like {file: <base64-encoded file>, name: <file name>}"
        )


@app.route("/publisher/<package_id>/counter5_trj1", methods=["GET", "POST", "DELETE"])
@jwt_optional
def jump_counter5_trj1(package_id):
    return jump_counter(package_id)

@app.route("/publisher/<package_id>/counter", methods=["GET", "POST", "DELETE"])
@jwt_optional
# @timeout_decorator.timeout(25, timeout_exception=TimeoutError)
def jump_counter(package_id):
    authenticate_for_publisher(package_id, Permission.view() if request.method == "GET" else Permission.modify())

    if request.method == "GET":
        rows = Counter.query.filter(Counter.package_id == package_id).all()
        if rows:
            return jsonify_fast_no_sort({"rows": [row.to_dict() for row in rows]})
        else:
            return abort_json(404, u"no counter file for package {}".format(package_id))
    elif request.method == "DELETE":
        return jsonify_fast_no_sort({"message": CounterInput.delete(package_id)})
    else:
        if request.args.get("error", False):
            return abort_json(400, _long_error_message())
        else:
            response = _load_package_file(package_id, request, CounterInput)

            # add a package scenario if there isn't already one
            my_package = Package.query.get(package_id)
            if not my_package.scenario_ids:
                new_scenario = default_scenario(package_id)
                db.session.add(new_scenario)
                safe_commit(db)

            return jsonify_fast_no_sort(response)


# @app.route("/publisher/<package_id>/counter/raw", methods=["GET"])
# @jwt_optional
# def jump_get_raw_counter(package_id):
#     authenticate_for_publisher(package_id, Permission.view() if request.method == "GET" else Permission.modify())
#
#     raw = CounterInput.get_raw_upload_object(package_id)
#
#     if not raw:
#         return abort_json(404, u"no raw counter file for package {}".format(package_id))
#
#     return Response(raw["body"], content_type=raw["content_type"], headers=raw["headers"])


@app.route("/publisher/<package_id>/perpetual-access", methods=["GET", "POST", "DELETE"])
@jwt_optional
# @timeout_decorator.timeout(25, timeout_exception=TimeoutError)
def jump_perpetual_access(package_id):
    authenticate_for_publisher(package_id, Permission.view() if request.method == "GET" else Permission.modify())

    if request.method == "GET":
        rows = PerpetualAccess.query.filter(PerpetualAccess.package_id == package_id).all()
        if rows:
            return jsonify_fast_no_sort({"rows": [row.to_dict() for row in rows]})
        else:
            return abort_json(404, u"no perpetual access file for package {}".format(package_id))
    elif request.method == "DELETE":
        return jsonify_fast_no_sort({"message": PerpetualAccessInput.delete(package_id)})
    else:
        if request.args.get("error", False):
            return abort_json(400, _long_error_message())
        else:
            response = {}

            if "file" in request.json and "name" in request.json:
                response.update(_load_package_file(package_id, request, PerpetualAccessInput))

            if not response:
                return abort_json(400, u"expected a JSON object containing {file: <base64-encoded file>, name: <file name>}")

            return jsonify_fast_no_sort(response)


# @app.route("/publisher/<package_id>/perpetual-access/raw", methods=["GET"])
# @jwt_optional
# def jump_get_raw_perpetual_access(package_id):
#     authenticate_for_publisher(package_id, Permission.view() if request.method == "GET" else Permission.modify())
#
#     raw = PerpetualAccessInput.get_raw_upload_object(package_id)
#
#     if not raw:
#         return abort_json(404, u"no raw perpetual access file for package {}".format(package_id))
#
#     return Response(raw["body"], content_type=raw["content_type"], headers=raw["headers"])


@app.route("/publisher/<package_id>/price", methods=["GET", "POST", "DELETE"])
# @timeout_decorator.timeout(25, timeout_exception=TimeoutError)
@jwt_optional
def jump_journal_prices(package_id):
    package = authenticate_for_publisher(package_id, Permission.view() if request.method == "GET" else Permission.modify())

    if request.method == "GET":
        rows = JournalPrice.query.filter(JournalPrice.package_id == package_id).all()
        if rows:
            return jsonify_fast_no_sort({"rows": [row.to_dict() for row in rows]})
        else:
            return abort_json(404, u"no journal price file for package {}".format(package_id))
    elif request.method == "DELETE":
        return jsonify_fast_no_sort({"message": JournalPriceInput.delete(package_id)})
    else:
        if request.args.get("error", False):
            return abort_json(400, _long_error_message())
        else:
            return jsonify_fast_no_sort(_load_package_file(package_id, request, JournalPriceInput))


# @app.route("/publisher/<package_id>/price/raw", methods=["GET"])
# @jwt_optional
# def jump_get_raw_journal_prices(package_id):
#     authenticate_for_publisher(package_id, Permission.view() if request.method == "GET" else Permission.modify())
#
#     raw = JournalPriceInput.get_raw_upload_object(package_id)
#
#     if not raw:
#         return abort_json(404, u"no raw journal price file for package {}".format(package_id))
#
#     return Response(raw["body"], content_type=raw["content_type"], headers=raw["headers"])


def post_subscription_guts(scenario_id, scenario_name=None):
    # need to save before purging, to make sure don"t have race condition
    save_raw_scenario_to_db(scenario_id, request.get_json(), get_ip(request))

    dict_to_save = request.get_json()
    if scenario_name:
        scenario_name = scenario_name.replace(u'"', u'""')
        dict_to_save["scenario_name"] = scenario_name
        save_raw_scenario_to_db(scenario_id, dict_to_save, get_ip(request))

    return


def post_member_institutions_included_guts(scenario_id):
    print "request.get_json()", request.get_json()
    save_raw_member_institutions_included_to_db(scenario_id, request.get_json()["member_institutions"], get_ip(request))



# used for saving scenario contents, also updating scenario name
@app.route("/scenario/<scenario_id>", methods=["POST"])
@app.route("/scenario/<scenario_id>/post", methods=["GET"])  # just for debugging
@jwt_required
def scenario_id_post(scenario_id):

    if not request.is_json:
        return abort_json(400, "This post requires data.")

    get_saved_scenario(scenario_id, required_permission=Permission.modify())

    scenario_name = request.json.get("name", None)
    if scenario_name:
        command = u"update jump_package_scenario set scenario_name = %s where scenario_id = %s"

        with get_db_cursor() as cursor:
            cursor.execute(command, (scenario_name, scenario_id))

    my_timing = TimingMessages()
    post_subscription_guts(scenario_id, scenario_name)
    my_timing.log_timing("after post_subscription_guts()")

    consortium_ids = get_consortium_ids()
    if scenario_id in [d["scenario_id"] for d in consortium_ids]:
        my_consortium = Consortium(scenario_id)
        login_user = authenticated_user()
        my_consortium.queue_for_recompute(login_user.email)
        if not login_user.email:
            return jsonify_fast_no_sort({"status": "success, but no email will be sent because no email set on account"})

    return jsonify_fast_no_sort({"status": "success"})



@app.route("/scenario/<scenario_id>/subscriptions", methods=["POST"])
@jwt_required
def subscriptions_scenario_id_post(scenario_id):
    get_saved_scenario(scenario_id, required_permission=Permission.modify())

    my_timing = TimingMessages()
    post_subscription_guts(scenario_id)
    my_timing.log_timing("post_subscription_guts()")

    response = {"status": "success"}
    response["_timing"] = my_timing.to_dict()

    return jsonify_fast_no_sort(response)


@app.route("/scenario/<scenario_id>/member-institutions", methods=["POST"])
@jwt_required
def member_institutions_scenario_id_post(scenario_id):

    my_timing = TimingMessages()
    print u"request.get_json()", request.get_json()
    post_member_institutions_included_guts(scenario_id)

    my_timing.log_timing("post_member_institutions_included_guts()")

    response = {"status": "success"}
    response["_timing"] = my_timing.to_dict()

    return jsonify_fast_no_sort(response)



@app.route("/scenario/<scenario_id>", methods=["GET"])
@jwt_optional
def live_scenario_id_get(scenario_id):
    my_timing = TimingMessages()
    my_saved_scenario = get_saved_scenario(scenario_id, required_permission=Permission.view())
    my_timing.log_timing("after setting live scenario")
    response = my_saved_scenario.to_dict_definition()

    # these are used by consortium
    response["is_locked_pending_update"] = my_saved_scenario.is_locked_pending_update
    response["update_notification_email"] = my_saved_scenario.update_notification_email
    response["update_percent_complete"] = my_saved_scenario.update_percent_complete

    my_timing.log_timing("after to_dict()")
    response["_timing"] = my_timing.to_dict()
    response = jsonify_fast(response)
    return response


@app.route("/ror/autocomplete/<path:query>", methods=["GET"])
def ror_autocomplete(query):
    return jsonify_fast_no_sort({"results": ror_search.autocomplete(query)})


@app.route("/scenario/<scenario_id>/summary", methods=["GET"])
@jwt_optional
def scenario_id_summary_get(scenario_id):
    my_timing = TimingMessages()
    my_saved_scenario = get_saved_scenario(scenario_id)
    my_timing.log_timing("after setting live scenario")
    my_timing.log_timing("after to_dict()")
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_summary())



# @app.route("/scenario/tGUVWRiN/journals", methods=["GET"])
# @jwt_optional
# def scenario_id_journals_get_jisc_tGUVWRiN(scenario_id="tGUVWRiN"):
#     print u"in scenario_id_journals_get_jisc_tGUVWRiN"
#     response_dict = s3_cache_get("scenario/tGUVWRiN/journals")
#     from saved_scenario import get_latest_scenario_raw
#     my_latest_scenario_raw = get_latest_scenario_raw(scenario_id)
#     response_dict["saved"] = my_latest_scenario_raw
#     return jsonify_fast_no_sort(response_dict)
#
# @app.route("/scenario/scenario-QC2kbHfUhj9W/journals", methods=["GET"])
# @jwt_optional
# def scenario_id_journals_get_jisc_scenario_QC2kbHfUhj9W(scenario_id="scenario-QC2kbHfUhj9W"):
#     print u"in scenario_id_journals_get_jisc_scenario_QC2kbHfUhj9W"
#     response_dict = s3_cache_get("scenario/scenario-QC2kbHfUhj9W/journals")
#     from saved_scenario import get_latest_scenario_raw
#     my_latest_scenario_raw = get_latest_scenario_raw(scenario_id)
#     response_dict["saved"] = my_latest_scenario_raw
#     return jsonify_fast_no_sort(response_dict)
#
# @app.route("/scenario/CBy9gUC3/journals", methods=["GET"])
# @jwt_optional
# def scenario_id_journals_get_jisc_CBy9gUC3(scenario_id="CBy9gUC3"):
#     print u"in scenario_id_journals_get_jisc_CBy9gUC3"
#     response_dict = s3_cache_get("scenario/CBy9gUC3/journals")
#     from saved_scenario import get_latest_scenario_raw
#     my_latest_scenario_raw = get_latest_scenario_raw(scenario_id)
#     response_dict["saved"] = my_latest_scenario_raw
#     return jsonify_fast_no_sort(response_dict)

@app.route("/scenario/<scenario_id>/journals", methods=["GET"])
@jwt_optional
def scenario_id_journals_get(scenario_id):
    start_time = time()

    consortium_ids = get_consortium_ids()
    if scenario_id in [d["scenario_id"] for d in consortium_ids]:
        my_consortium = Consortium(scenario_id)
        my_saved_scenario_dict = my_consortium.to_dict_journals()
    else:
        my_saved_scenario = get_saved_scenario(scenario_id, required_permission=Permission.view())
        my_saved_scenario_dict = my_saved_scenario.to_dict_journals()

    response = jsonify_fast_no_sort(my_saved_scenario_dict)
    return response


@app.route("/scenario/<scenario_id>/member-institutions", methods=["GET"])
@jwt_optional
def scenario_member_institutions_get(scenario_id):
    consortium_ids = get_consortium_ids()
    for row in consortium_ids:
        if scenario_id == row["scenario_id"]:
            my_consortia = Consortium(scenario_id)
            return jsonify_fast_no_sort({"institutions": my_consortia.to_dict_institutions()})
    return abort_json(404, "not a consortium scenario_id")


@app.route("/package/<package_id>/member-institutions", methods=["GET"])
@jwt_optional
def package_member_institutions_get(package_id):
    consortium_ids = get_consortium_ids()
    for row in consortium_ids:
        if package_id == row["package_id"]:
            my_consortia = Consortium(scenario_id=None, package_id=package_id)
            return jsonify_fast_no_sort({"institutions": my_consortia.to_dict_institutions()})
    return abort_json(404, "not a consortium package_id")


def check_authorized():
    return True

@app.route("/scenario/<scenario_id>/details", methods=["GET"])
@jwt_optional
def scenario_id_details_get(scenario_id):
    my_saved_scenario = get_saved_scenario(scenario_id)
    return jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_details())


@app.route("/publisher/<publisher_id>/apc", methods=["GET"])
@jwt_optional
def live_publisher_id_apc_get(publisher_id):
    authenticate_for_publisher(publisher_id, required_permission=Permission.view())

    my_package = Package.query.get(publisher_id)

    if not my_package:
        abort_json(404, "Publisher not found")

    if my_package.unique_saved_scenarios:
        my_scenario = my_package.unique_saved_scenarios[0]
    else:
        my_scenario = default_scenario(my_package.package_id)
        db.session.add(my_scenario)
        safe_commit(db)

    scenario_id = my_scenario.scenario_id

    my_saved_scenario = get_saved_scenario(scenario_id, required_permission=Permission.view())
    response = jsonify_fast_no_sort(my_saved_scenario.live_scenario.to_dict_apc())
    return response


def export_get(table_dicts):
    keys_not_to_export = ["institution_name",
                          "institution_short_name",
                          "package_id",
                          "use_groups_free_instant",
                          "use_groups_if_subscribed",
                          "use_groups_if_not_subscribed",
                          "use_instant_for_debugging",
                          "is_hybrid_2019",
                          "use_social_networks_percent",
                          "use_green_percent",
                          "use_hybrid_percent",
                          "use_bronze_percent",
                          "use_peer_reviewed_percent",
                          ]

    filename = "export.csv"
    with open(filename, "w") as file:
        csv_writer = csv.writer(file, encoding="utf-8")
        keys = [my_key for my_key in table_dicts[0].keys() if my_key not in keys_not_to_export]

        csv_writer.writerow(keys)
        for table_dict in table_dicts:
            row = []
            for my_key in keys:
                if my_key in ["issn_l", "issns"]:
                    # doing this hacky thing so excel doesn't format the issn as a date :(
                    row.append(u"issn:{}".format(table_dict[my_key]))
                else:
                    row.append(table_dict[my_key])
            csv_writer.writerow(row)

    with open(filename, "r") as file:
        contents = file.readlines()

    return contents


@app.route("/scenario/<scenario_id>/export_subscriptions.txt", methods=["GET"])
@jwt_required
def scenario_id_export_subscriptions_txt_get(scenario_id):

    consortium_ids = get_consortium_ids()
    if scenario_id in [d["scenario_id"] for d in consortium_ids]:
        my_consortium = Consortium(scenario_id)
        table_dicts = my_consortium.to_dict_journals()["journals"]
    else:
        my_saved_scenario = get_saved_scenario(scenario_id, required_permission=Permission.view())
        table_dicts = my_saved_scenario.to_dict_journals()["journals"]

    subscription_list = [d["issn_l"] for d in table_dicts if d["subscribed"]]
    subscription_list_comma_separated = u",".join(subscription_list)

    return Response(subscription_list_comma_separated, mimetype="text/text")


@app.route("/scenario/<scenario_id>/export.csv", methods=["GET"])
@jwt_required
def scenario_id_export_csv_get(scenario_id):

    consortium_ids = get_consortium_ids()
    if scenario_id in [d["scenario_id"] for d in consortium_ids]:
        my_consortium = Consortium(scenario_id)
        table_dicts = my_consortium.to_dict_journals()["journals"]
    else:
        my_saved_scenario = get_saved_scenario(scenario_id, required_permission=Permission.view())
        table_dicts = my_saved_scenario.to_dict_journals()["journals"]

    contents = export_get(table_dicts)
    return Response(contents, mimetype="text/csv")


@app.route("/scenario/<scenario_id>/export", methods=["GET"])
@jwt_required
def scenario_id_export_get(scenario_id):
    consortium_ids = get_consortium_ids()
    if scenario_id in [d["scenario_id"] for d in consortium_ids]:
        my_consortium = Consortium(scenario_id)
        table_dicts = my_consortium.to_dict_journals()["journals"]
    else:
        my_saved_scenario = get_saved_scenario(scenario_id, required_permission=Permission.view())
        table_dicts = my_saved_scenario.to_dict_journals()["journals"]

    contents = export_get(table_dicts)
    return Response(contents, mimetype="text/text")


@app.route("/package/<package_id>/scenario", methods=["POST"])
@jwt_optional
def scenario_post(package_id):
    new_scenario_id = request.json.get("id", shortuuid.uuid()[0:8])
    new_scenario_name = request.json.get("name", "New Scenario")

    if package_id.startswith("demo-package") and not new_scenario_id.startswith("demo-scenario-"):
        new_scenario_id = "demo-scenario-" + new_scenario_id

    my_saved_scenario_to_copy_from = None

    copy_scenario_id = request.args.get("copy", None)
    if copy_scenario_id:
        my_saved_scenario_to_copy_from = get_saved_scenario(copy_scenario_id, required_permission=Permission.view())

    new_saved_scenario = SavedScenario(False, new_scenario_id, None)
    new_saved_scenario.package_id = package_id
    new_saved_scenario.scenario_name = new_scenario_name
    new_saved_scenario.is_base_scenario = False

    if my_saved_scenario_to_copy_from:
        dict_to_save = my_saved_scenario_to_copy_from.to_dict_saved_from_db()
        dict_to_save["id"] = new_scenario_id
        dict_to_save["name"] = new_scenario_name
    else:
        dict_to_save = new_saved_scenario.to_dict_saved_from_db()

    save_raw_scenario_to_db(new_scenario_id, dict_to_save, get_ip(request))

    db.session.add(new_saved_scenario)
    print "new_saved_scenario", new_saved_scenario
    safe_commit(db)

    consortium_ids = get_consortium_ids()
    if package_id in [d["package_id"] for d in consortium_ids]:
        if copy_scenario_id:
            consortia_to_copy_from = Consortium(copy_scenario_id)
            save_raw_member_institutions_included_to_db(new_scenario_id, consortia_to_copy_from.member_institution_included_list, get_ip(request))
            consortia_to_copy_from.copy_computed_journal_dicts(new_scenario_id)
        else:
            new_consortia = Consortium(new_scenario_id)
            save_raw_member_institutions_included_to_db(new_scenario_id, new_consortia.member_institution_included_list, get_ip(request))
            login_user = authenticated_user()
            new_consortia.queue_for_recompute(login_user.email)

    print "getting my_new_scenario"
    my_new_scenario = get_saved_scenario(new_scenario_id, required_permission=Permission.view())
    print "got my_new_scenario"

    return jsonify_fast_no_sort(my_new_scenario.to_dict_journals())


@app.route("/publisher/<publisher_id>/scenario", methods=["POST"])
@jwt_optional
# @timeout_decorator.timeout(25, timeout_exception=TimeoutError)
def publisher_scenario_post(publisher_id):
    authenticate_for_publisher(publisher_id, Permission.modify())

    new_scenario_id = request.json.get("id", "scenario-{}".format(shortuuid.uuid()[0:12]))
    new_scenario_name = request.json.get("name", "New Scenario")

    my_saved_scenario_to_copy_from = None

    copy_scenario_id = request.args.get("copy", None)
    if copy_scenario_id:
        my_saved_scenario_to_copy_from = get_saved_scenario(copy_scenario_id, required_permission=Permission.view())

    new_saved_scenario = SavedScenario(False, new_scenario_id, None)
    new_saved_scenario.package_id = publisher_id
    new_saved_scenario.scenario_name = new_scenario_name
    new_saved_scenario.is_base_scenario = False
    db.session.add(new_saved_scenario)
    safe_commit(db)

    if my_saved_scenario_to_copy_from:
        dict_to_save = my_saved_scenario_to_copy_from.to_dict_saved_from_db()
        dict_to_save["id"] = new_scenario_id
        dict_to_save["name"] = new_scenario_name
    else:
        dict_to_save = new_saved_scenario.to_dict_saved_from_db()

    save_raw_scenario_to_db(new_scenario_id, dict_to_save, get_ip(request))

    consortium_ids = get_consortium_ids()
    if publisher_id in [d["package_id"] for d in consortium_ids]:
        if copy_scenario_id:
            consortia_to_copy_from = Consortium(copy_scenario_id)
            save_raw_member_institutions_included_to_db(new_scenario_id, consortia_to_copy_from.member_institution_included_list, get_ip(request))
            consortia_to_copy_from.copy_computed_journal_dicts(new_scenario_id)
        else:
            new_consortia = Consortium(new_scenario_id)
            save_raw_member_institutions_included_to_db(new_scenario_id, new_consortia.member_institution_included_list, get_ip(request))
            new_consortia.recompute_journal_dicts()

    my_new_scenario = get_saved_scenario(new_scenario_id, required_permission=Permission.view())

    return jsonify_fast_no_sort(my_new_scenario.to_dict_meta())


@app.route("/scenario/<scenario_id>", methods=["DELETE"])
@jwt_optional
def scenario_delete(scenario_id):
    # just delete it out of the table, leave the saves
    # doing it this way makes sure we have permission to acces and therefore delete the scenario
    get_saved_scenario(scenario_id, required_permission=Permission.modify())

    command = "delete from jump_package_scenario where scenario_id = '{}'".format(scenario_id)
    with get_db_cursor() as cursor:
        cursor.execute(command)

    return jsonify_fast_no_sort({"response": "success"})


@app.route("/password/request-reset", methods=["POST"])
def request_password_reset():
    request_args = request.args
    if request.is_json:
        request_args = request.json

    username = request_args.get("username", None)
    email = request_args.get("email", None)
    user_id = request_args.get("user_id", None)

    if not (username or email or user_id):
        return abort_json(400, "User ID, username or email parameter is required.")

    reset_user = lookup_user(user_id=user_id, email=email, username=username)

    if not reset_user:
        return abort_json(404, u"User does not exist.")

    if not reset_user.email:
        return abort_json(404, u"User has no email address.")

    reset_request = password_reset.ResetRequest(user_id=reset_user.id)
    db.session.add(reset_request)
    safe_commit(db)

    email = create_email(reset_user.email, u"Change your Unsub password.", "password_reset", {"data": {
        "display_name": reset_user.display_name,
        "email": reset_user.email,
        "jump_url": os.environ.get("JUMP_URL"),
        "token": reset_request.token,
    }})

    send(email, for_real=True)

    return jsonify_fast_no_sort({"message": "reset request received"})


@app.route("/password/reset", methods=["POST"])
def reset_password():
    request_args = request.args
    if request.is_json:
        request_args = request.json

    token = request_args.get("token", None)
    password = request_args.get("password", None)

    if token is None:
        return abort_json(400, u"Missing required parameter: token.")

    if password is None:
        return abort_json(400, u"Missing required parameter: password.")

    reset_request = password_reset.ResetRequest.query.get(token)

    if not reset_request or reset_request.expires < datetime.datetime.utcnow():
        return abort_json(404, u"Unrecognized reset token {}.".format(token))

    reset_user = User.query.get(reset_request.user_id)

    if not reset_user:
        return abort_json(404, u"Unrecognized user id {}.".format(reset_request.user_id))

    reset_user.password_hash = generate_password_hash(password)
    command = "delete from jump_password_reset_request where user_id = '{}'".format(reset_user.id)
    with get_db_cursor() as cursor:
        cursor.execute(command)

    safe_commit(db)

    return jsonify_fast_no_sort({"message": u"password reset for user {}".format(reset_user.id)})


@app.route("/admin/change-password", methods=["GET"])
def admin_change_password():
    return abort_json(404, "Removed. Use /user/me or /password-request-reset.")


@app.route("/admin/register", methods=["GET"])
def admin_register_user():
    return abort_json(404, "Removed. Use /user/new or /user/demo.")



cache_last_updated = "1999-01-01"

def start_cache_thread():

    from time import sleep

    def do_stuff(dummy):
        print "in do stuff"
        global cache_last_updated

        while True:
            command = "select cache_call, updated from jump_cache_status where updated > '{}'::timestamp".format(cache_last_updated)
            # print command
            with get_db_cursor() as cursor:
                cursor.execute(command)
                rows = cursor.fetchall()
                random.shuffle(rows)

            # print ".",

            for row in rows:
                print "CACHE: found some things that need refreshing"
                try:
                    del app.my_memorycache_dict[row["cache_call"]]
                except KeyError:
                    # hadn't been cached before
                    pass

                (module_name, function_name, args) = json.loads(row["cache_call"])

                module = __import__(module_name)
                func = getattr(module, function_name)
                func(*args)

            if rows:
                cache_last_updated = max([row["updated"] for row in rows]).isoformat()
            sleep( 1 * random.random())


    import threading

    data = None
    t = threading.Thread(target=do_stuff, args=[data])
    t.daemon = True  # so it doesn't block
    t.start()


def do_things():
    # consortium
    # scenario_id = "scenario-QC2kbHfUhj9W"

    # not consortium
    scenario_id = "scenario-VCebMrfWahSZ"

    consortium_ids = get_consortium_ids()
    if scenario_id in [d["scenario_id"] for d in consortium_ids]:
        my_consortium = Consortium(scenario_id)
        my_saved_scenario_dict = my_consortium.to_dict_journals()
    else:
        my_saved_scenario = get_saved_scenario(scenario_id)
        my_saved_scenario_dict = my_saved_scenario.to_dict_journals()

#
# # https://goshippo.com/blog/measure-real-size-any-python-object/
# def get_size(obj, seen=None):
#     """Recursively finds size of objects"""
#     size = sys.getsizeof(obj)
#     if seen is None:
#         seen = set()
#     obj_id = id(obj)
#     if obj_id in seen:
#         return 0
#     # Important mark as seen *before* entering recursion to gracefully handle
#     # self-referential objects
#     seen.add(obj_id)
#     if isinstance(obj, dict):
#         size += sum([get_size(v, seen) for v in obj.values()])
#         size += sum([get_size(k, seen) for k in obj.keys()])
#     elif hasattr(obj, '__dict__'):
#         size += get_size(obj.__dict__, seen)
#     elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
#         size += sum([get_size(i, seen) for i in obj])
#     return size
#
# # https://stackoverflow.com/questions/24455615/python-how-to-display-size-of-all-variables/51046503#51046503
# def sizeof_fmt(num, suffix='B'):
#     ''' by Fred Cirera,  https://stackoverflow.com/a/1094933/1870254, modified'''
#     for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
#         if abs(num) < 1024.0:
#             return "%3.1f %s%s" % (num, unit, suffix)
#         num /= 1024.0
#     return "%.1f %s%s" % (num, 'Yi', suffix)
#
# import gc
# from pympler import asizeof
# from pympler import tracker
# memory_tracker = tracker.SummaryTracker()
#
# @app.route("/admin/memory", methods=["GET"])
# def admin_memory():
#     response = []
#     # for name, size in sorted(((name, get_size(value)) for name, value in locals().items()),
#     # for name, size in sorted(((name, get_size(name)) for name in gc.get_objects()),
#     # for name, size in sorted(((name, asizeof(name)) for name in gc.get_objects()),
#     #                          key= lambda x: -x[1])[:100]:
#     #     response += ["{:>30}: {:>8}".format(name, sizeof_fmt(size))]
#
#     response = "hi heather"
#     global memory_tracker
#     memory_tracker.print_diff()
#
#     return jsonify_fast_no_sort({"response": response})


#  flask run -h 0.0.0.0 -p 5004 --with-threads --reload
if __name__ == "__main__":

    # start_cache_thread()

    # do_things()

    port = int(os.environ.get("PORT", 5004))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True, use_reloader=True)
