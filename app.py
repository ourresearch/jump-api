# coding: utf-8

import traceback
import logging
import sys
import os
import requests
import simplejson as json
import functools
import hashlib
import pickle
import random
import warnings
import urlparse
import numpy
from contextlib import contextmanager
from collections import OrderedDict

warnings.filterwarnings("ignore", category=UserWarning, module='psycopg2')
import psycopg2
import psycopg2.extras # needed though you wouldn't guess it
from psycopg2.pool import ThreadedConnectionPool

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_compress import Compress
from flask_debugtoolbar import DebugToolbarExtension
from flask_jwt_extended import JWTManager
from sqlalchemy import exc
from sqlalchemy import event
from sqlalchemy.pool import NullPool
from sqlalchemy.pool import Pool
import bmemcached

from util import safe_commit
from util import elapsed
from util import HTTPMethodOverrideMiddleware

HEROKU_APP_NAME = "jump-api"
DEMO_PACKAGE_ID = "658349d9"
USE_PAPER_GROWTH = False

# set up logging
# see http://wiki.pylonshq.com/display/pylonscookbook/Alternative+logging+configuration
logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format='%(thread)d: %(message)s'  #tried process but it was always "6" on heroku
)
logger = logging.getLogger("jump-api")

libraries_to_mum_warning = [
    "requests",
    "urllib3",
    "requests.packages.urllib3",
    "requests_oauthlib",
    "stripe",
    "oauthlib",
    "boto",
    "newrelic",
    "RateLimiter",
    "paramiko",
    "chardet",
    "cryptography",
    "bmemcached"
]


libraries_to_mum_error = [
    "scipy",
    "psycopg2",
    "matplotlib",
    "numpy"
]

for a_library in libraries_to_mum_warning:
    the_logger = logging.getLogger(a_library)
    the_logger.setLevel(logging.WARNING)
    the_logger.propagate = True
    warnings.filterwarnings("ignore", category=UserWarning, module=a_library)

for a_library in libraries_to_mum_error:
    the_logger = logging.getLogger(a_library)
    the_logger.setLevel(logging.ERROR)
    the_logger.propagate = True
    warnings.filterwarnings("ignore", category=UserWarning, module=a_library)

logging.getLogger('pyexcel').setLevel(logging.WARNING)
logging.getLogger('lml').setLevel(logging.WARNING)
logging.getLogger('pyexcel_io').setLevel(logging.WARNING)

with warnings.catch_warnings():
    warnings.filterwarnings('ignore', r'RuntimeWarning: overflow encountered in exp')

numpy.seterr(over="ignore")

# disable extra warnings
requests.packages.urllib3.disable_warnings()
warnings.filterwarnings("ignore", category=DeprecationWarning)

app = Flask(__name__)

# authorization
app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY')
app.config['SECRET_KEY'] = os.getenv('JWT_SECRET_KEY')
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = False # doesn't expire
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = False # doesn't expire
app.config['JWT_TOKEN_LOCATION'] = ('headers', 'query_string')
jwt = JWTManager(app)

# database stuff
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True  # as instructed, to suppress warning

app.config['SQLALCHEMY_ECHO'] = (os.getenv("SQLALCHEMY_ECHO", False) == "True")
# app.config['SQLALCHEMY_ECHO'] = True

app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL_REDSHIFT")
app.config["SQLALCHEMY_BINDS"] = {
    "redshift_db": os.getenv("DATABASE_URL_REDSHIFT")
}


# from http://stackoverflow.com/a/12417346/596939
# class NullPoolSQLAlchemy(SQLAlchemy):
#     def apply_driver_hacks(self, app, info, options):
#         options['poolclass'] = NullPool
#         return super(NullPoolSQLAlchemy, self).apply_driver_hacks(app, info, options)
#
# db = NullPoolSQLAlchemy(app, session_options={"autoflush": False})

app.config["SQLALCHEMY_POOL_SIZE"] = 200
db = SQLAlchemy(app, session_options={"autoflush": False, "autocommit": False})

# do compression.  has to be above flask debug toolbar so it can override this.
compress_json = os.getenv("COMPRESS_DEBUG", "True")=="True"


# set up Flask-DebugToolbar
if (os.getenv("FLASK_DEBUG", False) == "True"):
    logger.info(u"Setting app.debug=True; Flask-DebugToolbar will display")
    compress_json = False
    app.debug = True
    app.config['DEBUG'] = True
    app.config["DEBUG_TB_INTERCEPT_REDIRECTS"] = False
    app.config["SQLALCHEMY_RECORD_QUERIES"] = True
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY")
    toolbar = DebugToolbarExtension(app)

# gzip responses
Compress(app)
app.config["COMPRESS_DEBUG"] = compress_json


redshift_url = urlparse.urlparse(os.getenv("DATABASE_URL_REDSHIFT"))
app.config['postgreSQL_pool'] = ThreadedConnectionPool(2, 200,
                                  database=redshift_url.path[1:],
                                  user=redshift_url.username,
                                  password=redshift_url.password,
                                  host=redshift_url.hostname,
                                  port=redshift_url.port)

app.config['PROFILE_REQUESTS'] = (os.getenv("PROFILE_REQUESTS", False) == "True")


@contextmanager
def get_db_connection():
    try:
        connection = app.config['postgreSQL_pool'].getconn()
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        connection.autocommit=True
        # connection.readonly = True
        yield connection
    finally:
        app.config['postgreSQL_pool'].putconn(connection)

@contextmanager
def get_db_cursor(commit=False):
    with get_db_connection() as connection:
      cursor = connection.cursor(
                  cursor_factory=psycopg2.extras.RealDictCursor)
      try:
          yield cursor
          if commit:
              connection.commit()
      finally:
          cursor.close()
          pass


memcached_servers = os.environ.get('MEMCACHIER_SERVERS', '').split(',')
memcached_user = os.environ.get('MEMCACHIER_USERNAME', '')
memcached_password = os.environ.get('MEMCACHIER_PASSWORD', '')
my_memcached = bmemcached.Client(memcached_servers, username=memcached_user, password=memcached_password)
my_memcached.enable_retry_delay(True)  # Enabled by default. Sets retry delay to 5s.
# my_memcached.flush_all()

use_groups_lookup = OrderedDict()
use_groups_lookup["oa_plus_social_networks"] = {"display": "OA", "free_instant": True}
# use_groups_lookup["social_networks"] = {"display": "ASNs", "free_instant": True}
use_groups_lookup["backfile"] = {"display": "Backfile", "free_instant": True}
use_groups_lookup["subscription"] = {"display": "Subscription", "free_instant": False}
use_groups_lookup["ill"] = {"display": "ILL", "free_instant": False}
use_groups_lookup["other_delayed"] = {"display": "Other", "free_instant": False}
use_groups_lookup["total"] = {"display": "*Total*", "free_instant": False}
use_groups = use_groups_lookup.keys()
use_groups_free_instant = [k for k, v in use_groups_lookup.iteritems() if v["free_instant"]]

suny_consortium_package_ids = ["P2NFgz7B", "PN3juRC5", "2k4Qs74v", "uwdhDaJ2"]





app.my_memorycache_dict = {}

def build_cache_key(module_name, function_name, *args):
    # just ignoring kwargs for now
    hashable_args = args

    # Generate unique cache key
    key_raw = (module_name, function_name, hashable_args)
    cache_key = json.dumps(key_raw)
    return cache_key


def memorycache(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # global my_memorycache_dict

        cache_key = build_cache_key(func.__module__, func.__name__, *args)

        # Return cached version if available
        result = app.my_memorycache_dict.get(cache_key, None)
        if result is not None:
            print "cache hit on", cache_key
            return result

        print "cache miss on", cache_key

        # Generate output
        print u"> calling {}.{} with {}".format(func.__module__, func.__name__, args)
        result = func(*args)

        # Cache output if allowed
        if result is not None:
            app.my_memorycache_dict[cache_key] = result

        # reset_cache(func.__module__, func.__name__, *args)

        return result

    return wrapper


def reset_cache(module_name, function_name, *args):
    # global my_memorycache_dict

    print "args", args
    cache_key = build_cache_key(module_name, function_name, *args)
    print "cache_key", cache_key

    if cache_key in app.my_memorycache_dict:
        del app.my_memorycache_dict[cache_key]

    delete_command = """delete from jump_cache_status where cache_call = '{}';""".format(cache_key)
    insert_command = """insert into jump_cache_status (cache_call, updated)
        values ('{}', sysdate) """.format(cache_key)
    with get_db_cursor() as cursor:
        cursor.execute(delete_command)
        cursor.execute(insert_command)


if os.getenv('PRELOAD_LARGE_TABLES', False) == 'True':
    print u"loading cache"
    scenario_id = "scenario-fsVitXLd"

    import consortium
    consortium.get_consortium_ids()
    consortium.consortium_get_computed_data(scenario_id)
    consortium.consortium_get_issns(scenario_id)

    import scenario
    scenario.get_common_package_data_for_all()

    print u"done loading to cache"

#
# print "clearing cache"
# reset_cache("consortium", "consortium_get_computed_data", "scenario-fsVitXLd")
# print "cache clear set"
