# coding: utf-8

import logging
import sys
import os
import gzip
import requests
import simplejson as json
import functools
import warnings
import urllib.parse
from time import time
import numpy
from contextlib import contextmanager
from collections import OrderedDict
# from dozer import Dozer
import boto3

warnings.filterwarnings("ignore", category=UserWarning, module='psycopg2')
import psycopg2
import psycopg2.extras # needed though you wouldn't guess it
from psycopg2.pool import ThreadedConnectionPool

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_compress import Compress
from flask_debugtoolbar import DebugToolbarExtension
from flask_jwt_extended import JWTManager
# from sqlalchemy.pool import NullPool

from util import elapsed
from util import HTTPMethodOverrideMiddleware

HEROKU_APP_NAME = "jump-api"
DEMO_PACKAGE_ID = "658349d9"
JISC_PACKAGE_ID = "package-3WkCDEZTqo6S"
JISC_INSTITUTION_ID = "institution-Afxc4mAYXoJH"
USE_PAPER_GROWTH = False
DATABASE_URL = os.getenv("DATABASE_URL_REDSHIFT_TEST") if os.getenv("TESTING_DB") else os.getenv("DATABASE_URL_REDSHIFT")

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
    "stripe",
    "boto",
    "boto3",
    "botocore",
    "s3transfer",
    # "newrelic",
    "RateLimiter",
    "paramiko",
    "chardet",
    "cryptography",
    "pyexcel",
    "lml",
    "pyexcel_io"
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

for name in list(logging.Logger.manager.loggerDict.keys()):
    if ('boto' in name) or ('urllib3' in name) or ('s3transfer' in name) or ('boto3' in name) or ('botocore' in name):
        logging.getLogger(name).setLevel(logging.ERROR)

with warnings.catch_warnings():
    warnings.filterwarnings('ignore', r'RuntimeWarning: overflow encountered in exp')

numpy.seterr(over="ignore")

# disable extra warnings
requests.packages.urllib3.disable_warnings()
warnings.filterwarnings("ignore", category=DeprecationWarning)



app = Flask(__name__)

# memory profiling
# app.wsgi_app = Dozer(app.wsgi_app, profile_path='./dozer_profiles')


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

# test or production database
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_BINDS"] = {
    "redshift_db": DATABASE_URL
}

# see https://stackoverflow.com/questions/43594310/redshift-sqlalchemy-long-query-hangs
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = { "pool_pre_ping": True,
                                            "pool_recycle": 300,
                                            "connect_args": {
                                                "keepalives": 1,
                                                "keepalives_idle": 10,
                                                "keepalives_interval": 2,
                                                "keepalives_count": 5
                                            }
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
    logger.info("Setting app.debug=True; Flask-DebugToolbar will display")
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

redshift_url = urllib.parse.urlparse(DATABASE_URL)
app.config['postgreSQL_pool'] = ThreadedConnectionPool(2, 200,
                                  database=redshift_url.path[1:],
                                  user=redshift_url.username,
                                  password=redshift_url.password,
                                  host=redshift_url.hostname,
                                  port=redshift_url.port,
                                  keepalives=1,
                                  keepalives_idle=10,
                                  keepalives_interval=2,
                                  keepalives_count=5)

app.config['PROFILE_REQUESTS'] = (os.getenv("PROFILE_REQUESTS", False) == "True")

logger.info("Database URL host: {}".format(redshift_url.hostname))

# celery background tasks
app.config.update(
    CELERY_BROKER_URL=os.environ['REDIS_URL'],
    CELERY_RESULT_BACKEND=os.environ['REDIS_URL']
)

@contextmanager
def get_db_connection():
    try:
        connection = app.config['postgreSQL_pool'].getconn()
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        connection.autocommit=True
        # connection.readonly = True
        yield connection
    # except Exception as e:
    #     print u"error in get_db_connection", e
    #     raise
    finally:
        app.config['postgreSQL_pool'].putconn(connection)

@contextmanager
def get_db_cursor(commit=False, use_realdictcursor=False, use_defaultcursor=False):
    with get_db_connection() as connection:
        if use_realdictcursor:
            # takes more memory, so default is no
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        elif use_defaultcursor:
            cursor = connection.cursor()
        else:
            cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
              yield cursor
              if commit:
                  connection.commit()
        except Exception as e:
            print("Error: error in get_db_cursor: {} {}, rolling back".format(e, str(e)))
            try:
                connection.rollback()
            except:
                pass
        finally:
            cursor.close()
            pass

s3_client = boto3.client("s3")
print("made s3_client")

use_groups_lookup = OrderedDict()
use_groups_lookup["oa_plus_social_networks"] = {"display": "OA", "free_instant": True}
# use_groups_lookup["social_networks"] = {"display": "ASNs", "free_instant": True}
use_groups_lookup["backfile"] = {"display": "Backfile", "free_instant": True}
use_groups_lookup["subscription"] = {"display": "Subscription", "free_instant": False}
use_groups_lookup["ill"] = {"display": "ILL", "free_instant": False}
use_groups_lookup["other_delayed"] = {"display": "Other", "free_instant": False}
use_groups_lookup["total"] = {"display": "*Total*", "free_instant": False}
use_groups = list(use_groups_lookup.keys())
use_groups_free_instant = [k for k, v in use_groups_lookup.items() if v["free_instant"]]

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
        cache_key = build_cache_key(func.__module__, func.__name__, *args)

        # Return cached version if available
        result = app.my_memorycache_dict.get(cache_key, None)
        if result is not None:
            # print "cache hit on", cache_key
            # print("cache hit")
            return result

        # print("cache miss on", cache_key)
        # print("cache miss")

        # Generate output
        # print("> calling {}.{} with {}".format(func.__module__, func.__name__, args))
        result = func(*args)

        # Cache output if allowed
        if result is not None:
            app.my_memorycache_dict[cache_key] = result

        # reset_cache(func.__module__, func.__name__, *args)

        return result

    return wrapper


def reset_cache(module_name, function_name, *args):
    print("args", args)
    cache_key = build_cache_key(module_name, function_name, *args)
    print("cache_key", cache_key)

    if cache_key in app.my_memorycache_dict:
        del app.my_memorycache_dict[cache_key]

    delete_command = "delete from jump_cache_status where cache_call = %s"
    insert_command = "insert into jump_cache_status (cache_call, updated) values (%s, sysdate)"
    with get_db_cursor() as cursor:
        cursor.execute(delete_command, (cache_key,))
        cursor.execute(insert_command, (cache_key,))

cached_consortium_scenario_ids = ["tGUVWRiN", "scenario-QC2kbHfUhj9W", "EcUvEELe", "CBy9gUC3", "6it6ajJd", "GcAsm5CX", "aAFAuovt"]

@memorycache
def fetch_common_package_data():
    try:
        print("downloading common_package_data_for_all.json.gz")
        s3_clientobj = s3_client.get_object(Bucket="unsub-cache", Key="common_package_data_for_all_forecast_years.json.gz")
        with gzip.open(s3_clientobj["Body"], 'r') as f:
            data_from_s3 = json.loads(f.read().decode('utf-8'))
        return data_from_s3
    except Exception as e:
        print("no S3 data, so computing.  Error message: ", e)
        pass

    from common_data import gather_common_data
    return gather_common_data()

common_data_dict = None

def warm_common_data(lst):
    lst.append(fetch_common_package_data())

# NOTE: we have to do this when app loads for now - return to this later 
## if there's a different take on dealing with common data
# if os.getenv('PRELOAD_LARGE_TABLES', False) == 'True':
if not common_data_dict:
    import threading
    import time
    an_lst = []
    t = threading.Thread(target=warm_common_data, args=[an_lst])
    t.daemon = True
    t.start()
    while t.is_alive():
        time.sleep(0.1)
    common_data_dict = an_lst[0]
    print("warm_common_data done!")
else:
    print("not warming common data cache")
