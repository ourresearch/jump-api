# coding: utf-8

import bisect
import codecs
import collections
import datetime
import locale
import logging
import math
import os
import re
import tempfile
import time
import traceback
import unicodedata
import urllib.parse
from codecs import BOM_UTF8, BOM_UTF16_BE, BOM_UTF16_LE, BOM_UTF32_BE, BOM_UTF32_LE
import chardet
import numpy as np

import heroku3
import requests
import simplejson as json
import sqlalchemy
import unicodecsv as csv
from flask import current_app
from flask_jwt_extended import get_jwt_identity
from requests.adapters import HTTPAdapter
from simplejson import dumps
from sqlalchemy import exc
from sqlalchemy import sql
from unidecode import unidecode
from werkzeug.wsgi import ClosingIterator

try:
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8') #use locale.format for commafication
except locale.Error:
    locale.setlocale(locale.LC_ALL, '') #set to default locale (works on windows)

def str2bool(v):
  return v.lower() in ("yes", "true", "t", "1")

class NoDoiException(Exception):
    pass

class TimingMessages(object):
    def __init__(self):
        self.start_time = time.time()
        self.section_time = time.time()
        self.messages = []

    def format_timing_message(self, message, use_start_time=False):
        my_elapsed = elapsed(self.section_time, 2)
        if use_start_time:
            my_elapsed = elapsed(self.start_time, 2)

        # now reset section time
        self.section_time = time.time()

        return "{: <30} {: >6}s".format(message, my_elapsed)

    def log_timing(self, message):
        self.messages.append(self.format_timing_message(message))

    def to_dict(self):
        self.messages.append(self.format_timing_message("TOTAL", use_start_time=True))
        return self.messages


class DelayedAdapter(HTTPAdapter):
    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        # logger.info(u"in DelayedAdapter getting {}, sleeping for 2 seconds".format(request.url))
        # sleep(2)
        start_time = time.time()
        response = super(DelayedAdapter, self).send(request, stream, timeout, verify, cert, proxies)
        # logger.info(u"   HTTPAdapter.send for {} took {} seconds".format(request.url, elapsed(start_time, 2)))
        return response

def read_csv_file(filename, sep=","):
    with open(filename, "rU") as csv_file:
        my_reader = csv.DictReader(csv_file, delimiter=sep, encoding='utf-8-sig')
        rows = [row for row in my_reader]
    return rows

# from http://stackoverflow.com/a/3233356/596939
def update_recursive_sum(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = update_recursive_sum(d.get(k, {}), v)
            d[k] = r
        else:
            if k in d:
                d[k] += u[k]
            else:
                d[k] = u[k]
    return d

# returns dict with values that are proportion of all values
def as_proportion(my_dict):
    if not my_dict:
        return {}
    total = sum(my_dict.values())
    resp = {}
    for k, v in my_dict.items():
        resp[k] = round(float(v)/total, 2)
    return resp

def calculate_percentile(refset, value):
    if value is None:  # distinguish between that and zero
        return None

    matching_index = bisect.bisect_left(refset, value)
    percentile = float(matching_index) / len(refset)
    # print u"percentile for {} is {}".format(value, percentile)

    return percentile

def clean_html(raw_html):
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return cleantext

# good for deduping strings.  warning: output removes spaces so isn't readable.
def normalize(text):
    response = text.lower()
    response = unidecode(str(response))
    response = clean_html(response)  # has to be before remove_punctuation
    response = remove_punctuation(response)
    response = re.sub(r"\b(a|an|the)\b", "", response)
    response = re.sub(r"\b(and)\b", "", response)
    response = re.sub(r"\s+", "", response)
    return response

def normalize_simple(text):
    response = text.lower()
    response = remove_punctuation(response)
    response = re.sub(r"\b(a|an|the)\b", "", response)
    response = re.sub(r"\s+", "", response)
    return response

def remove_everything_but_alphas(input_string):
    # from http://stackoverflow.com/questions/265960/best-way-to-strip-punctuation-from-a-string-in-python
    only_alphas = input_string
    if input_string:
        only_alphas = "".join(e for e in input_string if (e.isalpha()))
    return only_alphas

def remove_punctuation(input_string):
    # from http://stackoverflow.com/questions/265960/best-way-to-strip-punctuation-from-a-string-in-python
    no_punc = input_string
    if input_string:
        no_punc = "".join(e for e in input_string if (e.isalnum() or e.isspace()))
    return no_punc

# from http://stackoverflow.com/a/11066579/596939
def replace_punctuation(text, sub):
    punctutation_cats = set(['Pc', 'Pd', 'Ps', 'Pe', 'Pi', 'Pf', 'Po'])
    chars = []
    for my_char in text:
        if unicodedata.category(my_char) in punctutation_cats:
            chars.append(sub)
        else:
            chars.append(my_char)
    return "".join(chars)


# from http://stackoverflow.com/a/22238613/596939
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError ("Type not serializable")

def conversational_number(number):
    words = {
        "1.0": "one",
        "2.0": "two",
        "3.0": "three",
        "4.0": "four",
        "5.0": "five",
        "6.0": "six",
        "7.0": "seven",
        "8.0": "eight",
        "9.0": "nine",
    }

    if number < 1:
        return round(number, 2)

    elif number < 1000:
        return round(math.floor(number))

    elif number < 1000000:
        divided = number / 1000.0
        unit = "thousand"

    else:
        divided = number / 1000000.0
        unit = "million"

    short_number = '{}'.format(round(divided, 2))[:-1]
    if short_number in words:
        short_number = words[short_number]

    return short_number + " " + unit



def safe_commit(db):
    try:
        db.session.commit()
        return True
    except (KeyboardInterrupt, SystemExit):
        # let these ones through, don't save anything to db
        raise
    except sqlalchemy.exc.DataError:
        try:
            print("sqlalchemy.exc.DataError on commit.  rolling back.")
            db.session.rollback()
        except:
            pass
    except Exception:
        try:
            print("generic exception in commit.  rolling back.")
            db.session.rollback()
        except:
            pass
        logging.exception("commit error")
    return False


def is_pmc(url):
    return "ncbi.nlm.nih.gov/pmc" in url or "europepmc.org/articles/" in url


def is_doi(text):
    if not text:
        return False

    try_to_clean_doi = clean_doi(text, return_none_if_error=True)
    if try_to_clean_doi:
        return True
    return False

def is_issn(text):
    if not text:
        return False

    # include X and F
    p = re.compile(r"[\dxf]{4}-[\dxf]{4}")
    matches = re.findall(p, text.lower())
    if len(matches) > 0:
        return True
    return False


def is_doi_url(url):
    if not url:
        return False

    # test urls at https://regex101.com/r/yX5cK0/2
    p = re.compile(r"https?:\/\/(?:dx.)?doi.org\/(.*)")
    matches = re.findall(p, url.lower())
    if len(matches) > 0:
        return True
    return False

def is_ip(ip):
    if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip):
        return True
    return False

def clean_doi(dirty_doi, return_none_if_error=False):
    if not dirty_doi:
        if return_none_if_error:
            return None
        else:
            raise NoDoiException("There's no DOI at all.")

    dirty_doi = dirty_doi.strip()
    dirty_doi = dirty_doi.lower()

    # test cases for this regex are at https://regex101.com/r/zS4hA0/1
    p = re.compile(r'(10\.\d+\/[^\s]+)')

    matches = re.findall(p, dirty_doi)
    if len(matches) == 0:
        if return_none_if_error:
            return None
        else:
            raise NoDoiException("There's no valid DOI.")

    match = matches[0]
    match = remove_nonprinting_characters(match)

    try:
        resp = str(match, "utf-8")  # unicode is valid in dois
    except (TypeError, UnicodeDecodeError):
        resp = match

    # remove any url fragments
    if "#" in resp:
        resp = resp.split("#")[0]

    # remove double quotes, they shouldn't be there as per http://www.doi.org/syntax.html
    resp = resp.replace('"', '')

    # remove trailing period, comma -- it is likely from a sentence or citation
    if resp.endswith(",") or resp.endswith("."):
        resp = resp[:-1]

    return resp


def pick_best_url(urls):
    if not urls:
        return None

    #get a backup
    response = urls[0]

    # now go through and pick the best one
    for url in urls:
        # doi if available
        if "doi.org" in url:
            response = url

        # anything else if what we currently have is bogus
        if response == "http://www.ncbi.nlm.nih.gov/pmc/articles/PMC":
            response = url

    return response

def date_as_iso_utc(datetime_object):
    if datetime_object is None:
        return None

    date_string = "{}{}".format(datetime_object, "+00:00")
    return date_string


def dict_from_dir(obj, keys_to_ignore=None, keys_to_show="all"):

    if keys_to_ignore is None:
        keys_to_ignore = []
    elif isinstance(keys_to_ignore, str):
        keys_to_ignore = [keys_to_ignore]

    ret = {}

    if keys_to_show != "all":
        for key in keys_to_show:
            ret[key] = getattr(obj, key)

        return ret


    for k in dir(obj):
        value = getattr(obj, k)

        if k.startswith("_"):
            pass
        elif k in keys_to_ignore:
            pass
        # hide sqlalchemy stuff
        elif k in ["query", "query_class", "metadata"]:
            pass
        elif callable(value):
            pass
        else:
            try:
                # convert datetime objects...generally this will fail becase
                # most things aren't datetime object.
                ret[k] = time.mktime(value.timetuple())
            except AttributeError:
                ret[k] = value
    return ret


def median(my_list):
    """
    Find the median of a list of ints

    from https://stackoverflow.com/questions/24101524/finding-median-of-list-in-python/24101655#comment37177662_24101655
    """
    my_list = sorted(my_list)
    if len(my_list) < 1:
            return None
    if len(my_list) %2 == 1:
            return my_list[((len(my_list)+1)/2)-1]
    if len(my_list) %2 == 0:
            return float(sum(my_list[(len(my_list)/2)-1:(len(my_list)/2)+1]))/2.0


def underscore_to_camelcase(value):
    words = value.split("_")
    capitalized_words = []
    for word in words:
        capitalized_words.append(word.capitalize())

    return "".join(capitalized_words)

def chunks(l, n):
    """
    Yield successive n-sized chunks from l.

    from http://stackoverflow.com/a/312464
    """
    for i in range(0, len(l), n):
        yield l[i:i+n]

def page_query(q, page_size=1000):
    offset = 0
    while True:
        r = False
        print("util.page_query() retrieved {} things".format(page_query()))
        for elem in q.limit(page_size).offset(offset):
            r = True
            yield elem
        offset += page_size
        if not r:
            break

def elapsed(since, round_places=2):
    return round(time.time() - since, round_places)



def truncate(str, max=100):
    if len(str) > max:
        return str[0:max] + "..."
    else:
        return str


def str_to_bool(x):
    if x.lower() in ["true", "1", "yes"]:
        return True
    elif x.lower() in ["false", "0", "no"]:
        return False
    else:
        raise ValueError("This string can't be cast to a boolean.")

# from http://stackoverflow.com/a/20007730/226013
ordinal = lambda n: "%d%s" % (n,"tsnrhtdd"[(n/10%10!=1)*(n%10<4)*n%10::4])

#from http://farmdev.com/talks/unicode/
def to_unicode_or_bust(obj, encoding='utf-8'):
    if isinstance(obj, str):
        if not isinstance(obj, str):
            obj = str(obj, encoding)
    return obj

def remove_nonprinting_characters(input, encoding='utf-8'):
    input_was_unicode = True
    if isinstance(input, str):
        if not isinstance(input, str):
            input_was_unicode = False

    unicode_input = to_unicode_or_bust(input)

    # see http://www.fileformat.info/info/unicode/category/index.htm
    char_classes_to_remove = ["C", "M", "Z"]

    response = ''.join(c for c in unicode_input if unicodedata.category(c)[0] not in char_classes_to_remove)

    if not input_was_unicode:
        response = response.encode(encoding)

    return response

# getting a "decoding Unicode is not supported" error in this function?
# might need to reinstall libaries as per
# http://stackoverflow.com/questions/17092849/flask-login-typeerror-decoding-unicode-is-not-supported
class HTTPMethodOverrideMiddleware(object):
    allowed_methods = frozenset([
        'GET',
        'HEAD',
        'POST',
        'DELETE',
        'PUT',
        'PATCH',
        'OPTIONS'
    ])
    bodyless_methods = frozenset(['GET', 'HEAD', 'OPTIONS', 'DELETE'])

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        method = environ.get('HTTP_X_HTTP_METHOD_OVERRIDE', '').upper()
        if method in self.allowed_methods:
            method = method.encode('ascii', 'replace')
            environ['REQUEST_METHOD'] = method
        if method in self.bodyless_methods:
            environ['CONTENT_LENGTH'] = '0'
        return self.app(environ, start_response)


# could also make the random request have other filters
# see docs here: https://github.com/CrossRef/rest-api-doc/blob/master/rest_api.md#sample
# usage:
# dois = get_random_dois(50000, from_date="2002-01-01", only_journal_articles=True)
# dois = get_random_dois(100000, only_journal_articles=True)
# fh = open("data/random_dois_articles_100k.txt", "w")
# fh.writelines(u"\n".join(dois))
# fh.close()
def get_random_dois(n, from_date=None, only_journal_articles=True):
    dois = []
    while len(dois) < n:
        # api takes a max of 100
        number_this_round = min(n, 100)
        url = "https://api.crossref.org/works?sample={}".format(number_this_round)
        if only_journal_articles:
            url += "&filter=type:journal-article"
        if from_date:
            url += ",from-pub-date:{}".format(from_date)
        print(url)
        print("calling crossref, asking for {} dois, so far have {} of {} dois".format(
            number_this_round, len(dois), n))
        r = requests.get(url)
        items = r.json()["message"]["items"]
        dois += [item["DOI"].lower() for item in items]
    return dois


# from https://github.com/elastic/elasticsearch-py/issues/374
# to work around unicode problem
# class JSONSerializerPython2(elasticsearch.serializer.JSONSerializer):
#     """Override elasticsearch library serializer to ensure it encodes utf characters during json dump.
#     See original at: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
#     A description of how ensure_ascii encodes unicode characters to ensure they can be sent across the wire
#     as ascii can be found here: https://docs.python.org/2/library/json.html#basic-usage
#     """
#     def dumps(self, data):
#         # don't serialize strings
#         if isinstance(data, elasticsearch.compat.string_types):
#             return data
#         try:
#             return json.dumps(data, default=self.default, ensure_ascii=True)
#         except (ValueError, TypeError) as e:
#             raise elasticsearch.exceptions.SerializationError(data, e)



def is_the_same_url(url1, url2):
    norm_url1 = strip_jsessionid_from_url(url1.replace("https", "http"))
    norm_url2 = strip_jsessionid_from_url(url2.replace("https", "http"))
    if norm_url1 == norm_url2:
        return True
    return False

def strip_jsessionid_from_url(url):
    url = re.sub(r";jsessionid=\w+", "", url)
    return url

def get_link_target(url, base_url, strip_jsessionid=True):
    if strip_jsessionid:
        url = strip_jsessionid_from_url(url)
    if base_url:
        url = urllib.parse.urljoin(base_url, url)
    return url

def sql_escape_string(value):
    if value == None:
        return "null"
    value = value.replace("'", "''")
    return value

def sql_bool(is_value):
    if is_value==True:
        return "true"
    if is_value==False:
        return "false"
    return "null"

def run_sql(db, q):
    q = q.strip()
    if not q:
        return
    start = time.time()
    try:
        con = db.engine.connect()
        trans = con.begin()
        con.execute(q)
        trans.commit()
    except exc.ProgrammingError as e:
        pass
    finally:
        con.close()

def get_sql_answer(db, q):
    row = db.engine.execute(sql.text(q)).first()
    if row:
        return row[0]
    return None

def get_sql_answers(db, q):
    rows = db.engine.execute(sql.text(q)).fetchall()
    if not rows:
        return []
    return [row[0] for row in rows if row]

def get_sql_rows(db, q):
    rows = db.engine.execute(sql.text(q)).fetchall()
    if not rows:
        return []
    return rows

def get_sql_dict_rows(query, values):
    from app import get_db_cursor
    with get_db_cursor() as cursor:
        cursor.execute(query, values)
        rows = cursor.fetchall()
    return rows

# https://github.com/psycopg/psycopg2/issues/897
def build_row_dict(columns, row):
    index = 0
    dict = {}
    for key in columns:
        value = row[index]
        dict[key] = value
        index += 1
    return dict

def cursor_rows_to_dicts(column_string, cursor_rows):
    column_list = column_string.replace(" ", "").split(",")
    response = []
    for row in cursor_rows:
        row_dict = build_row_dict(column_list, row)
        response.append(row_dict)
    return response


def normalize_title(title):
    if not title:
        return ""

    # just first n characters
    response = title[0:500]

    # lowercase
    response = response.lower()

    # deal with unicode
    response = unidecode(str(response))

    # has to be before remove_punctuation
    # the kind in titles are simple <i> etc, so this is simple
    response = clean_html(response)

    # remove articles and common prepositions
    response = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", response)

    # remove everything except alphas
    response = remove_everything_but_alphas(response)

    return response


# from https://gist.github.com/douglasmiranda/5127251
# deletes a key from nested dict
def delete_key_from_dict(dictionary, key):
    for k, v in dictionary.items():
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in delete_key_from_dict(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                for result in delete_key_from_dict(key, d):
                    yield result


def restart_dynos(app_name, dyno_prefix):
    heroku_conn = heroku3.from_key(os.getenv('HEROKU_API_KEY'))
    app = heroku_conn.apps()[app_name]
    dynos = app.dynos()
    for dyno in dynos:
        if dyno.name.startswith(dyno_prefix):
            dyno.restart()
            print("restarted {} on {}!".format(dyno.name, app_name))

def is_same_publisher(publisher1, publisher2):
    if publisher1 and publisher2:
        return normalize(publisher1) == normalize(publisher2)
    return False

def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    if isinstance(o, np.int64):
        return int(o)
    raise TypeError(repr(o) + " is not JSON serializable")

# from https://stackoverflow.com/a/50762571/596939
def jsonify_fast_no_sort(*args, **kwargs):
    if args and kwargs:
        raise TypeError('jsonify() behavior undefined when passed both args and kwargs')
    elif len(args) == 1:  # single args are passed directly to dumps()
        data = args[0]
    else:
        data = args or kwargs

    # turn this to False to be even faster, but warning then responses may not cache
    sort_keys = False

    return current_app.response_class(
        dumps(data,
              skipkeys=True,
              ensure_ascii=True,
              check_circular=False,
              allow_nan=True,
              cls=None,
              default=myconverter,
              indent=None,
              # separators=None,
              sort_keys=sort_keys) + '\n', mimetype=current_app.config['JSONIFY_MIMETYPE']
    )


# from https://stackoverflow.com/a/50762571/596939
def jsonify_fast(*args, **kwargs):
    if args and kwargs:
        raise TypeError('jsonify() behavior undefined when passed both args and kwargs')
    elif len(args) == 1:  # single args are passed directly to dumps()
        data = args[0]
    else:
        data = args or kwargs

    # turn this to False to be even faster, but warning then responses may not cache
    sort_keys = True

    return current_app.response_class(
        dumps(data,
              skipkeys=True,
              ensure_ascii=True,
              check_circular=False,
              allow_nan=True,
              cls=None,
              default=myconverter,
              indent=None,
              # separators=None,
              sort_keys=sort_keys) + '\n', mimetype=current_app.config['JSONIFY_MIMETYPE']
    )

def find_normalized_license(text):
    if not text:
        return None

    normalized_text = text.replace(" ", "").replace("-", "").lower()

    # the lookup order matters
    # assumes no spaces, no dashes, and all lowercase
    # inspired by https://github.com/CottageLabs/blackbox/blob/fc13e5855bd13137cf1ef8f5e93883234fdab464/service/licences.py
    # thanks CottageLabs!  :)

    license_lookups = [
        ("koreanjpathol.org/authors/access.php", "cc-by-nc"),  # their access page says it is all cc-by-nc now
        ("elsevier.com/openaccess/userlicense", "elsevier-specific: oa user license"),  #remove the - because is removed in normalized_text above
        ("pubs.acs.org/page/policy/authorchoice_termsofuse.html", "acs-specific: authorchoice/editors choice usage agreement"),

        ("creativecommons.org/licenses/byncnd", "cc-by-nc-nd"),
        ("creativecommonsattributionnoncommercialnoderiv", "cc-by-nc-nd"),
        ("ccbyncnd", "cc-by-nc-nd"),

        ("creativecommons.org/licenses/byncsa", "cc-by-nc-sa"),
        ("creativecommonsattributionnoncommercialsharealike", "cc-by-nc-sa"),
        ("ccbyncsa", "cc-by-nc-sa"),

        ("creativecommons.org/licenses/bynd", "cc-by-nd"),
        ("creativecommonsattributionnoderiv", "cc-by-nd"),
        ("ccbynd", "cc-by-nd"),

        ("creativecommons.org/licenses/bysa", "cc-by-sa"),
        ("creativecommonsattributionsharealike", "cc-by-sa"),
        ("ccbysa", "cc-by-sa"),

        ("creativecommons.org/licenses/bync", "cc-by-nc"),
        ("creativecommonsattributionnoncommercial", "cc-by-nc"),
        ("ccbync", "cc-by-nc"),

        ("creativecommons.org/licenses/by", "cc-by"),
        ("creativecommonsattribution", "cc-by"),
        ("ccby", "cc-by"),

        ("creativecommons.org/publicdomain/zero", "cc0"),
        ("creativecommonszero", "cc0"),

        ("creativecommons.org/publicdomain/mark", "pd"),
        ("publicdomain", "pd"),

        # ("openaccess", "oa")
    ]

    for (lookup, license) in license_lookups:
        if lookup in normalized_text:
            if license=="pd":
                try:
                    if "worksnotinthepublicdomain" in normalized_text.decode(errors='ignore'):
                        return None
                except:
                    # some kind of unicode exception
                    return None
            return license
    return None

def for_sorting(x):
    if x is None:
        return float('inf')
    return x

def response_json(r):
    from flask import make_response
    response = make_response(r.json(), r.status_code)
    response.mimetype = "application/json"
    return response

def abort_json(status_code, msg):
    from flask import make_response
    from flask import abort

    body_dict = {"HTTP_status_code": status_code, "message": msg, "error": True}
    response_json = json.dumps(body_dict, sort_keys=True, indent=4)
    response = make_response(response_json, status_code)
    response.mimetype = "application/json"
    abort(response)

def format_currency(amount, cents=False):
    if amount == None:
        return None

    if not cents:
        amount = round(round(amount))
        my_string = locale.currency(amount, grouping=True)
        my_string = my_string.replace(".00", "")
    else:
        my_string = locale.currency(amount, grouping=True)
    return my_string


def format_percent(amount, num_decimals=0):
    if amount == None:
        return None

    my_string = "{:0." + str(num_decimals) + "f}%"
    my_string = my_string.format(amount)
    return my_string

def format_with_commas(amount, num_decimals=0):
    if amount == None:
        return None

    try:
        my_string = "{:0,." + str(num_decimals) + "f}"
        my_string = my_string.format(amount)
        return my_string
    except:
        return locale.format('%d', amount, True)

def get_ip(request):
    # from http://stackoverflow.com/a/12771438/596939
    if request.headers.getlist("X-Forwarded-For"):
       ip = request.headers.getlist("X-Forwarded-For")[0]
    else:
       ip = request.remote_addr
    return ip

# this is to support fully after-flask response sent efforts
# from # https://stackoverflow.com/a/51013358/596939
# use like
# @app.after_response
# def say_hi():
#     print("hi")
class AfterResponse:
    def __init__(self, app=None):
        self.callbacks = []
        if app:
            self.init_app(app)

    def __call__(self, callback):
        self.callbacks.append(callback)
        return callback

    def init_app(self, app):
        # install extension
        app.after_response = self

        # install middleware
        app.wsgi_app = AfterResponseMiddleware(app.wsgi_app, self)

    def flush(self):
        for fn in self.callbacks:
            try:
                fn()
            except Exception:
                traceback.print_exc()

class AfterResponseMiddleware:
    def __init__(self, application, after_response_ext):
        self.application = application
        self.after_response_ext = after_response_ext

    def __call__(self, environ, after_response):
        iterator = self.application(environ, after_response)
        try:
            return ClosingIterator(iterator, [self.after_response_ext.flush])
        except Exception:
            traceback.print_exc()
            return iterator


def authenticated_user_id():
    jwt_identity = get_jwt_identity()
    return jwt_identity.get('user_id', None) if jwt_identity else None


def convert_to_utf_8(file_name):
    with open(file_name, 'rb') as input_file:
        sample = input_file.read(1024*1024)
        if not sample:
            return file_name

        # first, look for a unicode BOM
        # https://unicodebook.readthedocs.io/guess_encoding.html#check-for-bom-markers
        BOMS = (
            (BOM_UTF8,     'UTF-8'),
            (BOM_UTF32_BE, 'UTF-32-BE'),
            (BOM_UTF32_LE, 'UTF-32-LE'),
            (BOM_UTF16_BE, 'UTF-16-BE'),
            (BOM_UTF16_LE, 'UTF-16-LE'),
        )

        possible_encodings = [encoding for bom, encoding in BOMS if sample.startswith(bom)]

        # look for UTF-32 or UTF-16 by null byte frequency
        nulls = len([c for c in sample if c == b'\x00']) / float(len(sample))
        leading_nulls = len([c for i, c in enumerate(sample) if c == b'\x00' and i % 4 == 0]) / float(len(sample)/4)

        if nulls > .6  :
            if leading_nulls > .9:
                possible_encodings.append('UTF-32-BE')
            else:
                possible_encodings.append('UTF-32-LE')
        elif nulls > .1:
            if leading_nulls > .9:
                possible_encodings.append('UTF-16-BE')
            else:
                possible_encodings.append('UTF-16-LE')

        possible_encodings.append('UTF-8')
        possible_encodings.append('windows-1252')

        chardet_encoding = chardet.detect(sample)['encoding']
        if chardet_encoding:
            possible_encodings.append(chardet_encoding)

        possible_encodings.append('cp437')


        for pe in possible_encodings:
            try:
                new_file_name = tempfile.mkstemp('_{}_to_utf8_{}'.format(pe, os.path.split(file_name)[-1]))[1]
                with codecs.open(file_name, 'r', pe) as input_file:
                    with codecs.open(new_file_name, 'w', 'utf-8') as output_file:
                        while True:
                            contents = input_file.read(1024*1024)
                            if not contents:
                                break
                            output_file.write(contents)
                return new_file_name
            except UnicodeDecodeError:
                continue

    raise UnicodeError("Can't determine text encoding (tried {}).".format(possible_encodings))


def write_to_tempfile(file_contents, strip=False):
    if strip:
        lines = file_contents.strip().split('\n')
        lines = [line.strip() for line in lines]
        file_contents = '\n'.join(lines)

    temp_file_name = tempfile.mkstemp()[1]
    with codecs.open(temp_file_name, 'w', 'utf-8') as temp_file:
        temp_file.write(file_contents)
    return temp_file_name

# f5 from https://www.peterbe.com/plog/uniqifiers-benchmark
def uniquify_list(seq, idfun=None):
   # order preserving
   if idfun is None:
       def idfun(x): return x
   seen = {}
   result = []
   for item in seq:
       marker = idfun(item)
       # in old Python versions:
       # if seen.has_key(marker)
       # but in new ones:
       if marker in seen: continue
       seen[marker] = 1
       result.append(item)
   return result
