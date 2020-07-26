# coding: utf-8

import os
import sys
import requests
from time import time
import argparse

from scenario import _load_ricks_journal_rows
from scenario import _load_hybrid_2019_from_db
from scenario import _load_journal_era_subjects_from_db
from scenario import get_oa_data_from_db
from scenario import get_society_data_from_db
from scenario import get_social_networks_data_from_db
from scenario import get_num_papers_from_db
from scenario import get_oa_recent_data_from_db

from util import elapsed


def warm_the_cache():

    start_time = time()
    _load_ricks_journal_rows()
    print u" finished _load_ricks_journal_rows in {}".format(elapsed(start_time))
    start_time = time()
    _load_hybrid_2019_from_db()
    print u" finished _load_hybrid_2019_from_db in {}".format(elapsed(start_time))
    start_time = time()
    _load_journal_era_subjects_from_db()
    print u" finished _load_journal_era_subjects_from_db in {}".format(elapsed(start_time))
    start_time = time()
    get_oa_data_from_db()
    print u" finished get_oa_data_from_db in {}".format(elapsed(start_time))
    start_time = time()
    get_society_data_from_db()
    print u" finished get_society_data_from_db in {}".format(elapsed(start_time))
    start_time = time()
    get_social_networks_data_from_db()
    print u" finished get_social_networks_data_from_db in {}".format(elapsed(start_time))
    start_time = time()
    get_num_papers_from_db()
    print u" finished get_num_papers_from_db in {}".format(elapsed(start_time))
    start_time = time()
    get_oa_recent_data_from_db()
    print u" finished get_oa_recent_data_from_db in {}".format(elapsed(start_time))
    start_time = time()



#
# def warm_the_cache():
#
#     packages = Package.query.all()
#     for package in packages:
#         if not package.is_demo_account and package.institution and not package.institution.is_demo_institution:
#             print u"\nstart: {} {}".format(package.package_id, package)
#             start_time = time.time()
#             url = "https://cdn.unpaywalljournals.org/live/data/common/{}?secret={}".format(
#                 package.package_id, os.getenv("JWT_SECRET_KEY"))
#             headers = {"Cache-Control": "public, max-age=31536000"}
#             r = requests.get(url, headers=headers)
#             print u"1st: {} {} {}".format(package.package_id, r.status_code, elapsed(start_time))
#
#             start_time = time.time()
#             r = requests.get(url, headers=headers)
#             print u"2nd: {} {} {}".format(package.package_id, r.status_code, elapsed(start_time))



# python import_accounts.py ~/Downloads/new_accounts.csv
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    warm_the_cache()



