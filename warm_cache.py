# coding: utf-8

import os
import sys
import requests
import time
import argparse

from package import Package
from util import elapsed

def warm_the_cache():

    packages = Package.query.all()
    for package in packages:
        print u"\nstart: {} {}".format(package.package_id, package)
        start_time = time.time()
        url = "https://cdn.unpaywalljournals.org/data/common/{}?secret={}".format(
            package.package_id, os.getenv("JWT_SECRET_KEY"))
        headers = {"Cache-Control": "public, max-age=31536000"}
        r = requests.get(url, headers=headers)
        print u"1st: {} {} {}".format(package.package_id, r.status_code, elapsed(start_time))

        start_time = time.time()
        r = requests.get(url, headers=headers)
        print u"2nd: {} {} {}".format(package.package_id, r.status_code, elapsed(start_time))



# python import_accounts.py ~/Downloads/new_accounts.csv
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    warm_the_cache()



