# coding: utf-8

import os
import sys
import requests
import time
import random
import argparse

from util import elapsed

host = "https://{}.herokuapp.com".format(os.getenv("APP_NAME"))

urls = """
{host}/institution/institution-Afxc4mAYXoJH?jwt={jwt1}
{host}/publisher/package-3WkCDEZTqo6S?jwt={jwt1}
{host}/scenario/scenario-QC2kbHfUhj9W/journals?jwt={jwt1}
{host}/scenario/tGUVWRiN/journals?jwt={jwt1}
{host}/scenario/6it6ajJd/journals?jwt={jwt1}
{host}/scenario/CBy9gUC3/journals?jwt={jwt1}
{host}/scenario/EcUvEELe/journals?jwt={jwt1}
{host}/scenario/GcAsm5CX/journals?jwt={jwt1}
{host}/scenario/aAFAuovt/journals?jwt={jwt1}
{host}/scenario/CBy9gUC3/member-institutions?jwt={jwt1}
{host}/publisher/package-NRmCNX8HD674?jwt={jwt2}
{host}/publisher/package-NRmCNX8HD674/apcs?jwt={jwt2}
{host}/scenario/scenario-VCebMrfWahSZ/journals?jwt={jwt2}
""".format(host=host, jwt1=os.getenv("WARM_CACHE_JWT1"), jwt2=os.getenv("WARM_CACHE_JWT2")).split()

def warm_the_cache():

    while True:
        random.shuffle(urls)
        for url in urls:
            start_time = time.time()
            display_url = url.split("?")[0]
            sleep_time = 3
            print "warm_cache: requesting {}".format(display_url)
            r = requests.get(url)
            if (r.status_code > 200) and (r.status_code != 404):
                display_url = url
                sleep_time = 60
            print u"warm_cache: finished {} in {}s with status {}".format(display_url, elapsed(start_time), r.status_code)
            time.sleep(sleep_time)


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



