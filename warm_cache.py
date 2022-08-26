# coding: utf-8

import os
import requests
import time
import random
import argparse

from util import elapsed

host = "https://{}.herokuapp.com".format(os.getenv("APP_NAME"))

urls = """
{host}/institution/institution-Afxc4mAYXoJH?jwt={jwt1}
{host}/publisher/package-3WkCDEZTqo6S?jwt={jwt1}
{host}/scenario/hDmEjmW6/journals?jwt={jwt1}
{host}/scenario/EBnR8kXj/journals?jwt={jwt1}
{host}/scenario/SU2Zm4nX/journals?jwt={jwt1}
{host}/scenario/hDmEjmW6/member-institutions?jwt={jwt1}
{host}/institution/institution-JnorxoyU4D8g?jwt={jwt2}
{host}/institution/institution-JnorxoyU4D8g/apc?jwt={jwt2}
{host}/publisher/package-j9NUn5ZRAdoy?jwt={jwt2}
{host}/scenario/8BABbkTQ/journals?jwt={jwt2}
""".format(host=host, jwt1=os.getenv("WARM_CACHE_JWT1"), jwt2=os.getenv("WARM_CACHE_JWT2")).split()

# WARM_CACHE_JWT1 = Jisc (team+jisc@ourresearch.org)
# WARM_CACHE_JWT2 = institution-JnorxoyU4D8g (scott+ademo@ourresearch.org)

def warm_the_cache():
    while True:
        random.shuffle(urls)
        for url in urls:
            start_time = time.time()
            display_url = url.split("?")[0]
            sleep_time = 3
            print("warm_cache: requesting {}".format(display_url))
            r = requests.get(url)
            if (r.status_code > 200) and (r.status_code != 404):
                display_url = url
                sleep_time = 60
            print("warm_cache: finished {} in {}s with status {}".format(display_url, elapsed(start_time), r.status_code))
            time.sleep(sleep_time)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    warm_the_cache()
