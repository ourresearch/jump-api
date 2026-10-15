# coding: utf-8

import sentry_setup  # noqa: F401  (#1685)

import sentry_sdk
from sentry_sdk.crons import capture_checkin
from sentry_sdk.crons.consts import MonitorStatus

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

# A Sentry Cron monitor is the dead-man switch (oxjob #1711): a check-in at most every 5 minutes, ok when the
# last pass warmed every page, error when one failed. No check-in for 20 minutes means the loop is stuck.
CHECKIN_EVERY_SECONDS = 300
MONITOR_CONFIG = {"schedule": {"type": "interval", "value": 5, "unit": "minute"}, "checkin_margin": 15,
                  "timezone": "UTC"}

def warm_the_cache():
    last_checkin = 0
    while True:
        random.shuffle(urls)
        pass_ok = True
        for url in urls:
            start_time = time.time()
            display_url = url.split("?")[0]
            sleep_time = 3
            print("warm_cache: requesting {}".format(display_url))
            try:
                # With no timeout, one stalled response froze this loop for good (oxjob #1711).
                r = requests.get(url, timeout=(10, 60))
            except requests.RequestException as e:
                print("warm_cache: failed {} after {}s: {!r}".format(display_url, elapsed(start_time), e))
                sentry_sdk.capture_exception(e)
                pass_ok = False
                time.sleep(60)
                continue
            if (r.status_code > 200) and (r.status_code != 404):
                display_url = url
                sleep_time = 60
                pass_ok = False
            print("warm_cache: finished {} in {}s with status {}".format(display_url, elapsed(start_time), r.status_code))
            time.sleep(sleep_time)
        if time.time() - last_checkin >= CHECKIN_EVERY_SECONDS:
            capture_checkin(monitor_slug="unsub-warm-cache", monitor_config=MONITOR_CONFIG,
                            status=MonitorStatus.OK if pass_ok else MonitorStatus.ERROR)
            last_checkin = time.time()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    warm_the_cache()
