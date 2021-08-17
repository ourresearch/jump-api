# coding: utf-8

import os
import sys
import random
import datetime
from time import time
from time import sleep
import shortuuid

import argparse

from app import db
from app import get_db_cursor
from consortium import Consortium
from package import Package
from saved_scenario import SavedScenario
from saved_scenario import save_raw_scenario_to_db
from saved_scenario import save_raw_member_institutions_included_to_db
from util import safe_commit


# heroku run --size=performance-l python consortium_recompute.py --package_id=package-3WkCDEZTqo6S -r heroku
# heroku run --size=performance-l python consortium_recompute.py --scenario_id=tGUVWRiN -r heroku
# python consortium_recompute.py --package_id=package-X9cgZdJWfmGy

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff :)")
    parser.add_argument("--package_id", type=str, default=None, help="package id of consortium to recompute")
    parser.add_argument("--scenario_id", type=str, default=None, help="scenario id of consortium to recompute")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    consortium_package_id = parsed_vars["package_id"]
    consortium_scenario_id = parsed_vars["scenario_id"]

    if consortium_scenario_id:
        new_consortia = Consortium(consortium_scenario_id)
        new_consortia.recompute_journal_dicts()
        print("recomputing {}".format(new_consortia))

    elif consortium_package_id:
        from consortium import get_consortium_ids
        consortium_ids = get_consortium_ids()
        for d in consortium_ids:
            # print d["package_id"]
            if consortium_package_id == d["package_id"]:
                print("starting to recompute row {}".format(d))
                new_consortia = Consortium(d["scenario_id"])
                new_consortia.recompute_journal_dicts()
                print("recomputing {}".format(new_consortia))
