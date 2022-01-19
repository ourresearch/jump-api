# coding: utf-8

import random
import datetime
from time import time
from time import sleep

import argparse

from app import get_db_cursor
from consortium import Consortium
from emailer import create_email, send
from util import elapsed


def consortium_calculate():
    # command = "truncate jump_scenario_computed_update_queue"
    # print command
    # with get_db_cursor() as cursor:
    #    cursor.execute(command)


    while True:
        command = "select * from jump_scenario_computed_update_queue where completed is null order by random()"
        # print command
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()

        for row in rows:
            start_time = time()
            print("in consortium_calculate, starting recompute_journal_dicts for scenario_id {}".format(
                row["scenario_id"]))

            my_consortium = Consortium(row["scenario_id"])
            my_consortium.recompute_journal_dicts()

            print("in consortium_calculate, done recompute_journal_dicts for scenario_id {} took {}s".format(
                row["scenario_id"], elapsed(start_time)))

            print("updating jump_scenario_computed_update_queue with completed")

            command = "update jump_scenario_computed_update_queue set completed=sysdate where scenario_id='{}' and completed is null".format(
                row["scenario_id"])

            # print command
            with get_db_cursor() as cursor:
                cursor.execute(command)

            if row["email"]:
                print("SENDING EMAIL")
                done_email = create_email(row["email"], 'Unsub update complete', 'update_done', {
                                'data': {
                                     'consortium_name': row.get("consortium_name", ""),
                                     'package_name': row.get("package_name", ""),
                                     'start_time': row.get("created", ""),
                                     'end_time': datetime.datetime.utcnow().isoformat(),
                                     'institution_id': row.get("institution_id", ""),
                                     'package_id': row.get("package_id", ""),
                                     'scenario_id': row["scenario_id"]
                                 }})
                send(done_email, for_real=True)
                print("SENT EMAIL DONE")

            print("DONE UPDATING", row["scenario_id"])

        sleep( 2 * random.random())


# python consortium_calculate.py
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff :)")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    consortium_calculate()



