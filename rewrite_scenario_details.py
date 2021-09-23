import json
from pprint import pprint
import shortuuid
from time import time

from app import get_db_cursor
from saved_scenario import get_latest_scenario_raw
from util import elapsed

#
#
# a_b_package_ids = ["P2NFgz7B", "PN3juRC5"]
# with get_db_cursor() as cursor:
#     command = """select distinct username
#         from jump_account_combo_view
#         where consortium_package_id in ('P2NFgz7B', 'PN3juRC5') order by consortium_package_id"""
#     cursor.execute(command)
#     rows = cursor.fetchall()
#     usernames = [row["username"] for row in rows]
#
#     command = """select *
#         from jump_account_combo_view
#         where consortium_package_id in ('P2NFgz7B', 'PN3juRC5') order by package_id asc"""
#     cursor.execute(command)
#     campuses = cursor.fetchall()
#
# for username in usernames:
#     scenario_id_february = [campus["scenario_id"] for campus in campuses if campus["scenario_name"] == "February Scenario" and campus["username"]==username][0]
#     scenario_id_smaller_february = [campus["scenario_id"] for campus in campuses if campus["scenario_name"] == "February Smaller Scenario" and campus["username"]==username][0]
#
#     my_scenario_february = get_saved_scenario(scenario_id_february, test_mode=True)
#     my_scenario_smaller_february = get_saved_scenario(scenario_id_smaller_february, test_mode=True)
#     my_scenario_february_live = my_scenario_february.live_scenario
#     my_scenario_smaller_february_live = my_scenario_smaller_february.live_scenario
#
#     # print username, my_scenario_february, my_scenario_smaller_february
#     # print
#     # print username, len(my_scenario_february_live.subscribed), len(my_scenario_smaller_february_live.subscribed)
#     # print "{} ${:,} ${:,}".format(username, int(my_scenario_february_live.cost), int(my_scenario_smaller_february_live.cost))
#     # print "{} {}% {}%".format(username, int(my_scenario_february_live.use_instant_percent), int(my_scenario_smaller_february_live.use_instant_percent))
#
#     print "{};{};{};{:};{:};{};{}".format(
#         username,
#         len(my_scenario_february_live.subscribed), len(my_scenario_smaller_february_live.subscribed),
#         int(my_scenario_february_live.cost), int(my_scenario_smaller_february_live.cost),
#         int(my_scenario_february_live.use_instant_percent), int(my_scenario_smaller_february_live.use_instant_percent))

# a_b_package_ids = ["P2NFgz7B", "PN3juRC5"]
# command = """select *
#     from jump_account_combo_view
#     where consortium_package_id in ('P2NFgz7B', 'PN3juRC5') order by package_id asc"""
# with get_db_cursor() as cursor:
#     cursor.execute(command)
#     campuses = cursor.fetchall()
#     campuses = [campus for campus in campuses if campus["scenario_name"] == "February Scenario"]
# # print rows
#
# for campus in campuses:
#     print
#     print campus["username"], campus["consortium_package_id"]
#     scenario_id = campus["scenario_id"]
#     my_scenario = get_saved_scenario(scenario_id, test_mode=True)
#     print len(my_scenario.live_scenario.subscribed)
#     print [(j.title, j.cpu) for j in my_scenario.live_scenario.journals_sorted_cpu[0:3]]
#
#     top_number = 50
#     if campus["consortium_package_id"] == "P2NFgz7B":
#         top_number = 200
#     top_issnls = [j.issn_l for j in my_scenario.live_scenario.journals_sorted_cpu[0:top_number] if j.subscribed]
#     print len(top_issnls)
#
#     with get_db_cursor() as cursor:
#         new_scenario_id = shortuuid.uuid()[0:8]
#         new_scenario_name = "February Smaller Scenario"
#
#         command = """insert into jump_package_scenario values ('{}', '{}', '{}', sysdate)""".format(
#             my_scenario.package_id, new_scenario_id, new_scenario_name
#         )
#         print command
#         cursor.execute(command)
#
#         scenario_definition_dict = my_scenario.to_dict_saved_from_db()
#         pprint(scenario_definition_dict)
#         scenario_definition_dict["subrs"] = top_issnls
#         scenario_json = json.dumps(scenario_definition_dict)
#         print "got json"
#         scenario_json = scenario_json.replace("'", "''")
#         print "\n\n scenario_json", scenario_json
#         with get_db_cursor() as cursor:
#             print "got cursor"
#             command = u"""INSERT INTO jump_scenario_details_paid
#                 (scenario_id, updated, ip, scenario_json) values ('{}', sysdate, '128.0.0.1', '{}');""".format(
#                 new_scenario_id, scenario_json
#             )
#             # print command
#             cursor.execute(command)
#

# goal:  for each package, get all the big deal prices and price increases stored in scenario, get the most recent, save that in the package

start_time = time()
from package import Package
all_packages = Package.query.all()
print("got all packages in {} seconds".format(elapsed(start_time)))
package_ids_to_change = []
for my_package in all_packages:

    data = []
    for my_scenario in my_package.saved_scenarios:
        (updated, saved) = get_latest_scenario_raw(my_scenario.scenario_id)
        if saved:
            try:
                cost = int(float(saved["configs"]["cost_bigdeal"]))
            except ValueError:
                try:
                    cost = int(float(saved["configs"]["cost_bigdeal"].replace(",", "")))
                except ValueError:
                    continue
            except KeyError:
                continue

            increase = float(saved["configs"]["cost_bigdeal_increase"])
            if cost != 2100000:
                # print u"{} {} {} {}".format(my_scenario, new_data)
                new_data = {"updated": updated.isoformat(), "cost": cost, "increase": increase}
                data += [new_data]
    if data:
        data = sorted(data, key=lambda x: x["updated"], reverse=True)
        print(data)
        command = ""
        if my_package.big_deal_cost == None:
            package_ids_to_change += [my_package.package_id]
            command += "update jump_account_package set big_deal_cost={} where package_id='{}';".format(data[0]["cost"], my_package.package_id)
        if my_package.big_deal_cost_increase == None:
            package_ids_to_change += [my_package.package_id]
            command += "update jump_account_package set big_deal_cost_increase={} where package_id='{}';".format(data[0]["increase"], my_package.package_id)

        if command:
            with get_db_cursor() as cursor:
                print(command)
                cursor.execute(command)

        print(my_package)
        print("deduplicated: {}".format(len(list(set(package_ids_to_change)))))
        print()


    #
    # print campus["username"], campus["consortium_package_id"]
    # scenario_id = campus["scenario_id"]
    # my_scenario = get_saved_scenario(scenario_id, test_mode=True)
    # print len(my_scenario.live_scenario.subscribed)
    # print [(j.title, j.cpu) for j in my_scenario.live_scenario.journals_sorted_cpu[0:3]]
    #
    # top_number = 50
    # if campus["consortium_package_id"] == "P2NFgz7B":
    #     top_number = 200
    # top_issnls = [j.issn_l for j in my_scenario.live_scenario.journals_sorted_cpu[0:top_number] if j.subscribed]
    # print len(top_issnls)
    #
    # with get_db_cursor() as cursor:
    #     new_scenario_id = shortuuid.uuid()[0:8]
    #     new_scenario_name = "February Smaller Scenario"
    #
    #     command = """insert into jump_package_scenario values ('{}', '{}', '{}', sysdate)""".format(
    #         my_scenario.package_id, new_scenario_id, new_scenario_name
    #     )
    #     print command
    #     cursor.execute(command)
    #
    #     scenario_definition_dict = my_scenario.to_dict_saved_from_db()
    #     pprint(scenario_definition_dict)
    #     scenario_definition_dict["subrs"] = top_issnls
    #     scenario_json = json.dumps(scenario_definition_dict)
    #     print "got json"
    #     scenario_json = scenario_json.replace("'", "''")
    #     print "\n\n scenario_json", scenario_json
    #     with get_db_cursor() as cursor:
    #         print "got cursor"
    #         command = u"""INSERT INTO jump_scenario_details_paid
    #             (scenario_id, updated, ip, scenario_json) values ('{}', sysdate, '128.0.0.1', '{}');""".format(
    #             new_scenario_id, scenario_json
    #         )
    #         # print command
    #         cursor.execute(command)

