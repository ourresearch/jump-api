import json
from pprint import pprint
import shortuuid

from app import get_db_cursor
from views import get_saved_scenario



a_b_package_ids = ["P2NFgz7B", "PN3juRC5"]
with get_db_cursor() as cursor:
    command = """select distinct username
        from jump_account_combo_view 
        where consortium_package_id in ('P2NFgz7B', 'PN3juRC5') order by consortium_package_id"""
    cursor.execute(command)
    rows = cursor.fetchall()
    usernames = [row["username"] for row in rows]

    command = """select *
        from jump_account_combo_view 
        where consortium_package_id in ('P2NFgz7B', 'PN3juRC5') order by package_id asc"""
    cursor.execute(command)
    campuses = cursor.fetchall()

for username in usernames:
    scenario_id_february = [campus["scenario_id"] for campus in campuses if campus["scenario_name"] == "February Scenario" and campus["username"]==username][0]
    scenario_id_smaller_february = [campus["scenario_id"] for campus in campuses if campus["scenario_name"] == "February Smaller Scenario" and campus["username"]==username][0]

    my_scenario_february = get_saved_scenario(scenario_id_february, test_mode=True)
    my_scenario_smaller_february = get_saved_scenario(scenario_id_smaller_february, test_mode=True)
    my_scenario_february_live = my_scenario_february.live_scenario
    my_scenario_smaller_february_live = my_scenario_smaller_february.live_scenario

    # print username, my_scenario_february, my_scenario_smaller_february
    # print
    # print username, len(my_scenario_february_live.subscribed), len(my_scenario_smaller_february_live.subscribed)
    # print "{} ${:,} ${:,}".format(username, int(my_scenario_february_live.cost), int(my_scenario_smaller_february_live.cost))
    # print "{} {}% {}%".format(username, int(my_scenario_february_live.use_instant_percent), int(my_scenario_smaller_february_live.use_instant_percent))

    print "{};{};{};{:};{:};{};{}".format(
        username,
        len(my_scenario_february_live.subscribed), len(my_scenario_smaller_february_live.subscribed),
        int(my_scenario_february_live.cost), int(my_scenario_smaller_february_live.cost),
        int(my_scenario_february_live.use_instant_percent), int(my_scenario_smaller_february_live.use_instant_percent))

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
#     print [(j.title, j.ncppu) for j in my_scenario.live_scenario.journals_sorted_ncppu[0:3]]
#
#     top_number = 50
#     if campus["consortium_package_id"] == "P2NFgz7B":
#         top_number = 200
#     top_issnls = [j.issn_l for j in my_scenario.live_scenario.journals_sorted_ncppu[0:top_number] if j.subscribed]
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
#         scenario_definition_dict = my_scenario.to_dict_saved()
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
