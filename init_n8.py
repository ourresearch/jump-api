# coding: utf-8

import os
import sys
import random
import datetime
from time import time
from time import sleep
from collections import OrderedDict
import shortuuid

import argparse

from app import db
from app import get_db_cursor
from package import Package
from saved_scenario import SavedScenario
from saved_scenario import save_raw_scenario_to_db
from saved_scenario import get_latest_scenario_raw
from n8_uni_result import N8UniResult
from util import safe_commit
from util import get_sql_answer



def copy_into_n8_package(old_package_id, new_package_id, copy_counter=True, copy_prices=True, copy_perpetual_access=True, copy_apcs=False):
    command = ""

    if copy_counter:
        command += """
            insert into jump_counter (issn_l, package_id, journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type, created) (
                select issn_l, '{new_package_id}', journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type, created
                from jump_counter
                where package_id = '{old_package_id}'
            );
            
            insert into jump_counter_input (issn, journal_name, total, package_id, report_year, report_name, report_version, metric_type, yop, access_type) (
                select issn, journal_name, total, '{new_package_id}', report_year, report_name, report_version, metric_type, yop, access_type
                from jump_counter_input
                where package_id = '{old_package_id}'
            );""".format(new_package_id=new_package_id, old_package_id=old_package_id)

    if copy_perpetual_access:
        command += """        
            insert into jump_perpetual_access (package_id, issn_l, start_date, end_date, created) (
                select '{new_package_id}', issn_l, start_date, end_date, created
                from jump_perpetual_access
                where package_id = '{old_package_id}'
            );
            
            insert into jump_perpetual_access_input (package_id, issn, start_date, end_date) (
                select '{new_package_id}', issn, start_date, end_date
                from jump_perpetual_access_input
                where package_id = '{old_package_id}'
            );""".format(new_package_id=new_package_id, old_package_id=old_package_id)

    if copy_prices:
        command += """        
            insert into jump_journal_prices (package_id, publisher, title, issn_l, price, created) (
                select '{new_package_id}', publisher, title, issn_l, price, created
                from jump_journal_prices
                where package_id = '{old_package_id}'
            );
            
            insert into jump_journal_prices_input (package_id, publisher, issn, price) (
                select '{new_package_id}', publisher, issn, price
                from jump_journal_prices_input
                where package_id = '{old_package_id}'
            );""".format(new_package_id=new_package_id, old_package_id=old_package_id)

    if copy_apcs:
        command += """                
            insert into jump_apc_authorships (
               package_id, doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc) (
               select '{new_package_id}', doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc
               from jump_apc_authorships
               where package_id = '{old_package_id}'
            );""".format(new_package_id=new_package_id, old_package_id=old_package_id)

    print command
    with get_db_cursor() as cursor:
        cursor.execute(command)



def package_create(jusp_id, institution_id, package_type):

    jisc_package_id = u"package-jiscels{}".format(jusp_id)
    package_id = u"package-n8els_{}_{}".format(jusp_id, package_type.replace(" ", ""))
    package_name = u"Elsevier n8 ({})".format(package_type)
    scenario_id = u"scenario-n8els_{}_{}".format(jusp_id, package_type.replace(" ", ""))
    scenario_name = u"n8 ({})".format(package_type)

    my_package = Package.query.get(package_id)
    if not my_package:
        print u"package {} doesn't exist, making".format(package_id)
        my_package = Package(
            package_id=package_id,
            publisher="Elsevier",
            package_name=package_name,
            created=datetime.datetime.utcnow().isoformat(),
            institution_id=institution_id,
            is_demo=False,
            currency="GBP"
        )
        db.session.add(my_package)
        print my_package
        safe_commit(db)

        if package_type == "own pta":
            copy_into_n8_package(old_package_id=jisc_package_id, new_package_id=package_id, copy_perpetual_access=True)
        elif package_type == "group pta":
            copy_into_n8_package(old_package_id=jisc_package_id, new_package_id=package_id, copy_perpetual_access=False)
        elif package_type == "uk pta":
            copy_into_n8_package(old_package_id=jisc_package_id, new_package_id=package_id, copy_perpetual_access=False)

    my_scenario = SavedScenario.query.get(scenario_id)
    if not my_scenario:
        print u"scenario {} doesn't exist, making".format(scenario_id)
        my_scenario = SavedScenario(False, scenario_id, None)
        my_scenario.package_id = package_id
        my_scenario.created = datetime.datetime.utcnow().isoformat()
        db.session.add(my_scenario)
        safe_commit(db)

    print "updating settings, including big deal cost from jisc package"
    big_deal_price = get_sql_answer(db, "select big_deal_cost from jump_account_package where package_id = '{}';".format(jisc_package_id))

    dict_to_save = my_scenario.to_dict_saved_from_db()
    dict_to_save["name"] = scenario_name
    dict_to_save["configs"]["cost_bigdeal"] = big_deal_price
    dict_to_save["configs"]["cost_bigdeal_increase"] = 2
    dict_to_save["configs"]["include_social_networks"] = True # set to true
    dict_to_save["configs"]["weight_authorship"] = 0 # 100
    dict_to_save["configs"]["weight_citation"] = 0 # 10
    save_raw_scenario_to_db(scenario_id, dict_to_save, None)


def update_group_pta(jusp_id, group_jusp_ids):
    print u"in update_group_pta with {}".format(jusp_id)

    package_type = "group pta"
    package_id = u"package-n8els_{}_{}".format(jusp_id, package_type.replace(" ", ""))
    jisc_package_ids = [u"package-jiscels{}".format(b) for b in group_jusp_ids]
    jisc_package_ids_string = ", ".join([u"'{}'".format(a) for a in jisc_package_ids])

    command = """        
        delete from jump_perpetual_access where package_id = '{package_id}';
        delete from jump_perpetual_access_input where package_id = '{package_id}';
        """.format(package_id=package_id)
    # print command
    with get_db_cursor() as cursor:
        cursor.execute(command)

    command = """        
        insert into jump_perpetual_access (package_id, issn_l, start_date, end_date) (
            select '{package_id}', issn_l, coalesce(min(start_date), '1850-01-01'::timestamp) as start_date, coalesce(max(end_date), max(end_date), null) as end_date
            from jump_perpetual_access
            where package_id in ({jisc_package_ids_string})
            group by issn_l
        );
        
        insert into jump_perpetual_access_input (package_id, issn, start_date, end_date) (
            select '{package_id}', issn_l as issn, coalesce(min(start_date), '1850-01-01'::timestamp) as start_date, coalesce(max(end_date), max(end_date), null) as end_date
            from jump_perpetual_access
            where package_id in ({jisc_package_ids_string})
            group by issn_l
        );""".format(package_id=package_id, jisc_package_ids_string=jisc_package_ids_string)

    print command
    with get_db_cursor() as cursor:
        cursor.execute(command)




def copy_subscriptions(jusp_id, package_type, subscriptions):
    scenario_id = u"scenario-n8els_{}_{}".format(jusp_id, package_type.replace(" ", ""))

    my_scenario = SavedScenario.query.get(scenario_id)
    dict_to_save = my_scenario.to_dict_saved_from_db()
    dict_to_save["subrs"] = subscriptions
    save_raw_scenario_to_db(scenario_id, dict_to_save, None)


def set_non_own_subscriptions(main_jusp_id, group_jusp_ids, package_type):
    main_scenario_id = u"scenario-n8els_{}_ownpta".format(main_jusp_id)
    (updated, main_scenario_dict) = get_latest_scenario_raw(main_scenario_id)
    main_subscriptions = main_scenario_dict["subrs"]

    all_subscriptions = []

    for jusp_id in group_jusp_ids:
        scenario_id = u"scenario-n8els_{}_ownpta".format(jusp_id)
        (updated, my_source_scenario_dict) = get_latest_scenario_raw(scenario_id)
        print "subscriptions: ", jusp_id, len(my_source_scenario_dict["subrs"])
        all_subscriptions += my_source_scenario_dict["subrs"]
        print "len all_subscriptions: ", len(list(set(all_subscriptions)))

    all_subscriptions = [sub for sub in all_subscriptions if sub not in main_subscriptions]
    all_subscriptions_dedup = list(set(all_subscriptions))

    copy_subscriptions(main_jusp_id, package_type, all_subscriptions_dedup)


# lan lancaster institution-4QK9FfFudHii https://unsub.org/i/institution-4QK9FfFudHii/p/package-oiajkfDidZWB/s/5QiKNg5m
# liv liverpool institution-D9TtsdbRs6du https://unsub.org/i/institution-D9TtsdbRs6du/p/package-wGLwEncjVvAx/s/eAj75CHL
# man manchester institution-3BGTjKioPdiZ https://unsub.org/i/institution-3BGTjKioPdiZ/p/package-DsUbYh6JV42N/s/scenario-ijURRDqV
# yor york institution-3pQc7HbKgqYD https://unsub.org/i/institution-3pQc7HbKgqYD/p/package-ioUUYHNQRwom/s/hPWVrTDf

def get_issnls(issns):
    from journalsdb import get_journal_metadata
    response = [get_journal_metadata(issn)["issn_l"] for issn in issns]
    return response

# python init_n8.py
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff :)")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    subs = []
    group_jusp_data = OrderedDict()
    group_jusp_data["lan"] = {"institution_id": "institution-4QK9FfFudHii", "orig_scenario_id": "5QiKNg5m"} #lancaster
    group_jusp_data["liv"] = {"institution_id": "institution-D9TtsdbRs6du", "orig_scenario_id": "eAj75CHL"} #liverpool
    group_jusp_data["man"] = {"institution_id": "institution-3BGTjKioPdiZ", "orig_scenario_id": "scenario-ijURRDqV"} #manchester
    group_jusp_data["yor"] = {"institution_id": "institution-3pQc7HbKgqYD", "orig_scenario_id": "hPWVrTDf"} #york

    group_jusp_data["ncl"] = {"institution_id": "institution-rNFhQD3woQaT", "subs": subs} #newcastle
    group_jusp_data["dur"] = {"institution_id": "institution-PpAFKbC7p7e5", "subs": subs} #durham
    group_jusp_data["lee"] = {"institution_id": "institution-iW8e2beWGxsi", "subs": subs} #leeds
    group_jusp_data["she"] = {"institution_id": "institution-3bpqX3Wzkd7e", "subs": subs} #sheffield
    group_jusp_data["cam"] = {"institution_id": "institution-N68ARB5Hr4AM", "subs": subs} #cambridge
    group_jusp_data["ucl"] = {"institution_id": "institution-jiscucl", "subs": subs} #university college london, not an unsub subscriber

    # group_jusp_data["icl"] = {"institution_id": "institution-jiscicl", "subs": subs} #imperial college london, just to see


    if False:
        # for jusp_id, data in group_jusp_data.iteritems():
        #     print jusp_id, data
        #     package_create(jusp_id, data["institution_id"], "own pta")
        #     # pta copied over in package_create from own jisc package
        #
        #     package_create(jusp_id, data["institution_id"], "group pta")
        #     update_group_pta(jusp_id, group_jusp_data.keys())

        # for jusp_id, data in group_jusp_data.iteritems():
        #     # my_source_scenario_dict = get_latest_scenario_raw(data["orig_scenario_id"])
        #     # subscriptions = my_source_scenario_dict["subrs"]
        #
        #     subscriptions = get_issnls(data["subs"])
        #     # print jusp_id, len(subscriptions), len(data["subs"])
        #     copy_subscriptions(jusp_id, "own pta", subscriptions)

        # for jusp_id, data in group_jusp_data.iteritems():
        #     set_non_own_subscriptions(jusp_id, group_jusp_data.keys(), "group pta")
        pass

    print "gathering results"
    results = []
    for jusp_id in group_jusp_data.keys():
        results.append(N8UniResult(jusp_id).to_list())

    for result_number in range(0, len(results[0])):
        print ";".join([str(results[column_number][result_number]) for column_number in range(0, len(results))])


