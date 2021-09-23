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
            delete from jump_counter where package_id = '{new_package_id}';

            insert into jump_counter (issn_l, package_id, journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type, created) (
                select issn_l, '{new_package_id}', journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type, created
                from jump_counter
                where package_id = '{old_package_id}'
            );

            delete from jump_counter_input where package_id = '{new_package_id}';

            insert into jump_counter_input (issn, journal_name, total, package_id, report_year, report_name, report_version, metric_type, yop, access_type) (
                select issn, journal_name, total, '{new_package_id}', report_year, report_name, report_version, metric_type, yop, access_type
                from jump_counter_input
                where package_id = '{old_package_id}'
            );""".format(new_package_id=new_package_id, old_package_id=old_package_id)

    if copy_perpetual_access:
        command += """
            delete from jump_perpetual_access where package_id = '{new_package_id}';

            insert into jump_perpetual_access (package_id, issn_l, start_date, end_date, created) (
                select '{new_package_id}', issn_l, start_date, end_date, created
                from jump_perpetual_access
                where package_id = '{old_package_id}'
            );

            delete from jump_perpetual_access_input where package_id = '{new_package_id}';

            insert into jump_perpetual_access_input (package_id, issn, start_date, end_date) (
                select '{new_package_id}', issn, start_date, end_date
                from jump_perpetual_access_input
                where package_id = '{old_package_id}'
            );""".format(new_package_id=new_package_id, old_package_id=old_package_id)

    if copy_prices:
        command += """
            delete from jump_journal_prices where package_id = '{new_package_id}';

            insert into jump_journal_prices (package_id, publisher, title, issn_l, price, created) (
                select '{new_package_id}', publisher, title, issn_l, price, created
                from jump_journal_prices
                where package_id = '{old_package_id}'
            );

            delete from jump_journal_prices_input where package_id = '{new_package_id}';

            insert into jump_journal_prices_input (package_id, publisher, issn, price) (
                select '{new_package_id}', publisher, issn, price
                from jump_journal_prices_input
                where package_id = '{old_package_id}'
            );""".format(new_package_id=new_package_id, old_package_id=old_package_id)

    if copy_apcs:
        command += """
            delete from jump_apc_authorships where package_id = '{new_package_id}';

            insert into jump_apc_authorships (
               package_id, doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc) (
               select '{new_package_id}', doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc
               from jump_apc_authorships
               where package_id = '{old_package_id}'
            );""".format(new_package_id=new_package_id, old_package_id=old_package_id)

    command += """         
        delete from jump_raw_file_upload_object where package_id = '{new_package_id}';
           
        insert into jump_raw_file_upload_object (
           package_id, file, bucket_name, object_name, created, num_rows) (
           select '{new_package_id}', file, bucket_name, object_name, created, num_rows
           from jump_raw_file_upload_object
           where package_id = '{old_package_id}'
        );""".format(new_package_id=new_package_id, old_package_id=old_package_id)

    print(command)
    with get_db_cursor() as cursor:
        cursor.execute(command)



def package_create(jusp_id, institution_id, package_type):

    jisc_package_id = "package-solojiscels{}".format(jusp_id)
    package_id = "package-n8els_{}_{}".format(jusp_id, package_type.replace(" ", ""))
    package_name = "Elsevier n8 ({})".format(package_type)
    scenario_id = "scenario-n8els_{}_{}".format(jusp_id, package_type.replace(" ", ""))
    scenario_name = "n8 ({})".format(package_type)

    big_deal_cost = get_sql_answer(db, "select big_deal_cost from jump_account_package where package_id = '{}';".format(jisc_package_id))

    my_package = Package.query.get(package_id)

    if not my_package:
        print(("package {} doesn't exist, making".format(package_id)))
        my_package = Package(
            package_id=package_id,
            publisher="Elsevier",
            package_name=package_name,
            created=datetime.datetime.utcnow().isoformat(),
            institution_id=institution_id,
            is_demo=False,
            currency="GBP",
            big_deal_cost=big_deal_cost,
            big_deal_cost_increase=2.0
        )
        db.session.add(my_package)
        print(my_package)
        safe_commit(db)

        if package_type == "own pta":
            copy_into_n8_package(old_package_id=jisc_package_id, new_package_id=package_id, copy_perpetual_access=True)
        else:
            copy_into_n8_package(old_package_id=jisc_package_id, new_package_id=package_id, copy_perpetual_access=False)

    my_scenario = SavedScenario.query.get(scenario_id)
    if not my_scenario:
        print(("scenario {} doesn't exist, making".format(scenario_id)))
        my_scenario = SavedScenario(False, scenario_id, None)
        my_scenario.package_id = package_id
        my_scenario.created = datetime.datetime.utcnow().isoformat()
        db.session.add(my_scenario)
        safe_commit(db)

        print(("updating scenario {}".format(scenario_id)))

        dict_to_save = my_scenario.to_dict_saved_from_db()
        dict_to_save["name"] = scenario_name
        dict_to_save["configs"]["include_social_networks"] = True # set to true
        dict_to_save["configs"]["weight_authorship"] = 0 # 100
        dict_to_save["configs"]["weight_citation"] = 0 # 10
        save_raw_scenario_to_db(scenario_id, dict_to_save, None)


def update_group_pta(jusp_id, group_jusp_ids, package_type):
    print(("in update_group_pta with {} {}".format(jusp_id, group_name)))

    package_id = "package-n8els_{}_{}".format(jusp_id, package_type.replace(" ", ""))
    jisc_package_ids = ["package-solojiscels{}".format(b) for b in group_jusp_ids]
    jisc_package_ids_string = ", ".join(["'{}'".format(a) for a in jisc_package_ids])

    command = """        
        delete from jump_perpetual_access where package_id = '{package_id}';
        delete from jump_perpetual_access_input where package_id = '{package_id}';
        """.format(package_id=package_id)
    # print command
    with get_db_cursor() as cursor:
        cursor.execute(command)

    command = """        
        insert into jump_perpetual_access (package_id, issn_l, start_date, end_date) (
            select '{package_id}', issn_l, coalesce(min(start_date), '1850-01-01'::timestamp) as start_date, coalesce(max(coalesce(end_date, '2050-01-01'::timestamp)), max(coalesce(end_date, '2050-01-01'::timestamp)), '2050-01-01'::timestamp) as end_date
            from jump_perpetual_access
            where package_id in ({jisc_package_ids_string})
            group by issn_l
        );
        
        insert into jump_perpetual_access_input (package_id, issn, start_date, end_date) (
            select '{package_id}', issn_l as issn, coalesce(min(start_date), '1850-01-01'::timestamp) as start_date, coalesce(max(coalesce(end_date, '2050-01-01'::timestamp)), max(coalesce(end_date, '2050-01-01'::timestamp)), '2050-01-01'::timestamp) as end_date
            from jump_perpetual_access
            where package_id in ({jisc_package_ids_string})
            group by issn_l
        );""".format(package_id=package_id, jisc_package_ids_string=jisc_package_ids_string)

    print(command)
    with get_db_cursor() as cursor:
        cursor.execute(command)



def set_lowest_cpu_subscriptions(jusp_id, package_type, big_deal_cost_proportion=0.5):
    scenario_id = "scenario-n8els_{}_{}".format(jusp_id, package_type.replace(" ", ""))

    my_scenario = SavedScenario.query.get(scenario_id)
    my_scenario.set_live_scenario()
    dict_to_save = my_scenario.to_dict_saved_from_db()
    big_deal_cost = float(my_scenario.package.big_deal_cost)
    subscriptions = []
    cost_so_far = 0
    print(("max cost {}".format(big_deal_cost_proportion * big_deal_cost)))
    for my_journal in my_scenario.live_scenario.journals_sorted_cpu:
        cost_so_far += my_journal.subscription_cost
        # print "cost so far {}".format(cost_so_far)
        if cost_so_far <= big_deal_cost_proportion * big_deal_cost:
            subscriptions += [my_journal]
        else:
            break
    print(("got it with {} journals".format(len(subscriptions))))
    dict_to_save["subrs"] = [j.issn_l for j in subscriptions]
    save_raw_scenario_to_db(scenario_id, dict_to_save, None)


def copy_subscriptions(jusp_id, subscriptions, package_type):
    scenario_id = "scenario-n8els_{}_{}".format(jusp_id, package_type.replace(" ", ""))

    my_scenario = SavedScenario.query.get(scenario_id)
    dict_to_save = my_scenario.to_dict_saved_from_db()
    dict_to_save["subrs"] = subscriptions
    save_raw_scenario_to_db(scenario_id, dict_to_save, None)


def set_non_own_subscriptions(main_jusp_id, group_jusp_ids, package_type):
    main_scenario_id = "scenario-n8els_{}_ownpta".format(main_jusp_id)
    (updated, main_scenario_dict) = get_latest_scenario_raw(main_scenario_id)
    main_subscriptions = main_scenario_dict["subrs"]

    all_subscriptions = []

    for jusp_id in group_jusp_ids:
        scenario_id = "scenario-n8els_{}_ownpta".format(jusp_id)
        (updated, my_source_scenario_dict) = get_latest_scenario_raw(scenario_id)
        print(("subscriptions: ", jusp_id, len(my_source_scenario_dict["subrs"])))
        all_subscriptions += my_source_scenario_dict["subrs"]
        print(("len all_subscriptions: ", len(list(set(all_subscriptions)))))

    all_subscriptions = [sub for sub in all_subscriptions if sub not in main_subscriptions]
    all_subscriptions_dedup = list(set(all_subscriptions))

    copy_subscriptions(main_jusp_id, all_subscriptions_dedup, package_type)


# lan lancaster institution-4QK9FfFudHii https://unsub.org/i/institution-4QK9FfFudHii/p/package-oiajkfDidZWB/s/5QiKNg5m
# liv liverpool institution-D9TtsdbRs6du https://unsub.org/i/institution-D9TtsdbRs6du/p/package-wGLwEncjVvAx/s/eAj75CHL
# man manchester institution-3BGTjKioPdiZ https://unsub.org/i/institution-3BGTjKioPdiZ/p/package-DsUbYh6JV42N/s/scenario-ijURRDqV
# yor york institution-3pQc7HbKgqYD https://unsub.org/i/institution-3pQc7HbKgqYD/p/package-ioUUYHNQRwom/s/hPWVrTDf

def get_issnls(issns):
    from journalsdb import get_journal_metadata
    response = [get_journal_metadata(issn).issn_l for issn in issns]
    return response

def get_group_pta_name(group_name):
    return "{} pta".format(group_name)

# python init_n8.py
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff :)")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    from init_n8_subscriptions import ncl_subs, cam_subs, she_subs, ucl_subs, lee_subs, dur_subs, icl_subs, abd_subs, ews_subs, edi_subs, sti_subs, gla_subs
    group_jusp_data = OrderedDict()
    group_jusp_data["lan"] = {"institution_id": "institution-4QK9FfFudHii", "orig_scenario_id": "5QiKNg5m"} #lancaster
    group_jusp_data["liv"] = {"institution_id": "institution-D9TtsdbRs6du", "orig_scenario_id": "eAj75CHL"} #liverpool
    group_jusp_data["man"] = {"institution_id": "institution-3BGTjKioPdiZ", "orig_scenario_id": "scenario-ijURRDqV"} #manchester
    group_jusp_data["yor"] = {"institution_id": "institution-3pQc7HbKgqYD", "orig_scenario_id": "hPWVrTDf"} #york

    group_jusp_data["ncl"] = {"institution_id": "institution-rNFhQD3woQaT", "subrs": ncl_subs} #newcastle
    group_jusp_data["dur"] = {"institution_id": "institution-PpAFKbC7p7e5", "subrs": dur_subs} #durham
    group_jusp_data["lee"] = {"institution_id": "institution-iW8e2beWGxsi", "subrs": lee_subs} #leeds
    group_jusp_data["she"] = {"institution_id": "institution-3bpqX3Wzkd7e", "subrs": she_subs} #sheffield
    group_jusp_data["cam"] = {"institution_id": "institution-N68ARB5Hr4AM", "subrs": cam_subs} #cambridge
    group_jusp_data["ucl"] = {"institution_id": "institution-jiscucl", "subrs": ucl_subs} #university college london, not an unsub subscriber
    group_jusp_data["oxf"] = {"institution_id": "institution-jiscoxf"} #oxford
    group_jusp_data["icl"] = {"institution_id": "institution-jiscicl", "subrs": icl_subs} #oxford

    group_jusp_data["abd"] = {"institution_id": "institution-cH6ZGAAtwkyy", "subrs": abd_subs} #Aberdeen
    group_jusp_data["ews"] = {"institution_id": "institution-jiscews", "subrs": ews_subs} #St Andrews
    group_jusp_data["edi"] = {"institution_id": "institution-jiscedi", "subrs": edi_subs} #Edinburgh
    group_jusp_data["sti"] = {"institution_id": "institution-jiscsti", "subrs": sti_subs} #Stirling
    group_jusp_data["gla"] = {"institution_id": "institution-jiscgla", "subrs": gla_subs} #Glasgow


    groups = {}
    groups["n8"] = """lan	liv	man	yor	ncl	dur	lee	she	cam	ucl oxf icl""".split()
    groups["scurl"] = ['abd', 'ews', 'gla', 'sti', 'edi']
    groups["n8+scurl"] = groups["n8"] + groups["scurl"]

    institution_id = "institution-Tfi2z4svqqkU"


    if True:
        for group_name, group_jusp_id_list in list(groups.items()):
            pass

            for jusp_id in group_jusp_id_list:
                print((jusp_id, group_name, group_jusp_id_list))
            #
            #
            #     package_create(jusp_id, institution_id, "own pta")
            #     # pta copied over in package_create from own jisc package
            #
            #     package_create(jusp_id, institution_id, get_group_pta_name(group_name))
            #
            #     update_group_pta(jusp_id, group_jusp_id_list, get_group_pta_name(group_name))


            for jusp_id in group_jusp_id_list:
                print(("setting subscriptions for ", jusp_id, group_name))
                data = group_jusp_data[jusp_id]
                if "orig_scenario_id" in data:
                    print(("getting subscriptions from unsub scenario for ", jusp_id, group_name))
                    (updated, my_source_scenario_dict) = get_latest_scenario_raw(data["orig_scenario_id"])
                    subscriptions = my_source_scenario_dict["subrs"]
                    copy_subscriptions(jusp_id, subscriptions, "own pta")
                elif "subrs" in data:
                    print(("getting subscriptions from python file for ", jusp_id, group_name))
                    subscriptions = get_issnls(data.get("subrs", None))
                    # print jusp_id, len(subscriptions), len(data["subrs"])
                    copy_subscriptions(jusp_id, subscriptions, "own pta")
                else:
                    print(("calculating best subscriptions for ", jusp_id, group_name))
                    set_lowest_cpu_subscriptions(jusp_id, "own pta", 0.5)

            print(("calculating best subscriptions for ", "liv", "own pta"))
            set_lowest_cpu_subscriptions("liv", "own pta", 0.425)  # set to make instant be 60%

            # don't let it cache
            db.session.expire_all()

            for jusp_id in group_jusp_id_list:
                print(("setting group subscriptions for ", jusp_id, group_name))
                data = group_jusp_data[jusp_id]
                set_non_own_subscriptions(jusp_id, group_jusp_id_list, get_group_pta_name(group_name))

    # don't let it cache
    db.session.expire_all()


    print("gathering results")

    # just pint out "n8+ now
    group_name = "n8+scurl"
    group_jusp_id_list = groups[group_name]

    results = []
    for jusp_id in group_jusp_id_list:
        results.append(N8UniResult(jusp_id, get_group_pta_name(group_name)).to_list())

    for result_number in range(0, len(results[0])):
        print(";".join([str(results[column_number][result_number]) for column_number in range(0, len(results))]))

    print("\n\n\n")
