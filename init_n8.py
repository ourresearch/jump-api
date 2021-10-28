# coding: utf-8

import os
import sys
import random
import datetime
from time import time
from time import sleep
from collections import OrderedDict
import shortuuid
import pandas as pd
from itertools import compress

import argparse
import textwrap

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



def package_create(jusp_id, institution_id, package_type, coreplus):

    jisc_package_id_prefix = "package-jiscels{}" if coreplus else "package-solojiscels{}"
    jisc_package_id = jisc_package_id_prefix.format(jusp_id)
    n8_id_prefix = "n8els_coreplus" if coreplus else "n8els"
    package_id = "package-{}_{}_{}".format(n8_id_prefix, jusp_id, package_type.replace(" ", ""))
    package_name = "Elsevier n8 ({})".format(package_type)
    scenario_id = "scenario-{}_{}_{}".format(n8_id_prefix, jusp_id, package_type.replace(" ", ""))
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
        dict_to_save["configs"]["cost_alacart_increase"] = 0 # 8%
        dict_to_save["configs"]["cost_content_fee_percent"] = 0 # 5.7%
        save_raw_scenario_to_db(scenario_id, dict_to_save, None)


def update_group_pta(jusp_id, group_jusp_ids, package_type, coreplus):
    print(("in update_group_pta with {} {}".format(jusp_id, group_name)))

    n8_id_prefix = "n8els_coreplus" if coreplus else "n8els"
    package_id = "package-{}_{}_{}".format(n8_id_prefix, jusp_id, package_type.replace(" ", ""))
    jisc_package_ids_prefix = "package-jiscels{}" if coreplus else "package-solojiscels{}"
    jisc_package_ids = [jisc_package_ids_prefix.format(b) for b in group_jusp_ids]
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



def set_lowest_cpu_subscriptions(jusp_id, package_type, coreplus, big_deal_cost_proportion=0.5):
    n8_id_prefix = "n8els_coreplus" if coreplus else "n8els"
    scenario_id = "scenario-{}_{}_{}".format(n8_id_prefix, jusp_id, package_type.replace(" ", ""))

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


def copy_subscriptions(jusp_id, subscriptions, package_type, coreplus):
    n8_id_prefix = "n8els_coreplus" if coreplus else "n8els"
    scenario_id = "scenario-{}_{}_{}".format(n8_id_prefix, jusp_id, package_type.replace(" ", ""))

    my_scenario = SavedScenario.query.get(scenario_id)
    dict_to_save = my_scenario.to_dict_saved_from_db()
    dict_to_save["subrs"] = subscriptions
    save_raw_scenario_to_db(scenario_id, dict_to_save, None)


def set_non_own_subscriptions(main_jusp_id, group_jusp_ids, package_type, coreplus):
    n8_id_prefix = "n8els_coreplus" if coreplus else "n8els"
    main_scenario_id = "scenario-{}_{}_ownpta".format(n8_id_prefix, main_jusp_id)
    (updated, main_scenario_dict) = get_latest_scenario_raw(main_scenario_id)
    main_subscriptions = main_scenario_dict["subrs"]

    all_subscriptions = []

    for jusp_id in group_jusp_ids:
        scenario_id = "scenario-{}_{}_ownpta".format(n8_id_prefix, jusp_id)
        (updated, my_source_scenario_dict) = get_latest_scenario_raw(scenario_id)
        print(("subscriptions: ", jusp_id, len(my_source_scenario_dict["subrs"])))
        all_subscriptions += my_source_scenario_dict["subrs"]
        print(("len all_subscriptions: ", len(list(set(all_subscriptions)))))

    all_subscriptions = [sub for sub in all_subscriptions if sub not in main_subscriptions]
    all_subscriptions_dedup = list(set(all_subscriptions))

    copy_subscriptions(main_jusp_id, all_subscriptions_dedup, package_type, coreplus)


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

def fetch_inst_subs(path):
    x = pd.read_csv(path, sep=",")
    issns_by_inst={}
    for a,b in x.groupby('Institution'):
        issns_by_inst[b['Institution'].to_list()[0]]=b['ISSN'].to_list()
    return issns_by_inst

# python init_n8.py
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
            Examples of use
            ---------------

            Notes:
                - heroku local:run required to make sure environment variables are loaded

            # Show this help
            heroku local:run python init_n8.py -h
            # Run classic model WITHOUT recalculating
            heroku local:run python init_n8.py
            # Run classic model WITH recalculating
            heroku local:run python init_n8.py --recalculate 
            # Run classic model WITH recalculating AND create packages
            heroku local:run python init_n8.py --recalculate --createpkgs  
            # Run coreplus model WITHOUT recalculating
            heroku local:run python init_n8.py --coreplus
            # Run coreplus model WITH recalculating
            heroku local:run python init_n8.py --coreplus --recalculate
            # Run coreplus model WITH recalculating AND create packages
            heroku local:run python init_n8.py --coreplus --recalculate --createpkgs   
            '''))
    parser.add_argument("--coreplus", help="True if want to run the N8+ CorePlus model - otherwise classic", action="store_true", default=False)
    parser.add_argument("--recalculate", help="True if want to recalculate - otherwise not", action="store_true", default=False)
    parser.add_argument("--createpkgs", help="True if want to create packages - otherwise not. If --recalculate is False, this flag is ignored", action="store_true", default=False)

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    print("Running the '{}' model\n".format("coreplus" if parsed_args.coreplus else "classic"))

    if parsed_args.coreplus:
        core = pd.read_csv("data/n8data/subscriptions_n8_core.csv", sep=",")["ISSN"].to_list()
        subs = fetch_inst_subs("data/n8data/subscriptions_n8_coreplus.csv")
        for x in subs:
            subs[x] += core
    else:
        subs = fetch_inst_subs("data/n8data/subscriptions_n8.csv")
    
    group_jusp_data = OrderedDict()
    if parsed_args.coreplus:
        group_jusp_data["lan"] = {"institution_id": "institution-4QK9FfFudHii", "subrs": subs['lan']} #lancaster
        group_jusp_data["liv"] = {"institution_id": "institution-D9TtsdbRs6du", "subrs": subs['liv']} #liverpool
        group_jusp_data["man"] = {"institution_id": "institution-3BGTjKioPdiZ", "subrs": subs['man']} #manchester
        group_jusp_data["yor"] = {"institution_id": "institution-3pQc7HbKgqYD", "subrs": subs['yor']} #york
    else:
        group_jusp_data["lan"] = {"institution_id": "institution-4QK9FfFudHii", "orig_scenario_id": "5QiKNg5m"} #lancaster
        group_jusp_data["liv"] = {"institution_id": "institution-D9TtsdbRs6du", "orig_scenario_id": "eAj75CHL"} #liverpool
        group_jusp_data["man"] = {"institution_id": "institution-3BGTjKioPdiZ", "orig_scenario_id": "scenario-ijURRDqV"} #manchester
        group_jusp_data["yor"] = {"institution_id": "institution-3pQc7HbKgqYD", "orig_scenario_id": "hPWVrTDf"} #york
    
    group_jusp_data["ncl"] = {"institution_id": "institution-rNFhQD3woQaT", "subrs": subs['ncl']} #newcastle
    group_jusp_data["dur"] = {"institution_id": "institution-PpAFKbC7p7e5", "subrs": subs['dur']} #durham
    group_jusp_data["lee"] = {"institution_id": "institution-iW8e2beWGxsi", "subrs": subs['lee']} #leeds
    group_jusp_data["she"] = {"institution_id": "institution-3bpqX3Wzkd7e", "subrs": subs['she']} #sheffield
    group_jusp_data["cam"] = {"institution_id": "institution-N68ARB5Hr4AM", "subrs": subs['cam']} #cambridge
    group_jusp_data["ucl"] = {"institution_id": "institution-jiscucl", "subrs": subs['ucl']} #university college london, not an unsub subscriber
    group_jusp_data["icl"] = {"institution_id": "institution-jiscicl", "subrs": subs['icl']} #imperial college
    
    if parsed_args.coreplus: # Oxford & King's College London
        group_jusp_data["oxf"] = {"institution_id": "institution-jiscoxf", "subrs": subs['oxf']}
        group_jusp_data["kcl"] = {"institution_id": "institution-jisckcl", "subrs": subs['kcl']}
    else:
        group_jusp_data["oxf"] = {"institution_id": "institution-jiscoxf"}
        group_jusp_data["kcl"] = {"institution_id": "institution-jisckcl"}
    
    group_jusp_data["abd"] = {"institution_id": "institution-cH6ZGAAtwkyy", "subrs": subs['abd']} #Aberdeen
    group_jusp_data["ews"] = {"institution_id": "institution-jiscews", "subrs": subs['ews']} #St Andrews
    group_jusp_data["edi"] = {"institution_id": "institution-jiscedi", "subrs": subs['edi']} #Edinburgh
    if not parsed_args.coreplus:
        group_jusp_data["sti"] = {"institution_id": "institution-jiscsti", "subrs": subs['sti']} #Stirling
    group_jusp_data["gla"] = {"institution_id": "institution-jiscgla", "subrs": subs['gla']} #Glasgow


    groups = {}
    groups["n8"] = """lan	liv	man	yor	ncl	dur	lee	she	cam	ucl oxf icl""".split()
    if parsed_args.coreplus:
        groups["n8"] += ['kcl']
    groups["scurl"] = ['abd', 'ews', 'gla', 'edi']
    if not parsed_args.coreplus:
        groups["scurl"] += ['sti']
    groups["n8+scurl"] = groups["n8"] + groups["scurl"]

    institution_id = "institution-Tfi2z4svqqkU"

    if parsed_args.recalculate:
        for group_name, group_jusp_id_list in list(groups.items()):
            pass

            for jusp_id in group_jusp_id_list:
                print((jusp_id, group_name, group_jusp_id_list))
            
                if parsed_args.createpkgs:
                    package_create(jusp_id, institution_id, "own pta", parsed_args.coreplus)
                    # pta copied over in package_create from own jisc package
                
                    package_create(jusp_id, institution_id, get_group_pta_name(group_name), parsed_args.coreplus)
                
                    update_group_pta(jusp_id, group_jusp_id_list, get_group_pta_name(group_name), parsed_args.coreplus)

            for jusp_id in group_jusp_id_list:
                print(("setting subscriptions for ", jusp_id, group_name))
                data = group_jusp_data[jusp_id]
                if "orig_scenario_id" in data:
                    print(("getting subscriptions from unsub scenario for ", jusp_id, group_name))
                    (updated, my_source_scenario_dict) = get_latest_scenario_raw(data["orig_scenario_id"])
                    subscriptions = my_source_scenario_dict["subrs"]
                    copy_subscriptions(jusp_id, subscriptions, "own pta", parsed_args.coreplus)
                elif "subrs" in data:
                    print(("getting subscriptions from python file for ", jusp_id, group_name))
                    subscriptions = get_issnls(data.get("subrs", None))
                    # print jusp_id, len(subscriptions), len(data["subrs"])
                    copy_subscriptions(jusp_id, subscriptions, "own pta", parsed_args.coreplus)
                else:
                    print(("calculating best subscriptions for ", jusp_id, group_name))
                    set_lowest_cpu_subscriptions(jusp_id, "own pta", parsed_args.coreplus, 0.5)

            if not parsed_args.coreplus:
                print(("calculating best subscriptions for ", "liv", "own pta"))
                set_lowest_cpu_subscriptions("liv", "own pta", parsed_args.coreplus, 0.425)  # set to make instant be 60%

            # don't let it cache
            db.session.expire_all()

            for jusp_id in group_jusp_id_list:
                print(("setting group subscriptions for ", jusp_id, group_name))
                data = group_jusp_data[jusp_id]
                set_non_own_subscriptions(jusp_id, group_jusp_id_list, get_group_pta_name(group_name), parsed_args.coreplus)

    # don't let it cache
    db.session.expire_all()


    print("gathering results")

    # just pint out "n8+ now
    group_name = "n8+scurl"
    group_jusp_id_list = groups[group_name]

    universities = {}
    results = []
    results_ill = []
    for jusp_id in group_jusp_id_list:
        uni = N8UniResult(jusp_id, get_group_pta_name(group_name), parsed_args.coreplus)
        universities[jusp_id] = uni
        tmp_res = uni.to_list()
        ill = tmp_res.pop() # pop() fetches the last list element & removes it
        results_ill.append(ill)
        results.append(tmp_res)

    # do ill calculations; add result back to results list
    print("calculating ill requests for sister universities ...\n")
    ill_sum_by_issnl = pd.concat(results_ill).groupby('issn_l').sum()
    ill_sum_by_issnl.reset_index(inplace=True) # make issn_l a column
    each_univ = dict.fromkeys(group_jusp_id_list, 0)
    for index, row in ill_sum_by_issnl.iterrows():
        issn = row['issn_l']
        # print("issn: {}".format(issn))
        univ_subscribing = []
        for jusp_id in group_jusp_id_list:
            journals = universities[jusp_id].saved_scenario_ownpta.journals
            issns = [x.issn_l for x in journals]
            if issn in issns:
                univ_subscribing.append(journals[issns.index(issn)].subscribed)
            else:
                univ_subscribing.append(False)
        num_univ_subscribing = sum(univ_subscribing)
        # print("    univ's subscribing: {}".format(num_univ_subscribing))
        if num_univ_subscribing > 0:
            num_ill_requests_split = row['downloads_ill'] / num_univ_subscribing
            univ_to_assign_to = list(compress(group_jusp_id_list, univ_subscribing))
            # print("    univ's: {}".format((*univ_to_assign_to,)))
            for univ in univ_to_assign_to:
                each_univ[univ] += num_ill_requests_split

    # combine ill requests for sister universities into output list
    [x.append(round(each_univ[x[0]])) for x in results]

    for result_number in range(0, len(results[0])):
        print(";".join([str(results[column_number][result_number]) for column_number in range(0, len(results))]))

    print("\n\n\n")
