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

consortium_institution_id = "institution-3tLYzP8JuYUf"
publisher = "Elsevier"

# consortium_display_name = "Elsevier (Full)"
# consortium_short_name = "viva_elsevier_full"
# member_package_name = "Elsevier (Full)"

consortium_display_name = "Elsevier (Freedom)"
consortium_short_name = "viva_elsevier_freedom"
member_package_name = "Elsevier (Freedom)"

institution_ids = """
        institution-79mQVmZAnGhj
        institution-mDphqabkcBRz
        institution-AcGRzdBPpVuP
        institution-uRR3tLL7xAaz
        institution-xFFDfqtaBXik
        institution-vh9p98yHbuoG
        institution-ehji2ZJBrdhc
        """.split()
institution_ids_string = u",".join(["'{}'".format(institution_id) for institution_id in institution_ids])


def copy_package(old_package_id, new_package_id):
    command = """
        insert into jump_counter (issn_l, package_id, organization, publisher, issn, journal_name, total) (
            select issn_l, '{new_package_id}', organization, publisher, issn, journal_name, total
            from jump_counter
            where package_id = '{old_package_id}'
        );
        
        insert into jump_counter_input (organization, publisher, issn, journal_name, total, package_id) (
            select organization, publisher, issn, journal_name, total, '{new_package_id}'
            from jump_counter_input
            where package_id = '{old_package_id}'
        );
        
        insert into jump_perpetual_access (package_id, issn_l, start_date, end_date) (
            select '{new_package_id}', issn_l, start_date, end_date
            from jump_perpetual_access
            where package_id = '{old_package_id}'
        );
        
        insert into jump_perpetual_access_input (package_id, issn, start_date, end_date) (
            select '{new_package_id}', issn, start_date, end_date
            from jump_perpetual_access_input
            where package_id = '{old_package_id}'
        );
        
        insert into jump_journal_prices (package_id, publisher, title, issn_l, usa_usd) (
            select '{new_package_id}', publisher, title, issn_l, usa_usd
            from jump_journal_prices
            where package_id = '{old_package_id}'
        );
        
        insert into jump_journal_prices_input (package_id, publisher, issn, usa_usd) (
            select '{new_package_id}', publisher, issn, usa_usd
            from jump_journal_prices_input
            where package_id = '{old_package_id}'
        );

        
        insert into jump_apc_authorships (
            package_id, doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc) (
            select '{new_package_id}', doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc
            from jump_apc_authorships
            where package_id = '{old_package_id}'
        );

        insert into jump_account_package (package_id, publisher, package_name, created, consortium_package_id, institution_id, is_demo, big_deal_cost, is_deleted, updated, default_to_no_perpetual_access) (
            select '{new_package_id}', publisher, package_name, sysdate, consortium_package_id, institution_id, is_demo, big_deal_cost, is_deleted, updated, default_to_no_perpetual_access
            from jump_account_package
            where package_id = '{old_package_id}'
        );
    """.format(new_package_id=new_package_id, old_package_id=old_package_id)
    print command
    with get_db_cursor() as cursor:
        cursor.execute(command)


def consortium_create():

    # create a package for this
    consortium_package_id = u'package-{}'.format(shortuuid.uuid()[0:12])
    my_package = Package(
        package_id=consortium_package_id,
        publisher=publisher,
        package_name=consortium_display_name,
        created=datetime.datetime.utcnow().isoformat(),
        institution_id=consortium_institution_id,
        is_demo=False
    )
    db.session.add(my_package)
    db.session.flush()

    my_scenario_id = u'scenario-{}'.format(shortuuid.uuid()[0:12])
    my_scenario_name = u'First Scenario'
    my_scenario = SavedScenario(False, my_scenario_id, None)
    my_scenario.package_id = my_package.package_id
    my_scenario.scenario_name = my_scenario_name
    my_scenario.created = datetime.datetime.utcnow().isoformat()
    my_scenario.is_base_scenario = True

    db.session.add(my_scenario)
    safe_commit(db)
    print u"made consortium package {} and scenario {}".format(my_package, my_scenario)

    dict_to_save = my_scenario.to_dict_saved()
    dict_to_save["name"] = my_scenario_name
    save_raw_scenario_to_db(my_scenario_id, dict_to_save, None)


    command = """select * from jump_account_package
        where institution_id in ({institution_ids_string})
        and package_name='{member_package_name}' and not is_deleted
        """.format(member_package_name=member_package_name, institution_ids_string=institution_ids_string)

    print command
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()


    for row in rows:
        old_package_id = row["package_id"]
        new_package_id = old_package_id.replace("-", "-cmp")
        print old_package_id
        print new_package_id

        command = """
            insert into jump_consortium_members (consortium_short_name, consortium_package_id, member_package_id)
            values ('{}', '{}', '{}')
            """.format(consortium_short_name, consortium_package_id, new_package_id)
        print command
        with get_db_cursor() as cursor:
            cursor.execute(command)
        print

        print u"copying package {} to {}".format(old_package_id, new_package_id)
        copy_package(old_package_id, new_package_id)
        print u"done copying package {} to {}".format(old_package_id, new_package_id)
        print

    # now save first scenario with all member institutions set
    command = """
        select member_package_id from jump_consortium_members where consortium_package_id = '{}'
        """.format(consortium_package_id)
    print command
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    all_member_ids = [row["member_package_id"] for row in rows]

    print all_member_ids
    save_raw_member_institutions_included_to_db(my_scenario_id, all_member_ids, None)

    # now kick off the computing
    new_consortia = Consortium(my_scenario_id)
    new_consortia.recompute_journal_dicts()



# python consortium_calculate.py
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff :)")
    parser.add_argument("--package_id", type=str, default=None, help="package id of consortium to recompute")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    consortium_package_id = parsed_vars["package_id"]


    # consortium_create()

    # consortium_package_id = "package-2NNrG6YCAvAh"
    # consortium_package_id = "package-3WkCDEZTqo6S"

    # copy_package("package-UKqnJcsns7QL", "package-cmpUKqnJcsns7QL")
    # command = """
    #     insert into jump_consortium_members (consortium_short_name, consortium_package_id, member_package_id)
    #     values ('{}', '{}', '{}')
    #     """.format("colorado_alliance", consortium_package_id, "package-cmpUKqnJcsns7QL")
    # print command
    # with get_db_cursor() as cursor:
    #     cursor.execute(command)

    from consortium import get_consortium_ids
    consortium_ids = get_consortium_ids()
    for d in consortium_ids:
        # print d["package_id"]
        if consortium_package_id == d["package_id"]:
            print "starting to recompute row {}".format(d)
            new_consortia = Consortium(d["scenario_id"])
            new_consortia.recompute_journal_dicts()
            print u"recomputing {}".format(new_consortia)
