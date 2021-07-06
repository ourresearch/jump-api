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

publisher = "Elsevier"
consortium_short_name = "consortiumtest"
member_package_name = "Elsevier"

institution_ids = """
        """.split()

ror_ids = """
        """.split()
ror_ids_string = u",".join(["'{}'".format(ror_id) for ror_id in ror_ids])

# member_package_ids = """
# package-2gFXGTxiHdg3y
# package-2HTzA9zSa584o
# package-23rZv64VdYgbP
# package-24wsbdzMo8bVd
# package-2jVu8pgprYq2f
# package-2s4LyTpNcXbRW
# package-2D9eQK57aKVET
# package-2Ye75MtF7KDom
# """.split()
# member_package_id_string = u",".join(["'{}'".format(package_id) for package_id in member_package_ids])

def copy_package(old_package_id, new_package_id, new_institution_id):
    command = """
        insert into jump_counter (issn_l, package_id, journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type, created) (
            select issn_l, '{new_package_id}', journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type, created
            from jump_counter
            where package_id = '{old_package_id}'
        );
        
        insert into jump_counter_input (issn, journal_name, total, package_id, report_year, report_name, report_version, metric_type, yop, access_type) (
            select issn, journal_name, total, '{new_package_id}', report_year, report_name, report_version, metric_type, yop, access_type
            from jump_counter_input
            where package_id = '{old_package_id}'
        );
        
        insert into jump_perpetual_access (package_id, issn_l, start_date, end_date, created) (
            select '{new_package_id}', issn_l, start_date, end_date, created
            from jump_perpetual_access
            where package_id = '{old_package_id}'
        );
        
        insert into jump_perpetual_access_input (package_id, issn, start_date, end_date) (
            select '{new_package_id}', issn, start_date, end_date
            from jump_perpetual_access_input
            where package_id = '{old_package_id}'
        );
        
        insert into jump_journal_prices (package_id, publisher, title, issn_l, price, created) (
            select '{new_package_id}', publisher, title, issn_l, price, created
            from jump_journal_prices
            where package_id = '{old_package_id}'
        );
        
        insert into jump_journal_prices_input (package_id, publisher, issn, price) (
            select '{new_package_id}', publisher, issn, price
            from jump_journal_prices_input
            where package_id = '{old_package_id}'
        );

        insert into jump_raw_file_upload_object (package_id, file, bucket_name, object_name, created, num_rows, error_details, error, to_delete_date) (
            select '{new_package_id}', file, bucket_name, object_name, created, num_rows, error_details, error, to_delete_date
            from jump_raw_file_upload_object
            where package_id = '{old_package_id}'
        );
        
        insert into jump_apc_authorships (
            package_id, doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc) (
            select '{new_package_id}', doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc
            from jump_apc_authorships
            where package_id = '{old_package_id}'
        );

        insert into jump_account_package (package_id, publisher, package_name, created, consortium_package_id, institution_id, is_demo, big_deal_cost, big_deal_cost_increase, is_deleted, updated, default_to_no_perpetual_access, currency) (
            select '{new_package_id}', publisher, package_name, sysdate, consortium_package_id, '{new_institution_id}', is_demo, big_deal_cost, big_deal_cost_increase, is_deleted, updated, default_to_no_perpetual_access, currency
            from jump_account_package
            where package_id = '{old_package_id}'
        );
    """.format(new_package_id=new_package_id, old_package_id=old_package_id, new_institution_id=new_institution_id)
    print command
    with get_db_cursor() as cursor:
        cursor.execute(command)


def copy_institution(old_institution_id, new_institution_id, publisher=None, old_package_ids=None):

    command = """
        insert into jump_institution  (
            select  '{new_institution_id}' as id, old_username, display_name, created, is_consortium, consortium_id, is_demo_institution
            from jump_institution
            where id = '{old_institution_id}'
        );
        insert into jump_ror_id  (
            select '{new_institution_id}' as institution_id, ror_id
            from jump_ror_id
            where institution_id = '{old_institution_id}'
        );
        insert into jump_grid_id  (
            select '{new_institution_id}' as institution_id, grid_id
            from jump_grid_id
            where institution_id = '{old_institution_id}'
        );
    """.format(old_institution_id=old_institution_id, new_institution_id=new_institution_id)
    print command
    with get_db_cursor() as cursor:
        cursor.execute(command)


    # figure out what package ids to copy over
    if publisher and not old_package_ids:
        command = """select package_id from jump_account_package
            where institution_id = '{old_institution_id}'
            and publisher='{}' and not is_deleted
            """.format(publisher=publisher, old_institution_id=old_institution_id)
        print command
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()
        old_package_ids = [row["package_id"] for row in rows]

    # copy them
    for old_package_id in old_package_ids:
        new_package_id = old_package_id.replace("package-", "package-testing")
        print new_package_id
        copy_package(old_package_id, new_package_id, new_institution_id)


def consortium_package_create(consortium_institution_id, consortium_package_display_name):

    # consortium_package_id = "package-3WkCDEZTqo6S"
    # my_scenario_id = "scenario-QC2kbHfUhj9W"

    # create a package for this
    consortium_package_id = u'package-{}'.format(shortuuid.uuid()[0:12])
    my_package = Package(
        package_id=consortium_package_id,
        publisher=publisher,
        package_name=consortium_package_display_name,
        created=datetime.datetime.utcnow().isoformat(),
        institution_id=consortium_institution_id,
        currency="USD",
        big_deal_cost=1000000,
        big_deal_cost_increase=1,
        is_demo=False
    )
    db.session.add(my_package)
    db.session.flush()

    my_scenario_id = u'scenario-{}'.format(shortuuid.uuid()[0:12])
    my_scenario_name = u'First Scenario'
    my_scenario = SavedScenario(False, my_scenario_id, None)
    my_scenario.package_id = my_package.package_id
    my_scenario.created = datetime.datetime.utcnow().isoformat()
    my_scenario.is_base_scenario = True

    db.session.add(my_scenario)
    safe_commit(db)
    print u"made consortium package {} and scenario {}".format(my_package, my_scenario)

    dict_to_save = my_scenario.to_dict_saved_from_db()
    dict_to_save["name"] = my_scenario_name
    save_raw_scenario_to_db(my_scenario_id, dict_to_save, None)

    return (consortium_package_id, my_scenario_id)


# old_package_ids =  """
#         institution-SMh3xurt2V5C
#         institution-8phSSAkfiXbm
#         institution-V9SQXczdPneA
#         institution-jscQRozbejja
#         institution-YJTPGtaJgntF
#         institution-gFnkmtWE5Z7S
#         institution-xr8pxYvWqJvT
#         institution-MKviQpUBbHEi
#         institution-577QzSroJYWJ
#         institution-i7guvoLsnBVM
#         institution-jsNxxfWm5zcM
#         institution-gWwB9iSCivMt
#         institution-2KoLu8fgJtbi
#         institution-ZLZYsK5AQyqC""".split()



# python consortium_calculate.py
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff :)")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)
    #
    # command = "select institution_id, listagg(package_id, ',') as package_ids from jump_account_package where institution_id ilike 'institution-demo%' and package_name = 'Elsevier' group by institution_id"
    # with get_db_cursor() as cursor:
    #     cursor.execute(command)
    #     old_institution_rows = cursor.fetchall()
    #
    # print "making new institutions"
    # new_institution_ids = []
    # for row in old_institution_rows:
    #     old_institution_id = row["institution_id"]
    #     old_package_ids = row["package_ids"].split(",")
    #     new_institution_id = old_institution_id.replace("institution-demo", "institution-testing")
    #     new_institution_ids.append(new_institution_id)
    #     print old_institution_id, new_institution_id, old_package_ids
    #     copy_institution(old_institution_id, new_institution_id, old_package_ids=old_package_ids)
    #
    # print new_institution_ids
    #
    # # use init_institution to set up an institution with the right name, user using --is_consortium flag like this:
    # # python init_institution.py --users --institutions --commit --is_consortium
    #
    # if True:
    #     consortium_institution_id = "institution-WzH2RdcHUPoR"
    #     consortium_package_display_name = "Elsevier"
    #
    # (consortium_package_id, consortium_scenario_id) = consortium_package_create(consortium_institution_id, consortium_package_display_name)
    # print consortium_package_id, consortium_scenario_id
    #
    if True:
        consortium_package_id = "package-X9cgZdJWfmGy"
        consortium_scenario_id = "scenario-RbUMonByfc4A"



    command = "select distinct package_id from jump_account_package where institution_id ilike 'institution-testing%'"
    with get_db_cursor() as cursor:
        cursor.execute(command)
        new_package_id_rows = cursor.fetchall()
        member_package_ids = [row["package_id"] for row in new_package_id_rows]

    print member_package_ids
    #
    # for member_package_id in member_package_ids:
    #     command = """
    #         insert into jump_consortium_members (consortium_short_name, consortium_package_id, member_package_id)
    #         values ('{}', '{}', '{}')
    #         """.format(consortium_short_name, consortium_package_id, member_package_id)
    #     print command
    #     with get_db_cursor() as cursor:
    #         cursor.execute(command)
    #     print

    # save_raw_member_institutions_included_to_db(consortium_scenario_id, member_package_ids, None)


    # now kick off the computing
    print "recomputing"
    new_consortia = Consortium(consortium_scenario_id)
    new_consortia.recompute_journal_dicts()

    print "done"


# heroku run --size=performance-l python init_consortium.py
