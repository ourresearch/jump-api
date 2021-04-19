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

consortium_institution_id = "institution-KSwxw3KCLaY6"
publisher = "Elsevier"
consortium_display_name = "Elsevier, average"
consortium_short_name = "julac_average_elsevier"
member_package_name = "Elsevier, average"

institution_ids = """
        """.split()

ror_ids = """
        """.split()
ror_ids_string = u",".join(["'{}'".format(ror_id) for ror_id in ror_ids])

member_package_ids = """
package-2gFXGTxiHdg3y
package-2HTzA9zSa584o
package-23rZv64VdYgbP
package-24wsbdzMo8bVd
package-2jVu8pgprYq2f
package-2s4LyTpNcXbRW
package-2D9eQK57aKVET
package-2Ye75MtF7KDom
""".split()
member_package_id_string = u",".join(["'{}'".format(package_id) for package_id in member_package_ids])

def copy_package(old_package_id, new_package_id):
    command = """
        insert into jump_counter (issn_l, package_id, journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type) (
            select issn_l, '{new_package_id}', journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type
            from jump_counter
            where package_id = '{old_package_id}'
        );
        
        insert into jump_counter_input (issn, journal_name, total, package_id, report_year, report_name, report_version, metric_type, yop, access_type) (
            select issn, journal_name, total, '{new_package_id}', report_year, report_name, report_version, metric_type, yop, access_type
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
        
        insert into jump_journal_prices (package_id, publisher, title, issn_l, price) (
            select '{new_package_id}', publisher, title, issn_l, price
            from jump_journal_prices
            where package_id = '{old_package_id}'
        );
        
        insert into jump_journal_prices_input (package_id, publisher, issn, price) (
            select '{new_package_id}', publisher, issn, price
            from jump_journal_prices_input
            where package_id = '{old_package_id}'
        );

        
        insert into jump_apc_authorships (
            package_id, doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc) (
            select '{new_package_id}', doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc
            from jump_apc_authorships
            where package_id = '{old_package_id}'
        );

        insert into jump_account_package (package_id, publisher, package_name, created, consortium_package_id, institution_id, is_demo, big_deal_cost, is_deleted, updated, default_to_no_perpetual_access, currency) (
            select '{new_package_id}', publisher, package_name, sysdate, consortium_package_id, institution_id, is_demo, big_deal_cost, is_deleted, updated, default_to_no_perpetual_access, currency
            from jump_account_package
            where package_id = '{old_package_id}'
        );
    """.format(new_package_id=new_package_id, old_package_id=old_package_id)
    print command
    with get_db_cursor() as cursor:
        cursor.execute(command)


def copy_institution(old_institution_id, new_institution_id):


    print "need to make this jump_institutions thing work"
    print "and also copy into jump_grid_id"
    print 1/0
    command = """
        insert into jump_institution (something) (
            select  '{new_institution_id}', something
            from jump_institution
            where package_id = '{old_institution_id}'
        );

    """.format(old_institution_id=old_institution_id, new_institution_id=new_institution_id)
    print command
    with get_db_cursor() as cursor:
        cursor.execute(command)

    command = """select package_id from jump_account_package
        where institution_id = '{old_institution_id}'
        and publisher='Elsevier' and not is_deleted
        """.format(old_institution_id=old_institution_id)

    print command
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()

    old_package_ids = [row["package_id"] for row in rows]

    for old_package_id in old_package_ids:
        new_package_id = u'package-{}'.format(shortuuid.uuid()[0:12])
        copy_package(old_package_id, new_package_id)


def consortium_create():

    # consortium_package_id = "package-3WkCDEZTqo6S"
    # my_scenario_id = "scenario-QC2kbHfUhj9W"

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

    dict_to_save = my_scenario.to_dict_saved_from_db()
    dict_to_save["name"] = my_scenario_name
    save_raw_scenario_to_db(my_scenario_id, dict_to_save, None)

    #
    # global institution_ids
    #
    # if not institution_ids:
    #     print "ror_ids_string", ror_ids_string
    #     command = """select * from jump_ror_id
    #         where ror_id in ({ror_ids_string})
    #         """.format(ror_ids_string=ror_ids_string)
    #     with get_db_cursor() as cursor:
    #         cursor.execute(command)
    #         rows = cursor.fetchall()
    #         institution_ids = [row["institution_id"] for row in rows]
    #
    #
    # for row in rows:
    #     row["package_id"] = u'package-{}'.format(shortuuid.uuid()[0:12])
    #
    # command = """select * from jump_account_package
    #     where institution_id in ({institution_ids_string})
    #     and package_name='{member_package_name}' and not is_deleted
    #     """.format(member_package_name=member_package_name, institution_ids_string=institution_ids_string)
    #
    # command = """select * from jump_account_package
    #     where package_id in ({member_package_id_string}) and not is_deleted
    #     """.format(member_package_id_string=member_package_id_string)
    #
    # print command
    # with get_db_cursor() as cursor:
    #     cursor.execute(command)
    #     rows = cursor.fetchall()
    #
    # for row in rows:
    #     old_package_id = row["package_id"]
    #     new_package_id = old_package_id
    #     # new_package_id = old_package_id.replace("-", "-2")
    #     print old_package_id
    #     print new_package_id
    #
    #     command = """
    #         insert into jump_consortium_members (consortium_short_name, consortium_package_id, member_package_id)
    #         values ('{}', '{}', '{}')
    #         """.format(consortium_short_name, consortium_package_id, new_package_id)
    #     print command
    #     with get_db_cursor() as cursor:
    #         cursor.execute(command)
    #     print
    #
    #     print u"copying package {} to {}".format(old_package_id, new_package_id)
    #     copy_package(old_package_id, new_package_id)
    #     print u"done copying package {} to {}".format(old_package_id, new_package_id)
    #     print


    return (consortium_package_id, my_scenario_id)



# python consortium_calculate.py
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff :)")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)


    # for old_institution_id in """
    #     institution-SMh3xurt2V5C
    #     institution-8phSSAkfiXbm
    #     institution-V9SQXczdPneA
    #     institution-jscQRozbejja
    #     institution-YJTPGtaJgntF
    #     institution-gFnkmtWE5Z7S
    #     institution-xr8pxYvWqJvT
    #     institution-MKviQpUBbHEi
    #     institution-577QzSroJYWJ
    #     institution-i7guvoLsnBVM
    #     institution-jsNxxfWm5zcM
    #     institution-gWwB9iSCivMt
    #     institution-2KoLu8fgJtbi
    #     institution-ZLZYsK5AQyqC""".split():
    #
    #     new_institution_id = u'package-demo{}'.format(shortuuid.uuid()[0:12])
    #     copy_institution(old_institution_id, new_institution_id)


    if False:
        (consortium_package_id, my_scenario_id) = consortium_create()
    else:
        consortium_package_id = "package-Py8q4qxq4F34"
        my_scenario_id = "scenario-F3awPQcDiG7C"

    if False:
        # if have a list of member_package_ids include this
        for old_package_id in member_package_ids:
            new_package_id = old_package_id.replace("-", "-avg")
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

    if False:
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

    from consortium import get_consortium_ids
    consortium_ids = get_consortium_ids()
    for d in consortium_ids:
        print d["package_id"]
        if consortium_package_id==d["package_id"]:
            print u"start recomputing {}".format(d["scenario_id"])
            new_consortia = Consortium(d["scenario_id"])
            new_consortia.recompute_journal_dicts()
            print u"done recomputing {}".format(d["scenario_id"])


# heroku run --size=performance-l python consortium_create.py
