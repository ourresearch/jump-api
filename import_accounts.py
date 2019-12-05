# coding: utf-8

import openpyxl
import unicodecsv as csv
import argparse
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import text
import datetime
import re
from dateutil import parser
from werkzeug.security import generate_password_hash
import shortuuid
import glob
import pycounter
import codecs

from app import db
from app import get_db_cursor
from account import Account
from package import Package
from account_grid_id import AccountGridId
from saved_scenario import SavedScenario
from util import read_csv_file
from util import safe_commit
from util import is_issn


def convert_from_utf16_to_utf8(filename):
    # https://stackoverflow.com/a/10300007/596939
    BLOCKSIZE = 1048576 # or some other, desired size in bytes
    with codecs.open(filename, "r", "utf-16") as sourceFile:
        new_filename = u"{}_utf8".format(filename)
        with codecs.open(new_filename, "w", "utf-8") as targetFile:
            while True:
                contents = sourceFile.read(BLOCKSIZE)
                if not contents:
                    break
                targetFile.write(contents)
    return new_filename

def convert_date_spaces_to_dashes(filename):
    # https://stackoverflow.com/a/10300007/596939
    BLOCKSIZE = 1048576 # or some other, desired size in bytes
    with codecs.open(filename, "r", "utf-8") as sourceFile:
        new_filename = u"{}_dashes".format(filename)
        with codecs.open(new_filename, "w", "utf-8") as targetFile:
            while True:
                contents = sourceFile.read(BLOCKSIZE)
                if not contents:
                    break
                contents = contents.replace(" 2018,", "-2018,")
                targetFile.write(contents)
    return new_filename

def build_counter_import_file(filename):

    rows = read_csv_file(filename)
    # todo just one for now
    reports = {}
    lines = []
    for row in rows:
        if row["add_now"] == "1":
            print "row:", row["username"], row["display_name"], row["grid_id"], row["add_now"]
            my_files = glob.glob("/Users/hpiwowar/Downloads/{}.*".format(row["username"]))
            if my_files:
                counter_file = my_files[0]
                print counter_file
                try:
                    report = pycounter.report.parse(counter_file)
                    # print report
                    reports[row["username"]] = report

                # except ValueError:
                #     convert_date_spaces_to_dashes(counter_file)
                #     return import_counter(filename)
                #
                # except UnicodeDecodeError:
                #     convert_from_utf16_to_utf8(counter_file)
                #     return import_counter(filename)

                except AttributeError:
                    report = None
                    print "error on", my_files
                    reports[row["username"]] = "error"

                if report:
                    my_account = Account.query.filter(Account.username == row["username"]).first()
                    package_id = my_account.packages[0].package_id
                    for journal in report:
                        export_dict = {}
                        export_dict["organization"] = row["username"]
                        export_dict["package_id"] = package_id
                        export_dict["publisher"] = journal.publisher
                        export_dict["issn"] = journal.issn
                        export_dict["total"] = journal.html_total + journal.pdf_total
                        lines.append(export_dict)

    print reports
    for username, report in reports.iteritems():
        first_journal = list(report)[0]
        publisher = first_journal.publisher
        report.write_tsv(u"/Users/hpiwowar/Downloads/{}_{}_2018_clean.tsv".format(username, publisher))

    # print lines
    with open("/Users/hpiwowar/Downloads/counter_import.csv", "w") as export_file:
        csv_writer = csv.writer(export_file, encoding='utf-8')
        keys = ["organization", "publisher", "package_id", "issn", "total"]
        csv_writer.writerow(keys)
        for line in lines:
            csv_writer.writerow([line[k] for k in keys])


def import_consortium_counter_xls(xls_filename):
    results = []
    xlsx_file = open(xls_filename, "rb")
    workbook = openpyxl.load_workbook(xlsx_file, read_only=True)
    sheetnames = list(workbook.sheetnames)

    for sheetname in sheetnames:
        university = sheetname.replace("JR1 ", "")
        print university

        sheet = workbook[sheetname]

        column_names = {}
        for i, column in enumerate(list(sheet.iter_rows(min_row=1, max_row=1))[0]):
            column_names[column.value] = i

        for row_cells in sheet.iter_rows(min_row=1):
            issn = row_cells[column_names['Print ISSN']].value
            total = row_cells[column_names['Reporting period total']].value
            if is_issn(issn):
                results.append({
                    "university": university,
                    "issn": issn,
                    "total": total
                })
                # print ".",
                # print university, issn, total

        with open("data/countercleaned.csv", "w") as csv_file:
            csv_writer = csv.writer(csv_file, encoding='utf-8')
            header = ["university", "issn", "total"]
            csv_writer.writerow(header)
            for my_dict in results:
                # doing this hacky thing so excel doesn't format the issn as a date :(
                csv_writer.writerow([my_dict[k] for k in header])


def create_accounts(filename):
    rows = read_csv_file(filename)
    # todo just one for now
    for row in rows:
        print "row:", row["username"], row["display_name"], row["grid_id"], row["add_now"]

        if not row["add_now"] == "1":
            print u"skipping {}, add_row != 1".format(row["username"])
        elif  Account.query.filter(Account.username==row["username"]).first():
            print u"skipping {}, already in db".format(row["username"])
        else:
            new_account = Account()
            new_account.username = row["username"]
            new_account.password_hash = generate_password_hash(row["username"])
            new_account.display_name = row["display_name"]
            new_account.is_consortium = False

            new_account_grid_object = AccountGridId()
            new_account_grid_object.grid_id = row["grid_id"]

            new_package = Package()
            new_package.package_id = shortuuid.uuid()[0:8]
            new_package.publisher = "Elsevier"
            new_package.package_name = u"{} Elsevier".format(row["display_name"])

            scenario_id = shortuuid.uuid()[0:8]
            new_saved_scenario = SavedScenario(False, scenario_id, None)
            new_saved_scenario.scenario_name = u"First Scenario".format(new_package.package_name)

            new_package.saved_scenarios = [new_saved_scenario]
            new_account.packages = [new_package]
            new_account.grid_id_objects = [new_account_grid_object]

            db.session.add(new_account)
            safe_commit(db)

            print u"created {}, package_id={}".format(new_account.username, new_account.packages[0].package_id)

    def warm_the_cache():
        import os
        import sys
        import logging
        import requests
        import time
        sys.path.insert(0, '../jump-api')
        mpl_logger = logging.getLogger("matplotlib")
        mpl_logger.setLevel(logging.WARNING)
        import views
        from package import Package
        from util import elapsed

        packages = Package.query.all()
        for package in packages:
            print u"\nstart: {} {}".format(package.package_id, package)
            start_time = time.time()
            url = "https://cdn.unpaywalljournals.org/data/common/{}?secret={}".format(
                package.package_id, os.getenv("JWT_SECRET_KEY"))
            headers = {"Cache-Control": "public, max-age=31536000"}
            r = requests.get(url, headers=headers)
            print u"1st: {} {} {}".format(package.package_id, r.status_code, elapsed(start_time))

            start_time = time.time()
            r = requests.get(url, headers=headers)
            print u"2nd: {} {} {}".format(package.package_id, r.status_code, elapsed(start_time))



# python import_accounts.py ~/Downloads/new_accounts.csv
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")
    parser.add_argument('filename', type=str, help="input file to parse")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)


    create_accounts(parsed_vars["filename"])
    build_counter_import_file(parsed_vars["filename"])


    # then import it into jump_counter_input
    # then
    # create table jump_counter_newest distkey (package_id) interleaved sortkey (package_id, issn_l) as (select * from jump_counter_view);
    # select * from jump_counter_newest order by random() limit 1000;
    # alter table jump_counter rename to jump_counter_old;
    # alter table jump_counter_newest rename to jump_counter;
    # drop table jump_counter_old;
    #
    # drop table jump_apc_authorships_new;
    # create table jump_apc_authorships_new distkey (package_id) interleaved sortkey (package_id, doi, issn_l) as (select * from jump_apc_authorships_view);
    # select * from jump_apc_authorships_new order by random() limit 1000;
    # alter table jump_apc_authorships rename to jump_apc_authorships_old;
    # alter table jump_apc_authorships_new rename to jump_apc_authorships;
    # drop table jump_apc_authorships_old;
    #
    # create table jump_citing_new distkey(issn_l) interleaved sortkey (citing_org, grid_id, year, issn_l) as (select * from jump_citing_view where grid_id in (select grid_id from jump_account_grid_id))
    # select * from jump_citing_new order by random() limit 1000;
    # alter table jump_citing rename to jump_citing_old;
    # alter table jump_citing_new rename to jump_citing;
    # drop table jump_citing_old;



