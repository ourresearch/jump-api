# coding: utf-8

import openpyxl
import unicodecsv as csv
import argparse
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import text
import datetime
import re
from dateutil import parser
from werkzeug.security import generate_password_hash, check_password_hash
import shortuuid
import glob
from lib.pycounter import report as reporter
from lib.pycounter.exceptions import UnknownReportTypeError
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
                report = None

                counter_file = my_files[0]
                print counter_file
                try:
                    report = reporter.parse(counter_file)
                    # print report
                    reports[row["username"]] = report

                # except ValueError:
                #     convert_date_spaces_to_dashes(counter_file)
                #     return import_counter(filename)
                #
                # except UnicodeDecodeError:
                #     convert_from_utf16_to_utf8(counter_file)
                #     return import_counter(filename)

                except UnknownReportTypeError:
                    class MockJournal(object):
                        pass

                    # counter_file = convert_from_utf16_to_utf8(counter_file)
                    counter_rows = read_csv_file(counter_file)
                    report = []
                    for counter_row in counter_rows:
                        my_journal = MockJournal()
                        my_journal.issn = counter_row["ISSN"]
                        my_journal.html_total = counter_row["Total"]
                        my_journal.pdf_total = 0
                        my_journal.publisher = None
                        report += [my_journal]
                    reports[row["username"]] = report

                # except AttributeError:
                #     report = None
                #     print "error on", my_files
                #     reports[row["username"]] = "error"

                if report:
                    my_account = Account.query.filter(Account.username == row["username"]).first()
                    package_id = my_account.packages[0].package_id
                    for journal in report:
                        export_dict = {}
                        export_dict["organization"] = row["username"]
                        export_dict["package_id"] = package_id
                        export_dict["publisher"] = journal.publisher
                        export_dict["issn"] = journal.issn
                        try:
                            if report.report_type == "TR_J1":
                                export_dict["total"] = journal.total_usage
                            else:
                                export_dict["total"] = journal.html_total + journal.pdf_total
                        except AttributeError:
                            export_dict["total"] = journal.html_total

                        lines.append(export_dict)

    print reports
    for username, report in reports.iteritems():
        first_journal = list(report)[0]
        publisher = first_journal.publisher
        try:
            report.write_tsv(u"/Users/hpiwowar/Downloads/{}_{}_2018_clean.tsv".format(username, publisher))
        except AttributeError:
            pass

    # print lines
    with open("/Users/hpiwowar/Downloads/counter_import.csv", "wb") as export_file:
        csv_writer = csv.writer(export_file, encoding='utf-8')
        keys = ["organization", "publisher", "package_id", "issn", "total"]
        csv_writer.writerow(keys)
        for line in lines:
            csv_writer.writerow([line[k] for k in keys])

    print "/Users/hpiwowar/Downloads/counter_import.csv"


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


def import_perpetual_access_files():
    results = []
    my_files = glob.glob("/Users/hpiwowar/Downloads/SUNY-PTA-files/2*.xlsx")
    my_files.reverse()
    for my_file in my_files:
        print my_file
        xlsx_file = open(my_file, "rb")
        workbook = openpyxl.load_workbook(xlsx_file, read_only=True)
        sheetnames = list(workbook.sheetnames)

        for sheetname in sheetnames:
            sheet = workbook[sheetname]

            column_names = {}
            for i, column in enumerate(list(sheet.iter_rows(min_row=1, max_row=1))[0]):
                column_names[column.value] = i

            for row_cells in sheet.iter_rows(min_row=1):
                university = row_cells[column_names['Account Name']].value
                issn = row_cells[column_names['ISSN (FS split)']].value
                start_date = row_cells[column_names['Content Start Date']].value
                end_date = row_cells[column_names['Content End Date']].value
                if is_issn(issn):
                    new_dict = {
                        "university": university,
                        "issn": issn,
                        "start_date": start_date,
                        "end_date": end_date
                    }
                    results.append(new_dict)
                    # print new_dict
                    print ".",

    with open("/Users/hpiwowar/Downloads/perpetual_access_cleaned.csv", "w") as csv_file:
        csv_writer = csv.writer(csv_file, encoding='utf-8')
        header = ["university", "issn", "start_date", "end_date"]
        csv_writer.writerow(header)
        for my_dict in results:
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
            new_account.password_hash = generate_password_hash(u"{}123".format(row["username"]))
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


def check_passwords():
    accounts = Account.query.all()
    for my_account in accounts:
        # hashed_username_password = generate_password_hash(u"{}".format(my_account.username))
        if check_password_hash(my_account.password_hash, my_account.username):
            if not my_account.consortium_id and my_account.username not in ("cern", "msu", "windsor", "demo", "suny"):
                print u"{} has NOT changed password.  {}".format(my_account, my_account.created.isoformat())
                new_hashed_username_password = generate_password_hash(u"{}123".format(my_account.username))
                my_account.password_hash = new_hashed_username_password
        else:
            pass
            # print ".",
            # print u"{} has changed password".format(my_account)

    safe_commit(db)
    print "committed"


# python import_accounts.py ~/Downloads/new_accounts.csv
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")
    parser.add_argument('filename', type=str, help="input file to parse")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)


    # check_passwords()
    # import_perpetual_access_files()

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



