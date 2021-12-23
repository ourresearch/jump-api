import argparse
import textwrap
import json
import os
import re
from collections import OrderedDict
from enum import Enum
from psycopg2 import sql
from psycopg2.extras import execute_values

from app import get_db_cursor
from app import s3_client
from data.jiscdata.exclusions import exclusions

class CounterPublishers(Enum):
    sage = "SAGE"
    wiley = "Wiley"
    tf = "TnF"
    springer = "Springer"

def fetch_file_names(publisher, exclude = None):
    paginator = s3_client.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket='unsub-jisc')
    results = []
    for page in page_iterator:
        results.append(page['Contents'])
    results_flat = [item for sublist in results for item in sublist]
    tmp_files = list(filter(lambda x: re.match(r'.*_{}'.format(CounterPublishers[publisher].value), x['Key']), results_flat))
    files = [w['Key'] for w in tmp_files]
    if exclude:
        files = list(filter(lambda x: re.split("_", x)[0] not in exclude, files))
    return files

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
            Examples of use
            ---------------

            Notes:
                - heroku local:run required to make sure environment variables are loaded

            # Show this help
            heroku local:run python import_counter_from_json.py -h

            heroku local:run python import_counter_from_json.py --publisher sage
            heroku local:run python import_counter_from_json.py --publisher tf
            heroku local:run python import_counter_from_json.py --publisher wiley
            heroku local:run python import_counter_from_json.py --publisher springer
            '''))
    parser.add_argument("--publisher", help="A publisher name, all lowercase (one of: springer, wiley, sage, tf)", type=str)
    parsed_args = parser.parse_args()

    jcinput_cols = ["issn", "total", "package_id", "report_year", "report_name", "report_version", "metric_type", "yop", "access_type", ]

    filenames = fetch_file_names(parsed_args.publisher, exclusions[parsed_args.publisher])
    print("number of files: {}".format(len(filenames)))

    package_ids = []

    print("starting jump_counter_input insert")
    for filename in filenames:
        input_tuple_list = []
        input_dict = {}

        print(filename)

        s3_clientobj = s3_client.get_object(Bucket="unsub-jisc", Key=filename)
        contents_string = s3_clientobj["Body"].read().decode("utf-8")

        contents_json = json.loads(contents_string)

        report_type = contents_json["Report_Header"]["Report_ID"]
        institution_name = contents_json["Report_Header"]["Institution_Name"]

        report_items = contents_json.get("Report_Items", [])
        print((report_type, institution_name, len(report_items)))

        input_dict["package_id"] = "package-jisc{}{}".format(parsed_args.publisher, filename[0:3])
        package_ids += [input_dict["package_id"]]
        input_dict["report_year"] = 2020
        input_dict["report_version"] = "5"
        if "tr_j2" in filename:
            input_dict["report_name"] = "trj2"
        elif "tr_j3" in filename:
            input_dict["report_name"] = "trj3"
        elif "tr_j4" in filename:
            input_dict["report_name"] = "trj4"

        for item in report_items:
            title = item["Title"]
            input_dict["access_type"] = item.get("Access_Type", None)
            input_dict["yop"] = item.get("YOP", None)
            input_dict["issn"] = None
            issn_ids = [id for id in item["Item_ID"] if "issn" in id["Type"].lower()]
            if issn_ids:
                input_dict["issn"] = issn_ids[0]["Value"]

            loop_totals = {}
            input_dict["total"] = 0
            for stat in item["Performance"]:
                for instance in stat["Instance"]:
                    metric_type = instance.get("Metric_Type")
                    if metric_type not in loop_totals:
                        loop_totals[metric_type] = 0
                    loop_totals[metric_type] += instance["Count"]
            
            if input_dict["report_name"] == "trj2":
                input_dict["metric_type"] = "No_License"
                input_dict["total"] = loop_totals["No_License"]
            else:
                if 'Unique_Item_Requests' in loop_totals:
                    input_dict["metric_type"] = 'Unique_Item_Requests'
                    input_dict["total"] = loop_totals["Unique_Item_Requests"]
                elif 'Total_Item_Requests' in loop_totals:
                    input_dict["metric_type"] = 'Total_Item_Requests'
                    input_dict["total"] = loop_totals["Total_Item_Requests"]
                elif 'Unique_Item_Investigations' in loop_totals:
                    input_dict["metric_type"] = 'Unique_Item_Investigations'
                    input_dict["total"] = loop_totals["Unique_Item_Investigations"]
                elif 'Total_Item_Investigations' in loop_totals:
                    input_dict["metric_type"] = 'Total_Item_Investigations'
                    input_dict["total"] = loop_totals["Total_Item_Investigations"]

            sorted_dict = OrderedDict([(el, input_dict[el]) for el in jcinput_cols])
            input_tuple_list += [tuple(sorted_dict.values())]

        with get_db_cursor() as cursor:
            qry = sql.SQL("INSERT INTO jump_counter_input ({}) VALUES %s").format(
                sql.SQL(', ').join(map(sql.Identifier, jcinput_cols)))
            execute_values(cursor, qry, input_tuple_list)

    print("jump_counter_input insert done")

    print("starting jump_counter insert")
    for package_id in list(set(package_ids)):
        with get_db_cursor() as cursor:
            jc = "insert into jump_counter (select * from jump_counter_view where package_id = %s);"
            cursor.execute(jc, (package_id,))
    
    print("jump_counter insert done")
