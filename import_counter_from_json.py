import json
import os

from app import get_db_cursor
from app import s3_client

# filenames = [key["Key"] for key in s3_client.list_objects(Bucket="unsub-jisc")["Contents"]]
# filenames.reverse()

filenames = ["smu_SD_tr_j2_2020-01_2020-12.json",
             "smu_SD_tr_j3_2020-01_2020-12.json",
             "smu_SD_tr_j4_2020-01_2020-12.json"]

print(filenames)
print(len(filenames))

for filename in filenames:
    input_string_list = []
    input_dict = {}

    print(filename)

    s3_clientobj = s3_client.get_object(Bucket="unsub-jisc", Key=filename)
    contents_string = s3_clientobj["Body"].read().decode("utf-8")

    contents_json = json.loads(contents_string)

    report_type = contents_json["Report_Header"]["Report_ID"]
    institution_name = contents_json["Report_Header"]["Institution_Name"]

    report_items = contents_json.get("Report_Items", [])
    print(report_type, institution_name, len(report_items))

    input_dict["package_id"] = "package-solojiscels{}".format(filename[0:3])
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

        input_dict["total"] = 0
        for stat in item["Performance"]:
            for instance in stat["Instance"]:
                input_dict["metric_type"] = instance.get("Metric_Type")
                if input_dict["metric_type"] in ["Unique_Item_Requests", "No_License"]:
                    input_dict["total"] += instance["Count"]

        # print title, issn, total_count, access_type, yop
        input_string = """ (
                '{issn}',
                {total},
                '{package_id}',
                {report_year},
                '{report_name}',
                '{report_version}',
                '{metric_type}',
                {yop},
                '{access_type}' ) """.format(**input_dict)  # must be in same order as "insert into jump_counter_input" below
        input_string_list += [input_string]

    input_strings = ",".join(input_string_list)
    input_strings = input_strings.replace("'None'", "null")
    input_strings = input_strings.replace("None", "null")

    with get_db_cursor() as cursor:
        print("starting db insert")
        command = """
                insert into jump_counter_input (issn, total, package_id, report_year, report_name, report_version, metric_type, yop, access_type) 
                values {}
            """.format(input_strings)
        # print command
        cursor.execute(command)
        print("db insert done")

with get_db_cursor() as cursor:
    command = """delete from jump_counter where package_id ilike 'package-solojiscels%'"""
    cursor.execute(command)
    command = """insert into jump_counter (select * from jump_counter_view where package_id ilike 'package-solojiscels%')""".format(input_dict["package_id"])
    cursor.execute(command)





