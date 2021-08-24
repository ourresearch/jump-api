# coding: utf-8

import os
import sys
import random
import datetime
from time import time
from time import sleep
import boto3

import argparse

from app import s3_client
from app import get_db_cursor
from app import db
import package
from package_input import PackageInput
from counter import CounterInput
from perpetual_access import PerpetualAccessInput
from journal_price import JournalPriceInput


def parse_uploads():

    while True:
        try:
            command = """select * from jump_raw_file_upload_object where to_delete_date is not null"""
            with get_db_cursor() as cursor:
                cursor.execute(command)
                raw_file_upload_rows_to_delete = cursor.fetchall()
            for row_to_delete in raw_file_upload_rows_to_delete:
                file = row_to_delete["file"]
                package_id = row_to_delete["package_id"]
                if file == "price":
                    JournalPriceInput().delete(package_id)
                elif file == "perpetual-access":
                    PerpetualAccessInput().delete(package_id)
                else:
                    report_name = "jr1"
                    if "-" in file:
                        report_name = file.split("-")[1]
                    CounterInput().delete(package_id, report_name=report_name)
                # the delete will also delete the raw row which will take it off this queue

        except Exception as e:
            print("Error: exception1 {} during parse_uploads".format(e))
            try:
                db.session.rollback()
            except:
                pass

        try:
            upload_preprocess_bucket = "unsub-file-uploads-preprocess"
            upload_finished_bucket = "unsub-file-uploads"
            preprocess_file_list = s3_client.list_objects(Bucket=upload_preprocess_bucket)
            for preprocess_file in preprocess_file_list.get("Contents", []):
                filename = preprocess_file["Key"]
                filename_base = filename.split(".")[0]
                try:
                    package_id, filetype = filename_base.split("_")
                except ValueError:
                    # not a valid file, skip it
                    continue

                print("loading {} {}".format(package_id, filetype))
                size = preprocess_file["Size"]
                age_seconds = (datetime.datetime.utcnow() - preprocess_file["LastModified"].replace(tzinfo=None)).total_seconds()

                s3_clientobj = s3_client.get_object(Bucket="unsub-file-uploads-preprocess", Key=filename)
                contents_string = s3_clientobj["Body"].read()
                with open(filename, "wb", encoding='utf-8') as temp_file:
                    temp_file.write(contents_string)

                loader = None
                if filetype.startswith("counter"):
                    loader = CounterInput()
                elif filetype.startswith("perpetual-access"):
                    loader = PerpetualAccessInput()
                elif filetype.startswith("price"):
                    loader = JournalPriceInput()

                if loader:
                    load_result = loader.load(package_id, filename, commit=True)

                    print("moving file {}".format(filename))
                    s3_resource = boto3.resource("s3")
                    copy_source = {"Bucket": upload_preprocess_bucket, "Key": filename}
                    s3_resource.meta.client.copy(copy_source, upload_finished_bucket, filename)
                    s3_resource.Object(upload_preprocess_bucket, filename).delete()
                    print("moved")

        except Exception as e:
            print("Error: exception2 {} during parse_uploads on file {}".format(e, filename))
            if loader and filename:
                print("because of error, deleting file {}".format(filename))
                s3_resource = boto3.resource("s3")
                s3_resource.Object(upload_preprocess_bucket, filename).delete()
                print("because of error, deleted {}".format(filename))

            try:
                db.session.rollback()
            except:
                pass

        sleep( 2 * random.random())
        # print ".",

        # move the file
        # write it into the file uploads table
        # counter input and counter




# python consortium_calculate.py
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff :)")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    parse_uploads()

    # package_id = "package-nALodSzDfzqv"
    # loader = PerpetualAccessInput()
    # filename = "/Users/hpiwowar/Downloads/test-perpetual-access.csv"

    # package_id = "package-DhFCs96d2Vnv"
    # loader = CounterInput()
    # filename = "/Users/hpiwowar/Downloads/package-DhFCs96d2Vnv_counter.xls"
    #
    # print loader.load(package_id, filename, commit=True)

    # print loader.delete(package_id)
