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
import package
from package_input import PackageInput
from counter import CounterInput
from perpetual_access import PerpetualAccessInput
from journal_price import JournalPriceInput


def parse_uploads():

    while True:
        upload_preprocess_bucket = "unsub-file-uploads-preprocess"
        upload_finished_bucket = "unsub-file-uploads"
        preprocess_file_list = s3_client.list_objects(Bucket=upload_preprocess_bucket)
        for preprocess_file in preprocess_file_list.get("Contents", []):
            filename = preprocess_file["Key"]
            filename_base = filename.split(".")[0]
            package_id, filetype = filename_base.split("_")
            print u"loading {} {}".format(package_id, filetype)
            size = preprocess_file["Size"]
            age_seconds = (datetime.datetime.utcnow() - preprocess_file["LastModified"].replace(tzinfo=None)).total_seconds()

            s3_clientobj = s3_client.get_object(Bucket="unsub-file-uploads-preprocess", Key=filename)
            contents_string = s3_clientobj["Body"].read()
            with open(filename, "wb") as temp_file:
                temp_file.write(contents_string)

            if filetype.startswith("counter"):
                loader = CounterInput()
            elif filetype.startswith("perpetual-access"):
                loader = PerpetualAccessInput
            elif filetype.startswith("price"):
                loader = JournalPriceInput

            load_result = loader.load(package_id, filename, commit=True)
            # print load_result

            print u"moving file {}".format(filename)
            s3_resource = boto3.resource("s3")
            copy_source = {"Bucket": upload_preprocess_bucket, "Key": filename}
            s3_resource.meta.client.copy(copy_source, upload_finished_bucket, filename)
            s3_resource.Object(upload_preprocess_bucket, filename).delete()
            print "moved"

        sleep( 2 * random.random())
        print ".",


        # try this https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
        # with header as offset
        # pass "names"
        # use usecols
        # use dtype
        # use engine=c
        # use skiprows

        # import chardet
        #
        # import pandas as pd
        #
        # with open(r'C:\Users\indreshb\Downloads\Pokemon.csv', 'rb') as f:
        #
        # result = chardet.detect(f.read()) # or readline if the file is large
        #
        # df=pd.read_csv(r'C:\Users\indreshb\Downloads\Pokemon.csv',encoding=result['encoding'])

        # move the file
        # write it into the file uploads table
        # counter input and counter




# python consortium_calculate.py
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff :)")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    parse_uploads()



