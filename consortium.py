# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
from collections import OrderedDict
import datetime
import shortuuid
from multiprocessing.pool import ThreadPool
import threading
import requests
from flask_jwt_extended import create_access_token, get_jwt_identity
import os

from app import db
from app import get_db_cursor
from consortium_journal import ConsortiumJournal

class Consortium(object):
    def __init__(self, package_id, my_jwt):
        self.package_id = package_id
        self.jwt = my_jwt

        command = """select package_id, scenario_id 
            from jump_account_combo_view 
            where consortium_package_id='{}' order by package_id asc""".format(self.package_id)
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()

        print rows
        self.org_ids = rows

        if not hasattr(threading.current_thread(), "_children"):
            threading.current_thread()._children = weakref.WeakKeyDictionary()

        my_thread_pool = ThreadPool(10)

        def call_cached_version(org_id_dict):

            # url = u"http://localhost:5004/scenario/{}/raw?secret={}".format(org_id_dict["scenario_id"], os.getenv("JWT_SECRET_KEY"))
            url = u"https://cdn.unpaywalljournals.org/scenario/{}/raw?secret={}".format(org_id_dict["scenario_id"], os.getenv("JWT_SECRET_KEY"))
            # url = u"https://cdn.unpaywalljournals.org/scenario/{}/raw?jwt={}".format(org_id_dict["scenario_id"], self.jwt)

            # print u"starting cache request for {}".format(url)
            headers = {"Cache-Control": "public, max-age=31536000",
                "Cache-Tag": "common, common_{}".format(package_id)}
            r = requests.get(url, headers=headers)
            # print
            # print r.headers
            if r.status_code == 200:
                data = r.json()
                data["scenario_id"] = org_id_dict["scenario_id"]
                data["package_id"] = org_id_dict["package_id"]
                data["status_code"] = r.status_code
                data["url"] = url
                print u"success in call_cached_version with {}".format(url)
            else:
                data = {}
                data["scenario_id"] = org_id_dict["scenario_id"]
                data["package_id"] = org_id_dict["package_id"]
                data["status_code"] = r.status_code
                data["url"] = url
                print u"not success in call_cached_version with {}".format(url)
            return data

        self.consortium_org_responses = my_thread_pool.imap_unordered(call_cached_version, self.org_ids)
        my_thread_pool.close()
        my_thread_pool.join()
        my_thread_pool.terminate()

        self.journal_org_data = defaultdict(list)
        self.journal_meta = {}
        for org_response in self.consortium_org_responses:
            if org_response["status_code"] == 200:
                for journal_dict in org_response["journals"]:
                    if journal_dict and "table_row" in journal_dict:
                        journal_dict["table_row"]["org_package_id"] = org_response["package_id"]
                        journal_dict["table_row"]["org_scenario_id"] = org_response["scenario_id"]
                        self.journal_org_data[journal_dict["meta"]["issn_l"]].append(journal_dict["table_row"])
                        self.journal_meta[journal_dict["meta"]["issn_l"]] = journal_dict["meta"]
            else:
                print "failed org response", org_response

        self.issn_ls = self.journal_meta.keys()

    @property
    def journals(self):
        return [ConsortiumJournal(issn_l, self.journal_meta[issn_l], self.journal_org_data[issn_l]) for issn_l in self.issn_ls]

    def __repr__(self):
        return u"<{} ({})>".format(self.__class__.__name__, self.package_id)


