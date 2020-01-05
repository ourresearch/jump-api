# coding: utf-8
import unittest

import requests_cache
from ddt import ddt, data
from pprint import pprint
from collections import defaultdict
from nose.tools import assert_equals
from nose.tools import assert_is_not_none
from nose.tools import assert_not_equals
from nose.tools import assert_true

import numpy as np
from numpy.testing import assert_almost_equal
from numpy.testing import assert_allclose

from views import get_saved_scenario
from app import use_groups

requests_cache.install_cache('jump_api_requests_cache', expire_after=60*60*24*7)  # expire_after is in seconds


# run default open and closed like this:
# nosetests --processes=50 --process-timeout=600 test/
# mynosy


my_data = ["boo"]

class MyTest(unittest.TestCase):
    # _multiprocess_can_split_ = True

    def setUp(self):
        self.scenario_id = "demo-debug"
        self.my_saved_scenario = get_saved_scenario(self.scenario_id, debug_mode=True)
        self.live_scenario = self.my_saved_scenario.live_scenario
        self.slider_dict = self.my_saved_scenario.live_scenario.to_dict_slider()
        self.table_dict = self.my_saved_scenario.live_scenario.to_dict_table()
        self.table_headers = self.table_dict["headers"]
        self.slider_journals = self.slider_dict["journals"]

    def test_nonzero(self):
        for header in self.table_headers:
            if header["value"] not in ["ncppu_rank", "use_subscription_percent"]:
                print header["value"]
                assert_not_equals(header["raw"], None)
                assert_not_equals(header["raw"], 0)
                if header["percent"]:
                    assert_true(header["percent"] <= 100)


    def test_total_sums(self):
        counts = defaultdict(int)

        for my_journal in self.slider_journals:
            calculated_total = 0
            calculated_total += my_journal["use_groups_if_subscribed"]["subscription"]
            calculated_total += my_journal["use_groups_free_instant"]["oa"]
            calculated_total += my_journal["use_groups_free_instant"]["backfile"]
            calculated_total += my_journal["use_groups_free_instant"]["social_networks"]

            compare_total = my_journal["use_total"]
            # pprint(my_journal)

            try:
                assert_allclose(calculated_total,
                                compare_total,
                                atol=2,
                                # err_msg=err_msg,
                                verbose=True)
                counts["total_pass"] += 1
            except AssertionError as e:
                pprint([my_journal["title"], my_journal["issn_l"], round(calculated_total, 0), round(compare_total), e])
                counts["total_fail"] += 1
                # raise

        pprint(counts)
        if len(counts.keys()) != 1:
            print 1/0

    #
    # def test_this_one(self):
    #     import requests
    #     use_instant = 0
    #     use_total = 0
    #     use_ill_total = 0
    #     use_oa_total = 0
    #     use_backfile_total = 0
    #     use_social_networks_total = 0
    #     url = "http://localhost:5004/live/scenario/demo-package-dtCDaJRoBA/slider?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiI4MTg4MWE3NC01ZjAyLTRkNDQtOTc2My0xNDNjNDhkNjFkMjYiLCJuYmYiOjE1NzgwODM0MzcsImlkZW50aXR5Ijp7ImFjY291bnRfaWQiOiJkZW1vIiwiY3JlYXRlZCI6IjIwMjAtMDEtMDNUMjA6MzA6MzcuNTgwMTEyIiwibG9naW5fdXVpZCI6ImR0Q0RhSlJvQkEiLCJpc19kZW1vX2FjY291bnQiOnRydWV9LCJ0eXBlIjoiYWNjZXNzIiwiaWF0IjoxNTc4MDgzNDM3LCJmcmVzaCI6ZmFsc2V9.MNFFBWHxRO_he2gcljodz3EBHUx3rQmXbXI98-MxQaY"
    #     r = requests.get(url)
    #     data = r.json()
    #     for my_journal in data["journals"]:
    #         use_instant += my_journal["use_instant"]
    #         use_total += my_journal["use_total"]
    #         use_ill_total += my_journal["use_groups_if_not_subscribed"]["ill"]
    #         use_oa_total += my_journal["use_groups_free_instant"]["oa"]
    #         use_backfile_total += my_journal["use_groups_free_instant"]["backfile"]
    #         use_social_networks_total += my_journal["use_groups_free_instant"]["social_networks"]
    #     print use_instant, use_total
    #     print float(use_instant) / use_total
    #     print use_ill_total
    #     print "use_ill_total", float(use_ill_total) / use_total
    #     print "use_oa_total", float(use_oa_total) / use_total
    #     print "use_backfile_total", float(use_backfile_total) / use_total
    #     print "use_social_networks_total", float(use_social_networks_total) / use_total
    #     print 1/0

    def test_instant_sums(self):
        counts = defaultdict(int)
        for my_journal in self.slider_journals:

            calculated_total = 0
            calculated_total += my_journal["use_groups_free_instant"]["oa"]
            calculated_total += my_journal["use_groups_free_instant"]["backfile"]
            calculated_total += my_journal["use_groups_free_instant"]["social_networks"]
            calculated_total = round(calculated_total, 0)

            compare_total = round(my_journal["use_instant"], 0)

            try:
                assert_allclose(calculated_total,
                                compare_total,
                                atol=2,
                                # err_msg=err_msg,
                                verbose=True)
                counts["instant_pass"] += 1
            except AssertionError as e:
                pprint([my_journal["title"], calculated_total, compare_total, e])
                counts["instant_fail"] += 1
                # raise

            err_msg = None

        pprint(counts)
        if len(counts.keys()) != 1:
            print 1/0


    def test_subscription_sums(self):
        counts = defaultdict(int)
        for my_journal in self.slider_journals:

            calculated_total = 0
            calculated_total += my_journal["use_groups_if_not_subscribed"]["ill"]
            calculated_total += my_journal["use_groups_if_not_subscribed"]["other_delayed"]
            calculated_total = round(calculated_total, 0)

            compare_total = round(my_journal["use_groups_if_subscribed"]["subscription"], 0)

            try:
                assert_allclose(calculated_total,
                                compare_total,
                                atol=2,
                                # err_msg=err_msg,
                                verbose=True)
                counts["subscription_pass"] += 1
            except AssertionError as e:
                # pprint([my_journal["title"], calculated_total, compare_total, e])
                counts["subscription_fail"] += 1
                raise

            err_msg = None

        pprint(counts)
        if len(counts.keys()) != 1:
            print 1/0


    # def test_use_sums_by_year(self):
    #     counts = defaultdict(int)
    #     for my_journal in self.live_scenario.journals:
    #
    #         for group in use_groups:
    #             if group in []: # ("backfile", "subscription"):
    #                 pass
    #             else:
    #                 calculated_total = np.mean(my_journal.__getattribute__("use_{}_by_year".format(group)))
    #                 compare_total = my_journal.__getattribute__("use_{}".format(group))
    #                 # print my_journal, group, round(compare_total - calculated_total), round(compare_total), round(calculated_total), my_journal.__getattribute__("use_{}_by_year".format(group))
    #
    #                 try:
    #                     assert_allclose(calculated_total,
    #                                     compare_total,
    #                                     atol=1000,
    #                                     # err_msg=err_msg,
    #                                     verbose=True)
    #                     counts["use_sum_{}_pass".format(group)] += 1
    #                 except AssertionError as e:
    #                     print my_journal, group, round(compare_total - calculated_total), round(compare_total), round(calculated_total), my_journal.__getattribute__("use_{}_by_year".format(group))
    #                     counts["use_sum_{}_fail".format(group)] += 1
    #                     # pprint(counts)
    #                     # raise
    #
    #         err_msg = None
    #
    #     pprint(counts)
    #     print 1/0
    #     if len(counts.keys()) != len(use_groups):
    #         print 1/0



