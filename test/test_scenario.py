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
# requests_cache.clear()


# run default open and closed like this:
# nosetests --processes=50 --process-timeout=600 test/
# mynosy
# mynosy test/test_scenario.py:MyTest.test_backfile


my_data = ["boo"]

def get_journal(my_list, issn_l):
    return next((item for item in my_list if item['issn_l'] == issn_l), None)

# more tests
# - when you change config params they change as they should
# - when you subscribe/unsubscribe it changes as it should

class MyTest(unittest.TestCase):
    # _multiprocess_can_split_ = True

    def setUp(self):
        self.scenario_id = "demo-debug"
        self.my_saved_scenario = get_saved_scenario(self.scenario_id, test_mode=True)
        self.live_scenario = self.my_saved_scenario.live_scenario
        self.slider_dict = self.my_saved_scenario.live_scenario.to_dict_slider()
        self.table_dict = self.my_saved_scenario.live_scenario.to_dict_table()
        self.table_headers = self.table_dict["headers"]
        self.slider_journals = self.slider_dict["journals"]

    def test_stable_summaries(self):
        assert_allclose(self.slider_dict["_summary"]["use_free_instant_percent"], 57.2, atol=0.2)
        assert_allclose(self.slider_dict["_summary"]["cost_percent"], 11.4, atol=0.2)

    def test_curve_fit(self):
        my_journal_objects = self.live_scenario.journals
        num_with_default_download_curves = len([1 for j in my_journal_objects if j.use_default_download_curve])
        num_with_non_default_download_curves = len([1 for j in my_journal_objects if not j.use_default_download_curve])

        assert_true(num_with_default_download_curves < 200) # 189
        assert_true(num_with_non_default_download_curves > 1600) # 1666

        from app import USE_PAPER_GROWTH
        if USE_PAPER_GROWTH:
            num_with_default_num_papers_curves = len([1 for j in my_journal_objects if j.use_default_num_papers_curve])
            num_with_non_default_num_papers_curves = len([1 for j in my_journal_objects if not j.use_default_num_papers_curve])
            assert_true(num_with_default_num_papers_curves < 200)  #169
            assert_true(num_with_non_default_num_papers_curves > 1600)

            # num_with_all_free_access = len([1 for j in my_journal_objects if j.use_instant_percent > .99])
            # assert_true(num_with_all_free_access < 1)

    def test_embargo(self):
        from app import USE_PAPER_GROWTH
        if USE_PAPER_GROWTH:
            my_journal_objects = self.live_scenario.journals
            for my_journal in my_journal_objects:
                if my_journal.oa_embargo_months:
                    for year in range(0, 5):
                        if year*12 > my_journal.oa_embargo_months:
                            for group in ["backfile", "social_networks", "ill", "other_delayed"]:
                                if my_journal.use_actual_by_year[group][year] != 0:
                                    print "here, is not zero", group, year, my_journal.issn_l, my_journal.use_actual_by_year[group][year]
                                assert_true(my_journal.use_actual_by_year[group][year] == 0)
                            assert_true(my_journal.use_actual_by_year["oa"][year] == my_journal.use_actual_by_year["total"][year])


    def test_non_embargo(self):
        from app import USE_PAPER_GROWTH
        if USE_PAPER_GROWTH:
            my_journal_objects = self.live_scenario.journals
            for my_journal in my_journal_objects:
                if not my_journal.oa_embargo_months:
                    for year in range(0, 5):
                        if my_journal.use_actual_by_year["backfile"][year] == 0:
                            print "here, is zero", year, my_journal.issn_l, my_journal.use_actual_by_year["backfile"][year]
                        assert_true(my_journal.use_actual_by_year["backfile"][year] != 0)


    def test_nonzero(self):
        number_that_have_zeros = 0
        for header in self.table_headers:
            if header["value"] not in ["ncppu_rank", "use_subscription_percent"]:
                print header["value"]
                try:
                    assert_not_equals(header["raw"], None)
                    assert_not_equals(header["raw"], 0)
                    if header["percent"]:
                        assert_true(header["percent"] <= 100)
                except AssertionError:
                    number_that_have_zeros += 1
        assert_true(number_that_have_zeros < 100)

    def test_counter_input(self):
        import glob
        import re
        from util import get_sql_answer
        from util import get_sql_answers
        from app import db

        bad_usernames = set()

        q = """select username from jump_account_combo_view where consortium_package_id is null and not is_consortium"""
        usernames = get_sql_answers(db, q)
        usernames = [u for u in usernames if u not in ["demo", "mit", "suny connect", "uva"]]
        for username in usernames:
            try:
                my_file = "/Users/hpiwowar/Dropbox/ti/unpaywall-journals-data/counter-clean/{}_Elsevier_2018_clean.tsv".format(username)
                lines = open(my_file).readlines()
                delimiter = "\t"
            except:
                my_file = "/Users/hpiwowar/Dropbox/ti/unpaywall-journals-data/counter-clean/{}_Elsevier_2018_clean.csv".format(username)
                lines = open(my_file).readlines()
                delimiter = ","

            for issn_l in ['0140-6736', '0747-5632', '0960-9822', '0020-7489', '0360-1315']:
                matching_lines = [line for line in lines if issn_l in line]
                for matching_line in matching_lines:
                    matching_line = matching_line.strip()
                    columns = matching_line.split(delimiter)
                    int_columns = [int(num) for num in columns if re.match("^\d+$", num)]
                    my_max = None
                    if int_columns:
                        my_max = max(int_columns)

                    q = """select total from jump_account_combo_view join jump_counter on jump_account_combo_view.package_id = jump_counter.package_id where issn_l='{}' and username='{}'""".format(issn_l, username)
                    total = int(get_sql_answer(db, q))

                    if my_max == total:
                        pass
                        # print "match", issn_l, username, my_max, total
                    else:
                        print "nope", issn_l, username, my_max, total
                        bad_usernames.add(username)

        print "bad_usernames", bad_usernames
        assert_true(len(bad_usernames) == 0)


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


    def test_backfile(self):
        # cell = get_journal(self.live_scenario.journals, "0092-8674")
        # "1474-4422", "0092-8674", "0260-6917
        my_journal = next((item for item in self.live_scenario.journals if item.issn_l == "1474-4422"), None)
        # pprint(cell_backfile)
        # pprint(cell.downloads_by_age)
        # pprint(cell.growth_scaling_downloads)
        # pprint(cell.downloads_oa_by_age)
        # pprint(cell.growth_scaling_oa_downloads)

        if True:
            by_age = my_journal.downloads_by_age
            by_age_old = my_journal.downloads_total_older_than_five_years/5.0
        else:
            by_age = my_journal.downloads_oa_by_age[0]
            # pprint(my_journal.downloads_oa_by_age)
            by_age_old = (my_journal.downloads_total_older_than_five_years/5.0) * (my_journal.downloads_oa_by_age[0][4]/(my_journal.downloads_by_age[4]+1))


        response = [[0 for j in range(0, 18)] for i in range(0, 8)]
        for year in range(0, 5):
            for age in range (0, 11):
                if age <= 4:
                    response[year][9 + year - age] = int(by_age[age])
            for i in range(0, 5):
                response[year][9 + year - i - 5] = int(by_age_old)

        print "downloads"
        my_matrix = my_journal.downloads_obs_pub
        pprint(my_journal.display_obs_pub_matrix(my_matrix))
        pprint(my_journal.sum_obs_pub_matrix_by_obs(my_matrix))

        print "oa"
        my_matrix = my_journal.oa_obs_pub
        pprint(my_journal.display_obs_pub_matrix(my_matrix))
        pprint(my_journal.sum_obs_pub_matrix_by_obs(my_matrix))

        print "backfile raw"
        my_matrix = my_journal.backfile_raw_obs_pub
        pprint(my_journal.display_obs_pub_matrix(my_matrix))
        pprint(my_journal.sum_obs_pub_matrix_by_obs(my_matrix))

        print "backfile real"
        my_matrix = my_journal.backfile_obs_pub
        pprint(my_journal.display_obs_pub_matrix(my_matrix))
        pprint(my_journal.sum_obs_pub_matrix_by_obs(my_matrix))

        # for obs_key, my_row in my_matrix.iteritems():
        #     print obs_key, "\t".join([str(my_col) for my_col in my_row.values()])

        print "\n\n"
        for year in range(0, 17):
            response[6][year] = "pub{}".format(int(2011 + year))

        for year in range(0, 17):
            total = 0
            for row_num in range(0, 5):
                total += int(response[row_num][year])
            response[7][year] = total

        for row in range(0, 5):
            response[row][15] = "obs{}".format(int(2020 + row))

        for row_num in range(0, 5):
            total = 0
            for col in range(0, 15):
                total += int(response[row_num][col])
            response[row_num][16] = total

        for row_num in range(0, 5):
            total = 0
            for col in range(0, 9):
                total += int(response[row_num][col])
            response[row_num][17] = total

        # pprint(response)
        for row in response:
            print "\t".join([str(col) for col in row])


        # assert_true(my_journal = "")


