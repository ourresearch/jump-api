# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
import inspect
import threading
import requests
from time import time
from kids.cache import cache
from collections import OrderedDict

from app import use_groups
from app import use_groups_free_instant
from app import use_groups_lookup
from app import get_db_cursor
from util import format_currency
from util import format_percent
from util import format_with_commas
from util import elapsed
from journal import Journal

# start_time = time()
# journal_dicts_by_issn_l = defaultdict(list)
# for row in self.journal_member_data:
#     journal_dicts_by_issn_l[row["issn_l"]].append(row["journals_dict"])
#
# response_list = []
# for issn_l, list_this_long in journal_dicts_by_issn_l.iteritems():
#     sum_of_usage = float(sum(j["usage"] for j in list_this_long))
#     for j in list_this_long:
#         if j["ncppu"] and j["usage"] and (isinstance(j["ncppu"], int) or isinstance(j["ncppu"], float)):
#             j["ncppu_combo_by_usage"] = j["ncppu"] * j["usage"] / sum_of_usage
#         elif (isinstance(j["ncppu"], int) or isinstance(j["ncppu"], float)):
#             j["ncppu_combo_by_usage"] = j["ncppu"]
#         else:
#             j["ncppu_combo_by_usage"] = "-"
#     if sum_of_usage < 10:
#         j["ncppu_combo_by_usage"] = "-"
#
#     normalized_cpu = sum(j["ncppu_combo_by_usage"] for j in list_this_long if j["ncppu_combo_by_usage"] != "-")
#     my_journal_dict = list_this_long[0]
#     my_journal_dict["ncppu"] = normalized_cpu
#     my_journal_dict["institution_names"] = [j.get("institution_name") for j in list_this_long]
#     response_list.append(my_journal_dict)


class ConsortiumJournal(Journal):
    years = range(0, 5)

    def __init__(self, issn_l, all_member_data):
        start_time = time()
        self.issn_l = issn_l
        self.member_data = all_member_data
        # self.member_data = [d["journals_dict"] for d in all_member_data if issn_l==d["issn_l"]]
        self.meta_data = self.member_data[0]
        self.subscribed_bulk = False
        self.subscribed_custom = False
        self.use_default_download_curve = False
        # print ".",

    @cached_property
    def years_by_year(self):
        return [2019 + year_index for year_index in self.years]

    @cached_property
    def historical_years_by_year(self):
        # used for citation, authorship lookup
        return range(2015, 2019+1)

    def sum_attribute(self, attribute_name):
        response = 0
        for my_member_dict in self.member_data:
            response += my_member_dict.get(attribute_name, 0) or 0
        return response

    def list_attribute(self, attribute_name):
        return [my_member_dict.get(attribute_name, None) for my_member_dict in self.member_data]

    @cached_property
    def has_perpetual_access(self):
        response = False
        for my_member_dict in self.member_data:
            if my_member_dict.get("has_perpetual_access"):
                response = True
        return response

    @cached_property
    def perpetual_access_years(self):
        for my_member_dict in self.member_data:
            if my_member_dict.get("perpetual_access_years"):
                return my_member_dict.get("perpetual_access_years")
        return []

    @cached_property
    def baseline_access(self):
        for my_member_dict in self.member_data:
            if my_member_dict.get("baseline_access"):
                return my_member_dict.get("baseline_access")
        return None

    @cached_property
    def institution_name(self):
        return self.list_attribute("institution_name")

    @cached_property
    def institution_short_name(self):
        return self.list_attribute("institution_short_name")

    @cached_property
    def package_id(self):
        return self.list_attribute("package_id")

    @cached_property
    def title(self):
        return self.meta_data["title"]

    @cached_property
    def subject(self):
        return self.meta_data["subject"]

    @property
    def era_subjects(self):
        return self.meta_data.get("era_subjects", [])

    @cached_property
    def is_society_journal(self):
        return self.meta_data["is_society_journal"]

    @cached_property
    def oa_embargo_months(self):
        return self.meta_data["oa_embargo_months"]

    @cached_property
    def is_hybrid_2019(self):
        return self.meta_data["is_hybrid_2019"]

    @cached_property
    def num_authorships(self):
        return self.sum_attribute("authorships")

    @cached_property
    def num_citations(self):
        return self.sum_attribute("citations")

    @cached_property
    def use_total(self):
        response = self.sum_attribute("usage")
        if response == 0:
            response = 0.0001
        return response

    @cached_property
    def downloads_total(self):
        return self.sum_attribute("downloads")

    @cached_property
    def cost_actual(self):
        if self.subscribed:
            return self.cost_subscription
        return self.cost_ill

    @cached_property
    def use_paywalled(self):
        return self.use_total - self.use_free_instant

    @cached_property
    def ncppu(self):
        if self.use_total < 10:
            return None
        ncppu = 0
        for j in self.member_data:
            if j["ncppu"] and j["usage"] and self.use_total:
                ncppu += j["ncppu"] * j["usage"] / self.use_total
        if ncppu:
            return ncppu
        return None

    @cached_property
    def ncppu_rank(self):
        return None  # overwrite this by at the consortium level by ordering journals

    @cached_property
    def cost_subscription(self):
        return self.sum_attribute("cost_subscription")

    @cached_property
    def cost_ill(self):
        return self.sum_attribute("cost_ill")

    @cached_property
    def cost_subscription_minus_ill(self):
        return self.cost_subscription - self.cost_ill

    @cached_property
    def use_actual(self):
        my_dict = {}
        for group in use_groups:
            my_dict[group] = 0
        # include the if to skip this if no useage
        if self.use_total:
            # true regardless
            for group in use_groups_free_instant + ["total", "oa_plus_social_networks", "oa", "social_networks"]:
                my_dict[group] = self.__getattribute__("use_{}".format(group))
            # depends
            if self.subscribed:
                my_dict["subscription"] = self.use_subscription
            else:
                my_dict["ill"] = self.use_ill
                my_dict["other_delayed"] = self.use_other_delayed
            my_dict["oa_no_social_networks"] = my_dict["oa"]
        return my_dict


    @cached_property
    def use_social_networks(self):
        return self.sum_attribute("use_asns")

    @cached_property
    def use_oa(self):
        return self.sum_attribute("use_oa")

    @cached_property
    def use_subscription(self):
        return self.sum_attribute("use_subscription")

    @cached_property
    def use_backfile(self):
        return self.sum_attribute("use_backfile")

    @cached_property
    def use_ill(self):
        return self.sum_attribute("use_ill")

    @cached_property
    def use_other_delayed(self):
        return self.sum_attribute("use_other_delayed")

    @cached_property
    def use_oa_green(self):
        return self.sum_attribute("use_green")

    @cached_property
    def use_oa_hybrid(self):
        return self.sum_attribute("use_hybrid")

    @cached_property
    def use_oa_bronze(self):
        return self.sum_attribute("use_bronze")

    @cached_property
    def use_oa_peer_reviewed(self):
        return self.sum_attribute("use_peer_reviewed")

    @cached_property
    def use_free_instant(self):
        response = 0
        for group in use_groups_free_instant:
            response += self.use_actual[group]
        return min(response, self.use_total)

    @cached_property
    def use_instant(self):
        response = self.use_free_instant + self.use_actual["subscription"]
        return min(response, self.use_total)

    @cached_property
    def use_instant_percent(self):
        if not self.use_total:
            return 0
        return min(100.0, round(100 * float(self.use_instant) / self.use_total, 4))

    @cached_property
    def use_free_instant_percent(self):
        if not self.use_total:
            return 0
        return min(100.0, round(100 * float(self.use_free_instant) / self.use_total, 4))

    @cached_property
    def num_papers_slope_percent(self):
        # need to figure out how to do this well here @todo
        return None


    def to_dict_details(self):
        response = OrderedDict()

        response["top"] = {
                "issn_l": self.issn_l,
                "title": self.title,
                "subject": self.subject,
                # "publisher": self.publisher,
                # "is_society_journal": self.is_society_journal,
                "subscribed": self.subscribed,
                "subscribed_bulk": self.subscribed_bulk,
                "subscribed_custom": self.subscribed_custom,
                # "num_papers": self.num_papers,
                "cost_subscription": format_currency(self.cost_subscription),
                "cost_ill": format_currency(self.cost_ill),
                "cost_actual": format_currency(self.cost_actual),
                "cost_subscription_minus_ill": format_currency(self.cost_subscription_minus_ill),
                "ncppu": format_currency(self.ncppu, True),
                "use_instant_percent": self.use_instant_percent,
        }
        return response





# sum_of_usage = float(sum(j["usage"] for j in list_this_long))
# for j in list_this_long:
#     if j["ncppu"] and j["usage"] and (isinstance(j["ncppu"], int) or isinstance(j["ncppu"], float)):
#         j["ncppu_combo_by_usage"] = j["ncppu"] * j["usage"] / sum_of_usage
#     elif (isinstance(j["ncppu"], int) or isinstance(j["ncppu"], float)):
#         j["ncppu_combo_by_usage"] = j["ncppu"]
#     else:
#         j["ncppu_combo_by_usage"] = "-"
# if sum_of_usage < 10:
#     j["ncppu_combo_by_usage"] = "-"
#
# # institution_names = u", ".join([j.get("institution_short_name", j["institution_name"]) for j in list_this_long])
# institution_names_string = u", ".join([j.get("institution_name") for j in list_this_long])
# package_ids = u", ".join([j["package_id"] for j in list_this_long])
# normalized_cpu = sum(j["ncppu_combo_by_usage"] for j in list_this_long if j["ncppu_combo_by_usage"] != "-")
# my_journal_dict = sorted_journal_dicts[0]
# my_journal_dict["ncppu"] = normalized_cpu
# my_journal_dict["usage"] = round(sum_of_usage)
# my_journal_dict["cost"] = sum(j["cost"] for j in list_this_long if j["ncppu_combo_by_usage"] != "-")
# my_journal_dict["cost_subscription"] = sum(j["cost_subscription"] for j in list_this_long if j["ncppu_combo_by_usage"] != "-")
# my_journal_dict["cost_ill"] = sum(j["cost_ill"] for j in list_this_long if j["ncppu_combo_by_usage"] != "-")
# my_journal_dict["institution_names"] = [j.get("institution_name") for j in list_this_long]
# # my_journal_dict["title"] += u" [#1-{}: {}]".format(len(list_this_long), institution_names)
# # my_journal_dict["issn_l"] += u"-[{}]".format(package_ids)
# response_list.append(my_journal_dict)
