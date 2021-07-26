# coding: utf-8

from cached_property import cached_property
from collections import defaultdict
from time import time
from kids.cache import cache
from collections import OrderedDict

from util import format_currency
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
#         if j["cpu"] and j["usage"] and (isinstance(j["cpu"], int) or isinstance(j["cpu"], float)):
#             j["cpu_combo_by_usage"] = j["cpu"] * j["usage"] / sum_of_usage
#         elif (isinstance(j["cpu"], int) or isinstance(j["cpu"], float)):
#             j["cpu_combo_by_usage"] = j["cpu"]
#         else:
#             j["cpu_combo_by_usage"] = "-"
#     if sum_of_usage < 10:
#         j["cpu_combo_by_usage"] = "-"
#
#     normalized_cpu = sum(j["cpu_combo_by_usage"] for j in list_this_long if j["cpu_combo_by_usage"] != "-")
#     my_journal_dict = list_this_long[0]
#     my_journal_dict["cpu"] = normalized_cpu
#     my_journal_dict["institution_names"] = [j.get("institution_name") for j in list_this_long]
#     response_list.append(my_journal_dict)


class ConsortiumJournal(Journal):
    years = range(0, 5)

    def __init__(self, issn_l, included_package_ids, all_member_data, is_jisc):
        start_time = time()
        self.issn_l = issn_l
        self.is_jisc = is_jisc
        self.included_package_ids = included_package_ids
        self.member_data = all_member_data
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

    def sum_attribute(self, attribute_name, nesting_key=None):
        response = 0
        for my_member_dict in self.member_data:
            if nesting_key:
                response += my_member_dict[nesting_key].get(attribute_name, 0) or 0
            else:
                response += my_member_dict.get(attribute_name, 0) or 0
        return float(response)

    def sum_attribute_multiplied_by_usage(self, attribute_name, nesting_key=None):
        response = 0
        for my_member_dict in self.member_data:
            if nesting_key:
                response += (my_member_dict[nesting_key].get(attribute_name, 0) or 0) * float(my_member_dict["usage"])
            else:
                response += (my_member_dict.get(attribute_name, 0) or 0) * float(my_member_dict["usage"])
        return float(response)

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
    def institution_id(self):
        return self.list_attribute("institution_id")

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
    def subject(self):
        return self.meta_data["subject"]

    @property
    def era_subjects(self):
        return self.meta_data.get("era_subjects", [])

    @cached_property
    def is_society_journal(self):
        return self.meta_data["is_society_journal"]

    @cached_property
    def bronze_oa_embargo_months(self):
        return self.meta_data["bronze_oa_embargo_months"]

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
            return self.subscription_cost
        return self.ill_cost

    @cached_property
    def use_paywalled(self):
        return self.use_total - self.use_free_instant

    # @cached_property
    # def cpu(self):
    #     if self.use_total < 10:
    #         return None
    #     cpu = 0
    #     for j in self.member_data:
    #         if j["cpu"] and j["usage"] and self.use_total:
    #             cpu += j["cpu"] * j["usage"] / self.use_total
    #     if cpu:
    #         return cpu
    #     return None


    @cached_property
    def subscription_cost(self):
        if self.is_jisc:
            if len(self.included_package_ids) >= 144:
                return self.meta_data["subscription_cost"] * 155.0
        return self.meta_data["subscription_cost"] * len(self.included_package_ids)

    @cached_property
    def ill_cost(self):
        return self.sum_attribute("ill_cost")

    @cached_property
    def cost_subscription_minus_ill(self):
        return self.subscription_cost - self.ill_cost

    @cached_property
    def use_oa_plus_social_networks(self):
        # return self.sum_attribute("oa", "use_groups_free_instant")
        return self.sum_attribute("use_oa")

    @cached_property
    def use_subscription(self):
        # return self.sum_attribute("subscription", "use_groups_if_subscribed")
        return self.sum_attribute("use_subscription")

    @cached_property
    def use_backfile(self):
        # return self.sum_attribute("backfile", "use_groups_free_instant")
        return self.sum_attribute("use_backfile")

    @cached_property
    def use_ill(self):
        # return self.sum_attribute("ill", "use_groups_if_not_subscribed")
        return self.sum_attribute("use_ill")

    @cached_property
    def use_other_delayed(self):
        # return self.sum_attribute("other_delayed", "use_groups_if_not_subscribed")
        return self.sum_attribute("use_other_delayed")

    @cached_property
    def use_social_networks(self):
        return self.sum_attribute_multiplied_by_usage("use_social_networks_percent")/100.0

    @cached_property
    def use_oa_green(self):
        return self.sum_attribute_multiplied_by_usage("use_green_percent")/100.0

    @cached_property
    def use_oa_hybrid(self):
        return self.sum_attribute_multiplied_by_usage("use_hybrid_percent")/100.0

    @cached_property
    def use_oa_bronze(self):
        return self.sum_attribute_multiplied_by_usage("use_bronze_percent")/100.0

    @cached_property
    def use_oa_peer_reviewed(self):
        return self.sum_attribute_multiplied_by_usage("use_peer_reviewed_percent")/100.0

    @cached_property
    def use_free_instant(self):
        response = self.use_oa_plus_social_networks + self.use_backfile
        return min(response, self.use_total)

    @cached_property
    def use_instant(self):
        response = self.use_free_instant + self.use_subscription
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

    @cached_property
    def cost_subscription_fuzzed(self):
        return None

    @cached_property
    def cost_subscription_minus_ill_fuzzed(self):
        return None

    @cached_property
    def cpu_fuzzed(self):
        return None

    @cached_property
    def use_total_fuzzed(self):
        return None

    @cached_property
    def downloads_fuzzed(self):
        return None

    @cached_property
    def num_authorships_fuzzed(self):
        return None

    @cached_property
    def num_citations_fuzzed(self):
        return None
