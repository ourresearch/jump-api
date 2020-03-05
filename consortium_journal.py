# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
import inspect
import threading
import requests
from kids.cache import cache
from collections import OrderedDict

from app import use_groups
from app import use_groups_free_instant
from app import use_groups_lookup
from app import get_db_cursor
from util import format_currency
from util import format_percent
from util import format_with_commas
from journal import Journal


class ConsortiumJournal(Journal):
    years = range(0, 5)

    def __init__(self, issn_l, meta_data, org_data):
        self.issn_l = issn_l
        self.org_data = org_data
        self.meta_data = meta_data
        self.subscribed_bulk = False
        self.subscribed_custom = False
        self.use_default_download_curve = False

    def set_scenario(self, scenario):
        if scenario:
            self.scenario = weakref.proxy(scenario)
            self.settings = self.scenario.settings
            # [j.set_scenario(scenario) for j in self.consortium_journals]
        else:
            self.scenario = None
            self.settings = None

    def set_scenario_data(self, scenario_data):
        # [j.set_scenario_data(scenario_data) for j in self.consortium_journals]
        self._scenario_data = scenario_data

    @cached_property
    def years_by_year(self):
        return [2019 + year_index for year_index in self.years]

    @cached_property
    def historical_years_by_year(self):
        # used for citation, authorship lookup
        return range(2015, 2019+1)

    def sum_attribute(self, attribute_name):
        response = 0
        for my_org_dict in self.org_data:
            response += my_org_dict.get(attribute_name, 0) or 0
        return response

    @cached_property
    def has_perpetual_access(self):
        response = False
        for my_org_dict in self.org_data:
            if my_org_dict.get("has_perpetual_access"):
                response = True
        return response

    @cached_property
    def perpetual_access_years(self):
        for my_org_dict in self.org_data:
            if my_org_dict.get("perpetual_access_years"):
                return my_org_dict.get("perpetual_access_years")
        return []

    @cached_property
    def baseline_access(self):
        for my_org_dict in self.org_data:
            if my_org_dict.get("baseline_access"):
                return my_org_dict.get("baseline_access")
        return None

    @cached_property
    def title(self):
        return self.meta_data["title"]

    @cached_property
    def subject(self):
        return self.meta_data["subject"]

    @cached_property
    def num_authorships(self):
        return self.sum_attribute("authorships")

    @cached_property
    def num_citations(self):
        return self.sum_attribute("citations")

    @cached_property
    def use_total(self):
        response = self.sum_attribute("total_usage")
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
        if not self.use_paywalled:
            return None
        return round(float(self.cost_subscription_minus_ill)/self.use_paywalled, 6)

    @cached_property
    def cost_subscription(self):
        return self.sum_attribute("subscription_cost")

    @cached_property
    def cost_ill(self):
        return self.sum_attribute("ill_cost")

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
            for group in use_groups_free_instant + ["total"]:
                my_dict[group] = self.__getattribute__("use_{}".format(group))
            # depends
            if self.subscribed:
                my_dict["subscription"] = self.use_subscription
            else:
                my_dict["ill"] = self.use_ill
                my_dict["other_delayed"] = self.use_other_delayed
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
    def ncppu_rank(self):
        if self.ncppu:
            return self.scenario.ncppu_rank_lookup[self.issn_l]
        return None

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
        #
        # group_list = []
        # for group in use_groups:
        #     group_dict = OrderedDict()
        #     group_dict["group"] = use_groups_lookup[group]["display"]
        #     group_dict["usage"] = format_with_commas(round(self.use_actual[group]))
        #     group_dict["usage_percent"] = format_percent(round(float(100)*self.use_actual[group]/self.use_total))
        #     # group_dict["timeline"] = u",".join(["{:>7}".format(self.use_actual_by_year[group][year]) for year in self.years])
        #     for year in self.years:
        #         group_dict["year_"+str(2020 + year)] = format_with_commas(round(self.use_actual_by_year[group][year]))
        #     group_list += [group_dict]
        # response["fulfillment"] = {
        #     "headers": [
        #         {"text": "Type", "value": "group"},
        #         {"text": "Usage (projected annual)", "value": "usage"},
        #         {"text": "Usage (percent)", "value": "usage_percent"},
        #         {"text": "Usage projected 2020", "value": "year_2020"},
        #         {"text": "2021", "value": "year_2021"},
        #         {"text": "2022", "value": "year_2022"},
        #         {"text": "2023", "value": "year_2023"},
        #         {"text": "2024", "value": "year_2024"},
        #     ],
        #     "data": group_list
        #     }
        # response["fulfillment"]["use_actual_by_year"] = self.use_actual_by_year
        # response["fulfillment"]["downloads_per_paper_by_age"] = self.downloads_per_paper_by_age
        # response["fulfillment"]["perpetual_access_years"] = self.perpetual_access_years
        # response["fulfillment"]["display_perpetual_access_years"] = self.display_perpetual_access_years
        #
        # oa_list = []
        # for oa_type in ["green", "hybrid", "bronze"]:
        #     oa_dict = OrderedDict()
        #     use = self.__getattribute__("use_oa_{}".format(oa_type))
        #     oa_dict["oa_status"] = oa_type.title()
        #     # oa_dict["num_papers"] = round(self.__getattribute__("num_{}_historical".format(oa_type)))
        #     oa_dict["usage"] = format_with_commas(use)
        #     oa_dict["usage_percent"] = format_percent(round(float(100)*use/self.use_total))
        #     oa_list += [oa_dict]
        # oa_list += [OrderedDict([("oa_status", "*Total*"),
        #                         # ("num_papers", round(self.num_oa_historical)),
        #                         ("usage", format_with_commas(self.use_oa)),
        #                         ("usage_percent", format_percent(round(100*float(self.use_oa)/self.use_total)))])]
        # response["oa"] = {
        #     "oa_embargo_months": self.oa_embargo_months,
        #     "headers": [
        #         {"text": "OA Type", "value": "oa_status"},
        #         # {"text": "Number of papers (annual)", "value": "num_papers"},
        #         {"text": "Usage (projected annual)", "value": "usage"},
        #         {"text": "Percent of all usage", "value": "usage_percent"},
        #     ],
        #     "data": oa_list
        #     }
        #
        # impact_list = [
        #     OrderedDict([("impact", "Downloads"),
        #                  ("raw", format_with_commas(self.downloads_total)),
        #                  ("weight", 1),
        #                  ("contribution", format_with_commas(self.downloads_total))]),
        #     OrderedDict([("impact", "Citations to papers in this journal"),
        #                  ("raw", format_with_commas(self.num_citations, 1)),
        #                  ("weight", self.settings.weight_citation),
        #                  ("contribution", format_with_commas(self.num_citations * self.settings.weight_citation))]),
        #     OrderedDict([("impact", "Authored papers in this journal"),
        #                  ("raw", format_with_commas(self.num_authorships, 1)),
        #                  ("weight", self.settings.weight_authorship),
        #                  ("contribution", format_with_commas(self.num_authorships * self.settings.weight_authorship))]),
        #     OrderedDict([("impact", "*Total*"),
        #                  ("raw", "-"),
        #                  ("weight", "-"),
        #                  ("contribution", format_with_commas(self.use_total))])
        #     ]
        # response["impact"] = {
        #     "usage_total": self.use_total,
        #     "headers": [
        #         {"text": "Impact", "value": "impact"},
        #         {"text": "Raw (projected annual)", "value": "raw"},
        #         {"text": "Weight", "value": "weight"},
        #         {"text": "Usage contribution", "value": "contribution"},
        #     ],
        #     "data": impact_list
        #     }
        #
        # cost_list = []
        # for cost_type in ["cost_actual_by_year", "cost_subscription_by_year", "cost_ill_by_year", "cost_subscription_minus_ill_by_year"]:
        #     cost_dict = OrderedDict()
        #     if cost_type == "cost_actual_by_year":
        #         cost_dict["cost_type"] = "*Your scenario cost*"
        #     else:
        #         cost_dict["cost_type"] = cost_type.replace("cost_", "").replace("_", " ").title()
        #         cost_dict["cost_type"] = cost_dict["cost_type"].replace("Ill", "ILL")
        #     costs = self.__getattribute__(cost_type)
        #     for year in self.years:
        #         cost_dict["year_"+str(2020 + year)] = format_currency(costs[year])
        #     cost_list += [cost_dict]
        #     cost_dict["cost_avg"] = format_currency(self.__getattribute__(cost_type.replace("_by_year", "")))
        #     if self.use_paywalled:
        #         cost_dict["cost_per_use"] = format_currency(self.__getattribute__(cost_type.replace("_by_year", "")) / float(self.use_paywalled), True)
        #     else:
        #         cost_dict["cost_per_use"] = "no paywalled usage"
        # response["cost"] = {
        #     "subscribed": self.subscribed,
        #     "ncppu": format_currency(self.ncppu, True),
        #     "headers": [
        #         {"text": "Cost Type", "value": "cost_type"},
        #         {"text": "Cost (projected annual)", "value": "cost_avg"},
        #         {"text": "Cost-Type per paid use", "value": "cost_per_use"},
        #         {"text": "Cost projected 2020", "value": "year_2020"},
        #         {"text": "2021", "value": "year_2021"},
        #         {"text": "2022", "value": "year_2022"},
        #         {"text": "2023", "value": "year_2023"},
        #         {"text": "2024", "value": "year_2024"},
        #     ],
        #     "data": cost_list
        #     }
        #
        # from apc_journal import ApcJournal
        # my_apc_journal = ApcJournal(self.issn_l, self._scenario_data)
        # response["apc"] = {
        #     "apc_price": my_apc_journal.apc_price_display,
        #     "annual_projected_cost": my_apc_journal.cost_apc_historical,
        #     "annual_projected_fractional_authorship": my_apc_journal.fractional_authorships_total,
        #     "annual_projected_num_papers": my_apc_journal.num_apc_papers_historical,
        # }

        # response_debug = OrderedDict()
        # response_debug["scenario_settings"] = self.settings.to_dict()
        # response_debug["use_instant_percent"] = self.use_instant_percent
        # response_debug["use_instant_percent_by_year"] = self.use_instant_percent_by_year
        # response_debug["oa_embargo_months"] = self.oa_embargo_months
        # response_debug["num_papers"] = self.num_papers
        # response_debug["use_weight_multiplier_normalized"] = self.use_weight_multiplier_normalized
        # response_debug["use_weight_multiplier"] = self.use_weight_multiplier
        # response_debug["downloads_counter_multiplier_normalized"] = self.downloads_counter_multiplier_normalized
        # response_debug["downloads_counter_multiplier"] = self.downloads_counter_multiplier
        # response_debug["use_instant_by_year"] = self.use_instant_by_year
        # response_debug["use_instant_percent_by_year"] = self.use_instant_percent_by_year
        # response_debug["use_actual_by_year"] = self.use_actual_by_year
        # response_debug["use_actual"] = self.use_actual
        # # response_debug["use_oa_green"] = self.use_oa_green
        # # response_debug["use_oa_hybrid"] = self.use_oa_hybrid
        # # response_debug["use_oa_bronze"] = self.use_oa_bronze
        # response_debug["perpetual_access_years"] = self.perpetual_access_years
        # response_debug["display_perpetual_access_years"] = self.display_perpetual_access_years
        # # response_debug["use_oa_peer_reviewed"] = self.use_oa_peer_reviewed
        # response_debug["use_oa"] = self.use_oa
        # response_debug["downloads_total_by_year"] = self.downloads_total_by_year
        # response_debug["use_default_download_curve"] = self.use_default_download_curve
        # response_debug["downloads_total_older_than_five_years"] = self.downloads_total_older_than_five_years
        # response_debug["raw_downloads_by_age"] = self.raw_downloads_by_age
        # response_debug["downloads_by_age"] = self.downloads_by_age
        # response_debug["num_papers_by_year"] = self.num_papers_by_year
        # response_debug["num_papers_growth_from_2018_by_year"] = self.num_papers_growth_from_2018_by_year
        # response_debug["raw_num_papers_historical_by_year"] = self.raw_num_papers_historical_by_year
        # response_debug["downloads_oa_by_year"] = self.downloads_oa_by_year
        # response_debug["downloads_backfile_by_year"] = self.downloads_backfile_by_year
        # response_debug["downloads_obs_pub_matrix"] = self.display_obs_pub_matrix(self.downloads_obs_pub)
        # response_debug["oa_obs_pub_matrix"] = self.display_obs_pub_matrix(self.oa_obs_pub)
        # response_debug["backfile_obs_pub_matrix"] = self.display_obs_pub_matrix(self.backfile_obs_pub)
        # response_debug["use_oa_percent_by_year"] = self.use_oa_percent_by_year
        # response_debug["ncppu"] = self.ncppu
        # response_debug["ncppu_rank"] = self.ncppu_rank
        # response_debug["old_school_cpu"] = self.old_school_cpu
        # response_debug["old_school_cpu_rank"] = self.old_school_cpu_rank
        # response_debug["downloads_oa_by_age"] = self.downloads_oa_by_age
        # response_debug["num_oa_historical_by_year"] = self.num_oa_historical_by_year
        # response_debug["num_oa_historical_by_year"] = self.num_oa_historical_by_year
        # response_debug["num_bronze_by_year"] = self.num_bronze_by_year
        # response_debug["num_hybrid_by_year"] = self.num_hybrid_by_year
        # response_debug["num_green_by_year"] = self.num_green_by_year
        # response_debug["downloads_oa_by_year"] = self.downloads_oa_by_year
        # response_debug["downloads_oa_bronze_by_year"] = self.downloads_oa_bronze_by_year
        # response_debug["downloads_oa_hybrid_by_year"] = self.downloads_oa_hybrid_by_year
        # response_debug["downloads_oa_green_by_year"] = self.downloads_oa_green_by_year
        # response_debug["downloads_oa_peer_reviewed_by_year"] = self.downloads_oa_peer_reviewed_by_year
        # response_debug["downloads_oa_by_age"] = self.downloads_oa_by_age
        # response_debug["downloads_oa_bronze_by_age"] = self.downloads_oa_bronze_by_age
        # response_debug["downloads_oa_hybrid_by_age"] = self.downloads_oa_hybrid_by_age
        # response_debug["downloads_oa_green_by_age"] = self.downloads_oa_green_by_age
        # response_debug["downloads_oa_bronze_older"] = self.downloads_oa_bronze_older
        # response_debug["downloads_oa_hybrid_older"] = self.downloads_oa_hybrid_older
        # response_debug["downloads_oa_green_older"] = self.downloads_oa_green_older
        # response["debug"] = response_debug

        return response