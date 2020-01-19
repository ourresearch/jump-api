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


class ConsortiumJournal(object):
    years = range(0, 5)

    def __init__(self, issn_l, meta_data, org_data):
        self.issn_l = issn_l
        self.org_data = org_data
        self.meta_data = meta_data
        self.subscribed = False
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

    def set_subscribe(self):
        self.subscribed = True
        # invalidate cache
        for key in self.__dict__:
            if "actual" in key:
                del self.__dict__[key]

    def set_unsubscribe(self):
        self.subscribed = False
        # invalidate cache
        for key in self.__dict__:
            if "actual" in key:
                del self.__dict__[key]

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
            if my_org_dict.get("has_perpetual_access", False):
                response = True
        return response

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

    def to_dict_table(self):
        response = OrderedDict()
        response["meta"] = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed}
        table_row = OrderedDict()

        # table
        table_row["ncppu"] = self.ncppu
        table_row["ncppu_rank"] = self.ncppu_rank
        table_row["cost"] = self.cost_actual
        table_row["usage"] = self.use_total
        table_row["instant_usage_percent"] = self.use_instant_percent
        table_row["free_instant_usage_percent"] = self.use_free_instant_percent

        # cost
        table_row["scenario_cost"] = round(self.cost_actual)
        table_row["subscription_cost"] = round(self.cost_subscription)
        table_row["ill_cost"] = round(self.cost_ill)
        table_row["subscription_minus_ill_cost"] = round(self.cost_subscription_minus_ill)
        table_row["old_school_cpu"] = None
        table_row["old_school_cpu_rank"] = None

        # fulfillment
        table_row["use_asns_percent"] = round(float(100)*self.use_actual["social_networks"]/self.use_total)
        table_row["use_oa_percent"] = round(float(100)*self.use_actual["oa"]/self.use_total)
        table_row["use_backfile_percent"] = round(float(100)*self.use_actual["backfile"]/self.use_total)
        table_row["use_subscription_percent"] = round(float(100)*self.use_actual["subscription"]/self.use_total)
        table_row["use_ill_percent"] = round(float(100)*self.use_actual["ill"]/self.use_total)
        table_row["use_other_delayed_percent"] =  round(float(100)*self.use_actual["other_delayed"]/self.use_total)
        table_row["has_perpetual_access"] = self.has_perpetual_access

        # oa
        table_row["use_oa_percent"] = round(float(100)*self.use_actual["oa"]/self.use_total)
        table_row["use_green_percent"] = round(float(100)*self.use_oa_green/self.use_total)
        table_row["use_hybrid_percent"] = round(float(100)*self.use_oa_hybrid/self.use_total)
        table_row["use_bronze_percent"] = round(float(100)*self.use_oa_bronze/self.use_total)
        table_row["use_peer_reviewed_percent"] =  round(float(100)*self.use_oa_peer_reviewed/self.use_total)

        # impact
        table_row["total_usage"] = round(self.use_total)
        table_row["downloads"] = round(self.downloads_total)
        table_row["citations"] = round(self.num_citations, 1)
        table_row["authorships"] = round(self.num_authorships, 1)


        response["table_row"] = table_row

        return response


    def to_dict_slider(self):
        response = {"issn_l": self.issn_l,
                "title": self.title,
                "subject": self.subject,
                "downloads_total": self.downloads_total,
                "use_total": self.use_total,
                "cost_subscription": self.cost_subscription,
                "cost_ill": self.cost_ill,
                "cost_subscription_minus_ill": self.cost_subscription_minus_ill,
                "ncppu": self.ncppu,
                "subscribed": self.subscribed,
                "use_instant": self.use_instant,
                "use_instant_percent": self.use_instant_percent,
                }
        response["use_groups_free_instant"] = {}
        for group in use_groups_free_instant:
            response["use_groups_free_instant"][group] = self.use_actual[group]
        response["use_groups_if_subscribed"] = {"subscription": self.use_subscription}
        response["use_groups_if_not_subscribed"] = {"ill": self.use_ill, "other_delayed": self.use_other_delayed}
        return response
