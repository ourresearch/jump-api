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
