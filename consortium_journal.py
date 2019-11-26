# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
import inspect
from multiprocessing.pool import ThreadPool
import threading

from app import use_groups
from app import use_groups_free_instant
from journal import Journal


class ConsortiumJournal(Journal):

    
    def __init__(self, issn_l, org_package_ids, package_id=None):
        self.issn_l = issn_l
        self.org_package_ids = org_package_ids
        self.consortium_journals = [Journal(self.issn_l, package_id=org_package_id) for org_package_id in org_package_ids]

        # if not hasattr(threading.current_thread(), "_children"):
        #     threading.current_thread()._children = weakref.WeakKeyDictionary()
        #
        # my_thread_pool = ThreadPool(50)
        #
        # def cache_attributes(my_journal):
        #     return my_journal.to_dict_slider()
        #
        # results = my_thread_pool.imap_unordered(cache_attributes, self.consortium_journals)
        # my_thread_pool.close()
        # my_thread_pool.join()
        # my_thread_pool.terminate()

        self.subscribed = False
        self.use_default_download_curve = False

    def set_scenario(self, scenario):
        if scenario:
            self.scenario = weakref.proxy(scenario)
            self.settings = self.scenario.settings
            [j.set_scenario(scenario) for j in self.consortium_journals]
        else:
            self.scenario = None
            self.settings = None

    def set_scenario_data(self, scenario_data):
        [j.set_scenario_data(scenario_data) for j in self.consortium_journals]
        self._scenario_data = scenario_data

    def sum_attribute_by_year(self):
        attribute_name = inspect.currentframe().f_back.f_code.co_name
        response = [np.sum([getattr(j, attribute_name)[year] or 0 for j in self.consortium_journals]) for year in self.years]
        return response

    def sum_attribute(self):
        attribute_name = inspect.currentframe().f_back.f_code.co_name
        values = [getattr(j, attribute_name) for j in self.consortium_journals if getattr(j, attribute_name) != None]
        if values:
            response = np.sum(values)
        else:
            response = None
        return response

    @cached_property
    def cost_subscription_2018(self):
        return self.sum_attribute()

    @cached_property
    def num_citations_historical_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_citations(self):
        return self.sum_attribute()

    @cached_property
    def num_authorships_historical_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_authorships(self):
        return self.sum_attribute()


    @cached_property
    def cost_actual_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def cost_actual(self):
        return self.sum_attribute()


    @cached_property
    def cost_subscription_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def cost_subscription(self):
        return self.sum_attribute()

    @cached_property
    def cppu_downloads(self):
        return self.sum_attribute()

    @cached_property
    def cppu_use(self):
        return self.sum_attribute()

    @cached_property
    def ncppu(self):
        return self.sum_attribute()

    @cached_property
    def use_instant_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def use_instant(self):
        return self.sum_attribute()

    @cached_property
    def downloads_subscription_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_subscription(self):
        return self.sum_attribute()

    @cached_property
    def use_subscription(self):
        return self.sum_attribute()

    @cached_property
    def use_subscription_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_social_networks_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def downloads_social_networks(self):
        return self.sum_attribute()

    @cached_property
    def use_social_networks_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def use_social_networks(self):
        return self.sum_attribute()


    @cached_property
    def downloads_ill_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def downloads_ill(self):
        return self.sum_attribute()

    @cached_property
    def use_ill(self):
        return self.sum_attribute()

    @cached_property
    def use_ill_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_other_delayed_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_other_delayed(self):
        return self.sum_attribute()

    @cached_property
    def use_other_delayed(self):
        return self.sum_attribute()

    @cached_property
    def use_other_delayed_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_backfile_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def downloads_backfile(self):
        return self.sum_attribute()

    @cached_property
    def use_backfile(self):
        return self.sum_attribute()

    @cached_property
    def use_backfile_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_oa_historical_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_oa_historical(self):
        return self.sum_attribute()

    @cached_property
    def num_oa_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_oa(self):
        return self.sum_attribute()

    @cached_property
    def downloads_oa_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def use_oa(self):
        return self.sum_attribute()

    @cached_property
    def use_oa_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def downloads_total_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_total(self):
        return self.sum_attribute()


    @cached_property
    def use_total_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def use_total(self):
        return self.sum_attribute()


    @cached_property
    def downloads_by_age(self):
        return self.sum_attribute_by_year()


    @cached_property
    def downloads_total_older_than_five_years(self):
        return self.sum_attribute()

    @cached_property
    def downloads_per_paper_by_age(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_oa_by_age(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_paywalled_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_paywalled(self):
        return self.sum_attribute()

    @cached_property
    def use_paywalled(self):
        return self.sum_attribute()

    @cached_property
    def use_actual_by_year(self):
        my_dict = {}
        for group in use_groups:
            my_dict[group] = [0 for year in self.years]
        for my_journal in self.consortium_journals:
            if my_journal.use_total:
                for year in self.years:
                    my_dict[group][year] += my_journal.use_actual_by_year[group][year]
        return my_dict


    @cached_property
    def cost_ill(self):
        return self.sum_attribute()

    @cached_property
    def cost_ill_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def use_instant_percent(self):
        if not self.use_total:
            return 0
        return min(100.0, round(100 * float(self.use_instant) / self.use_total, 4))

    @cached_property
    def num_green_historical_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_green_historical(self):
        return self.sum_attribute()

    @cached_property
    def downloads_oa_green(self):
        return self.sum_attribute()

    @cached_property
    def use_oa_green(self):
        return self.sum_attribute()


    @cached_property
    def num_hybrid_historical_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_hybrid_historical(self):
        return self.sum_attribute()

    @cached_property
    def downloads_oa_hybrid(self):
        return self.sum_attribute()

    @cached_property
    def use_oa_hybrid(self):
        return self.sum_attribute()


    @cached_property
    def num_bronze_historical_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def num_bronze_historical(self):
        return self.sum_attribute()

    @cached_property
    def downloads_oa_bronze(self):
        return self.sum_attribute()


    @cached_property
    def use_oa_bronze(self):
        return self.sum_attribute()


    @cached_property
    def num_peer_reviewed_historical_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def num_peer_reviewed_historical(self):
        return self.sum_attribute()

    @cached_property
    def downloads_oa_peer_reviewed(self):
        return self.sum_attribute()

    @cached_property
    def use_oa_peer_reviewed(self):
        return self.sum_attribute()

    def to_dict_details(self):
        response = super(ConsortiumJournal, self).to_dict_details()
        response["use_by_package_id"] = [(journal.package_id, round(float(journal.use_total)/self.use_total, 2)) for journal in self.consortium_journals if journal.use_total >= 1]
        response["use_by_package_id"] = sorted(response["use_by_package_id"], key=lambda x: x[1], reverse=True)
        return response

    def to_dict_slider(self):
        # response = super(ConsortiumJournal, self).to_dict_slider()
        # response["use_by_package_id"] = [(journal.package_id, round(float(journal.use_total)/self.use_total, 2)) for journal in self.consortium_journals if journal.use_total >= 1]
        # response["use_by_package_id"] = sorted(response["use_by_package_id"], key=lambda x: x[1], reverse=True)
        response = {}
        return response