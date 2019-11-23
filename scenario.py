# coding: utf-8

from cached_property import cached_property
import numpy as np
import pandas as pd
from collections import defaultdict
import weakref
from kids.cache import cache
import pickle
import requests
import os

from app import use_groups
from app import get_db_cursor
from time import time
from util import elapsed
from util import for_sorting
from util import TimingMessages

from journal import Journal
from consortium_journal import ConsortiumJournal
from apc_journal import ApcJournal
from assumptions import Assumptions

DEMO_PACKAGE_ID = "658349d9"
def get_clean_package_id(http_request_args):
    if not http_request_args:
        return DEMO_PACKAGE_ID
    package_id = http_request_args.get("package", "demo")
    if package_id == "demo" or package_id == "uva_elsevier":
        package_id = DEMO_PACKAGE_ID
    return package_id


def get_fresh_journal_list(issn_ls, scenario):
    journals_to_exclude = ["0370-2693"]
    if scenario.is_consortium:
        org_package_ids = scenario.data["org_package_ids"]
        journals = [ConsortiumJournal(issn_l, org_package_ids) for issn_l in issn_ls if issn_l not in journals_to_exclude]
    else:
        journals = [Journal(issn_l) for issn_l in issn_ls if issn_l not in journals_to_exclude]
        for journal in journals:
            journal.package_id = scenario.package_id
    return journals

def get_fresh_apc_journal_list(issn_ls, scenario):
    return [ApcJournal(issn_l, scenario.data, scenario) for issn_l in issn_ls]


class Scenario(object):
    years = range(0, 5)
    
    def log_timing(self, message):
        self.timing_messages.append("{: <30} {: >6}s".format(message, elapsed(self.section_time, 2)))
        self.section_time = time()
        
    def __init__(self, package_id, http_request_args=None):
        self.timing_messages = []; 
        self.section_time = time()        
        self.settings = Assumptions(http_request_args)
        self.starting_subscriptions = []
        self.is_consortium = False
        self.package_id = package_id

        if http_request_args:
            self.starting_subscriptions += http_request_args.get("subrs", []) + http_request_args.get("customSubrs", [])

        if get_consortium_package_ids(package_id):
            self.is_consortium = True

        print "getting data using package_id", package_id

        self.data = get_common_package_data_from_cache(package_id)

        self.journals = get_fresh_journal_list(self.data["unpaywall_downloads_dict"].keys(), self)
        self.log_timing("mint regular journals")
        [j.set_scenario(self) for j in self.journals]
        self.log_timing("set self in journals")
        [j.set_scenario_data(self.data) for j in self.journals]
        self.log_timing("set data in journals")

        self.log_timing("make all journals")
        for journal in self.journals:
            if journal.issn_l in self.starting_subscriptions:
                journal.set_subscribe()
        self.log_timing("subscribing to all journals")

    @cached_property
    def apc_journals(self):
        if self.data["apc"]:
            df = pd.DataFrame(self.data["apc"])
            # df["apc"] = df["apc"].astype(float)
            df["year"] = df["year"].astype(int)
            df["authorship_fraction"] = df.num_authors_from_uni/df.num_authors_total
            df["apc_fraction"] = df["apc"].astype(float) * df["authorship_fraction"]
            df_by_issn_l_and_year = df.groupby(["issn_l", "year"]).apc_fraction.agg([np.size, np.sum]).reset_index().rename(columns={'size': 'num_papers', "sum": "dollars"})
            my_dict = {"df": df, "df_by_issn_l_and_year": df_by_issn_l_and_year}
            return get_fresh_apc_journal_list(my_dict["df"].issn_l.unique(), self)
        return []

    @cached_property
    def journals_sorted_ncppu(self):
        self.journals.sort(key=lambda k: for_sorting(k.ncppu), reverse=False)
        return self.journals

    @cached_property
    def journals_sorted_use_total(self):
        self.journals.sort(key=lambda k: for_sorting(k.use_total), reverse=True)
        return self.journals

    @cached_property
    def apc_journals_sorted_spend(self):
        self.apc_journals.sort(key=lambda k: for_sorting(k.cost_apc_historical), reverse=True)
        return self.apc_journals

    @cached_property
    def subscribed(self):
        return [j for j in self.journals_sorted_ncppu if j.subscribed]

    @cached_property
    def num_citations_fuzzed_lookup(self):
        df = pd.DataFrame({"issn_l": [j.issn_l for j in self.journals], "lookup_value": [j.num_citations for j in self.journals]})
        df["ranked"] = df.lookup_value.rank(method='first')
        return dict(zip(df.issn_l, pd.qcut(df.ranked,  3, labels=["low", "medium", "high"])))

    @cached_property
    def num_authorships_fuzzed_lookup(self):
        df = pd.DataFrame({"issn_l": [j.issn_l for j in self.journals], "lookup_value": [j.num_authorships for j in self.journals]})
        df["ranked"] = df.lookup_value.rank(method='first')
        return dict(zip(df.issn_l, pd.qcut(df.ranked,  3, labels=["low", "medium", "high"])))

    @cached_property
    def use_total_fuzzed_lookup(self):
        df = pd.DataFrame({"issn_l": [j.issn_l for j in self.journals], "lookup_value": [j.use_total for j in self.journals]})
        df["ranked"] = df.lookup_value.rank(method='first')
        return dict(zip(df.issn_l, pd.qcut(df.ranked,  3, labels=["low", "medium", "high"])))


    @cached_property
    def use_total_by_year(self):
        return [np.sum([journal.use_total_by_year[year] for journal in self.journals]) for year in range(0, 5)]

    @cached_property
    def downloads_total_by_year(self):
        return [np.sum([journal.downloads_total_by_year[year] for journal in self.journals]) for year in range(0, 5)]

    @cached_property
    def use_total(self):
        return round(1 + np.mean(self.use_total_by_year), 4)

    @cached_property
    def downloads_total(self):
        return round(1 + np.mean(self.downloads_total_by_year), 4)

    @cached_property
    def downloads_actual_by_year(self):
        use = {}
        for group in use_groups:
            use[group] = [np.sum([journal.downloads_actual_by_year[group][year] for journal in self.journals]) for year in range(0, 5)]
        return use

    @cached_property
    def use_actual_by_year(self):
        use = {}
        for group in use_groups:
            use[group] = [np.sum([journal.use_actual_by_year[group][year] for journal in self.journals]) for year in range(0, 5)]
        return use

    @cached_property
    def downloads(self):
        use = {}
        for group in use_groups:
            use[group] = round(np.mean(self.downloads_actual_by_year[group]))
        return use

    @cached_property
    def use_actual(self):
        use = {}
        for group in use_groups:
            use[group] = round(np.mean(self.use_actual_by_year[group]))
        return use

    @cached_property
    def use_paywalled(self):
        return self.use_actual["subscription"] + self.use_actual["ill"] + self.use_actual["other_delayed"]

    @cached_property
    def cppu(self):
        return round(self.cost / self.use_paywalled, 2)

    @cached_property
    def ncppu(self):
        return round(self.cost / self.use_paywalled, 2)

    @cached_property
    def cost_ill(self):
        return round(sum([j.cost_ill for j in self.journals]))

    @cached_property
    def cost_subscription(self):
        return round(sum([j.cost_subscription for j in self.journals]))

    @cached_property
    def cost_subscription_minus_ill(self):
        return round(sum([j.cost_subscription_minus_ill for j in self.journals]))

    @cached_property
    def cost(self):
        return round(sum([j.cost_actual for j in self.journals]), 2)

    @cached_property
    def cost_actual_ill(self):
        return round(sum([j.cost_actual for j in self.journals if not j.subscribed]), 2)

    @cached_property
    def cost_actual_subscription(self):
        return round(sum([j.cost_actual for j in self.journals if j.subscribed]), 2)


    @cached_property
    def cost_bigdeal_projected_by_year(self):
        return [round(((1+self.settings.cost_bigdeal_increase/float(100))**year) * self.settings.cost_bigdeal )
                                            for year in self.years]

    @cached_property
    def cost_bigdeal_projected(self):
        return round(np.mean(self.cost_bigdeal_projected_by_year), 4)

    @cached_property
    def cost_saved_percent(self):
        return round(100 * float(self.cost_bigdeal_projected - self.cost) / self.cost_bigdeal_projected, 4)

    @cached_property
    def cost_spent_percent(self):
        return round(100 * float(self.cost) / self.cost_bigdeal_projected, 4)

    @cached_property
    def use_instant(self):
        return round(np.mean(self.use_instant_by_year), 4)

    @cached_property
    def use_instant_by_year(self):
        return [self.use_actual_by_year["social_networks"][year] +
                self.use_actual_by_year["backfile"][year] +
                self.use_actual_by_year["subscription"][year] +
                self.use_actual_by_year["oa"][year]
                for year in self.years]

    @cached_property
    def use_instant_percent(self):
        if not self.use_total:
            return 0
        return round(100 * float(self.use_instant) / self.use_total, 2)

    @cached_property
    def use_instant_percent_by_year(self):
        if not self.use_total:
            return [0 for year in self.years]
        return [100 * round(float(self.use_instant_by_year[year]) / self.use_total_by_year[year], 4) if self.use_total_by_year[year] else None for year in self.years]

    def get_journal(self, issn_l):
        for journal in self.journals:
            if journal.issn_l == issn_l:
                return journal
        return None


    def do_wizardly_things(self, spend):
        my_max = spend/100.0 * self.cost_bigdeal_projected

        my_spend_so_far = np.sum([j.cost_ill for j in self.journals])

        for journal in self.journals_sorted_ncppu:
            if journal.cost_subscription_minus_ill < 0:
                my_spend_so_far += journal.cost_subscription_minus_ill
                journal.set_subscribe()

        for journal in self.journals_sorted_ncppu:
            my_spend_so_far += journal.cost_subscription_minus_ill
            if my_spend_so_far > my_max:
                return
            journal.set_subscribe()

    @cached_property
    def historical_years_by_year(self):
        return range(2014, 2019)


    @cached_property
    def apc_journals_sorted_fractional_authorship(self):
        self.apc_journals.sort(key=lambda k: for_sorting(k.fractional_authorships_total), reverse=True)
        return self.apc_journals

    @cached_property
    def num_citations(self):
        return round(np.sum([j.num_citations for j in self.journals]), 4)

    @cached_property
    def num_authorships(self):
        return round(np.sum([j.num_authorships for j in self.journals]), 4)

    @cached_property
    def cost_apc_historical_by_year(self):
        return [round(np.sum([j.cost_apc_historical_by_year[year] for j in self.apc_journals])) for year in self.years]

    @cached_property
    def cost_apc_historical(self):
        return round(np.mean(self.cost_apc_historical_by_year))

    @cached_property
    def cost_apc_historical_hybrid_by_year(self):
        return [round(np.sum([j.cost_apc_historical_by_year[year] for j in self.apc_journals if j.oa_status=="hybrid"]), 4) for year in self.years]

    @cached_property
    def cost_apc_historical_hybrid(self):
        return round(np.mean(self.cost_apc_historical_hybrid_by_year))

    @cached_property
    def cost_apc_historical_gold_by_year(self):
        return [round(np.sum([j.cost_apc_historical_by_year[year] for j in self.apc_journals if j.oa_status=="gold"]), 4) for year in self.years]

    @cached_property
    def cost_apc_historical_gold(self):
        return round(np.mean(self.cost_apc_historical_gold_by_year))

    @cached_property
    def fractional_authorships_total_by_year(self):
        return [round(np.sum([j.fractional_authorships_total_by_year[year] for j in self.apc_journals]), 4) for year in self.years]

    @cached_property
    def fractional_authorships_total(self):
        return round(np.mean(self.fractional_authorships_total_by_year), 2)

    @cached_property
    def apc_price(self):
        if self.apc_journals:
            return np.max([j.apc_2019 for j in self.apc_journals])
        else:
            return 0

    @cached_property
    def num_citations_weight_percent(self):
        return (100*self.settings.weight_citation*self.num_citations)/self.use_total

    @cached_property
    def num_authorships_weight_percent(self):
        return (100*self.settings.weight_authorship*self.num_authorships)/self.use_total

    @cached_property
    def use_social_networks(self):
        return round(np.sum([j.use_actual["social_networks"] for j in self.journals]))

    @cached_property
    def use_oa(self):
        return round(np.sum([j.use_actual["oa"] for j in self.journals]))

    @cached_property
    def use_backfile(self):
        return round(np.sum([j.use_actual["backfile"] for j in self.journals]))

    @cached_property
    def use_subscription(self):
        response = round(np.sum([j.use_actual["subscription"] for j in self.journals]))
        if not response:
            response = 0.0
        return response

    @cached_property
    def use_ill(self):
        return round(np.sum([j.use_actual["ill"] for j in self.journals]))

    @cached_property
    def use_other_delayed(self):
        return round(np.sum([j.use_actual["other_delayed"] for j in self.journals]))

    @cached_property
    def use_green(self):
        return round(np.sum([j.use_oa_green for j in self.journals]))

    @cached_property
    def use_hybrid(self):
        return round(np.sum([j.use_oa_hybrid for j in self.journals]))

    @cached_property
    def use_bronze(self):
        return round(np.sum([j.use_oa_bronze for j in self.journals]))

    @cached_property
    def use_peer_reviewed(self):
        return round(np.sum([j.use_oa_peer_reviewed for j in self.journals]))

    @cached_property
    def downloads_counter_multiplier(self):
        return round(np.mean([j.downloads_counter_multiplier for j in self.journals]), 4)

    @cached_property
    def use_weight_multiplier(self):
        return round(np.mean([j.use_weight_multiplier for j in self.journals]), 4)

    @cached_property
    def use_subscription_percent(self):
        return round(float(100)*self.use_subscription/self.use_total, 1)

    @cached_property
    def use_ill_percent(self):
        return round(float(100)*self.use_ill/self.use_total, 1)

    @cached_property
    def use_free_instant_percent(self):
        return round(self.use_instant_percent - self.use_subscription_percent, 1)

    def to_dict_fulfillment(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": self.to_dict_summary_dict(),
                "name": "Fulfillment",
                "description": "Understand how uses will be filled, at the journal level.",
                "figure": [],
                "headers": [
                        {"text": "Instant Usage Percent", "value": "instant_usage_percent", "percent": self.use_instant_percent, "raw": self.use_instant_percent, "display": "number"},
                        {"text": "ASNs", "value": "use_asns", "percent": round(float(100)*self.use_social_networks/self.use_total), "raw": self.use_social_networks, "display": "number"},
                        {"text": "Open access", "value": "use_oa", "percent": round(float(100)*self.use_oa/self.use_total), "raw": self.use_oa, "display": "number"},
                        {"text": "Backfile", "value": "use_backfile", "percent": round(float(100)*self.use_backfile/self.use_total), "raw": self.use_backfile, "display": "number"},
                        {"text": "Subscription", "value": "use_subscription", "percent": round(float(100)*self.use_subscription/self.use_total), "raw": self.use_subscription, "display": "number"},
                        {"text": "ILL", "value": "use_ill", "percent": round(float(100)*self.use_ill/self.use_total), "raw": self.use_ill, "display": "number"},
                        {"text": "Other (delayed)", "value": "use_other_delayed", "percent": round(float(100)*self.use_other_delayed/self.use_total), "raw": self.use_other_delayed, "display": "number"},
                ],
                "journals": [j.to_dict_fulfillment() for j in self.journals_sorted_use_total[0:pagesize]],
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response


    def to_dict_oa(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "name": "Open Access",
                "_summary": self.to_dict_summary_dict(),
                "description": "Understand the Open Access availability of articles in journals.",
                "figure": [],
                "headers": [
                        {"text": "Percent of Usage that is OA", "value": "use_oa_percent", "percent": round(float(100)*self.use_oa/self.use_total), "raw": self.use_oa, "display": "percent"},
                        {"text": "Percent of Usage that is Green OA", "value": "use_green_percent", "percent": round(float(100)*self.use_green/self.use_total), "raw": self.use_green, "display": "percent"},
                        {"text": "Percent of Usage that is Hybrid OA", "value": "use_hybrid_percent", "percent": round(float(100)*self.use_hybrid/self.use_total), "raw": self.use_hybrid, "display": "percent"},
                        {"text": "Percent of Usage that is Bronze OA", "value": "use_bronze_percent", "percent": round(float(100)*self.use_bronze/self.use_total), "raw": self.use_bronze, "display": "percent"},
                        {"text": "Percent of Usage that is Peer-reviewed OA", "value": "use_peer_reviewed_percent", "percent": round(float(100)*self.use_peer_reviewed/self.use_total), "raw": self.use_peer_reviewed, "display": "percent"},
                ],
                "journals": [j.to_dict_oa() for j in self.journals_sorted_use_total[0:pagesize]],
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response



    def to_dict_impact(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": self.to_dict_summary_dict(),
                "name": "Institutional Value",
                "description": "Understand journal use by your institution.",
                "figure": [
                ],
                "headers": [
                        {"text": "Total Usage", "value":"total_usage", "percent": 100, "raw": self.use_total, "display": "number"},
                        {"text": "Downloads", "value":"downloads", "percent": 100*self.downloads_total/self.use_total, "raw": self.downloads_total, "display": "number"},
                        {"text": "Citations to papers", "value":"citations", "percent": self.num_citations_weight_percent, "raw": self.num_citations, "display": "float1"},
                        {"text": "Authored papers", "value":"authorships", "percent": self.num_authorships_weight_percent, "raw": self.num_authorships, "display": "float1"},
                ],
                "journals": [j.to_dict_impact() for j in self.journals_sorted_use_total[0:pagesize]],
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response


    def to_dict_table(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "name": "Overview",
                "_summary": self.to_dict_summary_dict(),
                "description": "Understand your scenario at the journal level.",
                "figure": [],
                "headers": [
                        {"text": "Net cost per paid use", "value": "ncppu", "percent": None, "raw": self.ncppu, "display": "currency"},
                        {"text": "Cost", "value": "cost", "percent": None, "raw": self.cost, "display": "currency_int"},
                        {"text": "Usage", "value": "use", "percent": None, "raw": self.use_total, "display": "number"},
                        {"text": "Instant Usage Percent", "value": "instant_usage_percent", "percent": self.use_instant_percent, "raw": self.use_instant_percent, "display": "percent"},
                        {"text": "Free Instant Usage Percent", "value": "free_instant_usage_percent", "percent": None, "raw": None, "display": "percent"},

                        # cost
                        {"text": "Scenario Cost", "value": "scenario_cost", "percent": None, "raw": self.cost, "display": "currency_int"},
                        {"text": "Subscription Cost", "value": "subscription_cost", "percent": None, "raw": self.cost_subscription, "display": "currency_int"},
                        {"text": "ILL Cost", "value": "ill_cost", "percent": None, "raw": self.cost_ill, "display": "currency_int"},
                        {"text": "Subscription minus ILL Cost", "value": "real_cost", "percent": None, "raw": self.cost_subscription_minus_ill, "display": "currency_int"},

                        # fulfillment
                        {"text": "Percent of Usage from ASNs", "value": "use_asns", "percent": round(float(100)*self.use_social_networks/self.use_total), "raw": self.use_social_networks, "display": "percent"},
                        {"text": "Percent of Usage from Open access", "value": "use_oa", "percent": round(float(100)*self.use_oa/self.use_total), "raw": self.use_oa, "display": "percent"},
                        {"text": "Percent of Usage from Backfile", "value": "use_backfile", "percent": round(float(100)*self.use_backfile/self.use_total), "raw": self.use_backfile, "display": "percent"},
                        {"text": "Percent of Usage from Subscription", "value": "use_subscription", "percent": round(float(100)*self.use_subscription/self.use_total), "raw": self.use_subscription, "display": "percent"},
                        {"text": "Percent of Usage from ILL", "value": "use_ill", "percent": round(float(100)*self.use_ill/self.use_total), "raw": self.use_ill, "display": "percent"},
                        {"text": "Percent of Usage from Other (delayed)", "value": "use_other_delayed", "percent": round(float(100)*self.use_other_delayed/self.use_total), "raw": self.use_other_delayed, "display": "percent"},

                        # oa
                        {"text": "Percent of Usage from OA", "value": "use_oa_percent", "percent": round(float(100)*self.use_oa/self.use_total), "raw": self.use_oa, "display": "percent"},
                        {"text": "Percent of Usage from Green OA", "value": "use_green_percent", "percent": round(float(100)*self.use_green/self.use_total), "raw": self.use_green, "display": "percent"},
                        {"text": "Percent of Usage from Hybrid OA", "value": "use_hybrid_percent", "percent": round(float(100)*self.use_hybrid/self.use_total), "raw": self.use_hybrid, "display": "percent"},
                        {"text": "Percent of Usage from Bronze OA", "value": "use_bronze_percent", "percent": round(float(100)*self.use_bronze/self.use_total), "raw": self.use_bronze, "display": "percent"},
                        {"text": "Percent of Usage from Peer-reviewed OA", "value": "use_peer_reviewed_percent", "percent": round(float(100)*self.use_peer_reviewed/self.use_total), "raw": self.use_peer_reviewed, "display": "percent"},

                        # impact
                        {"text": "Total Usage", "value":"total_usage", "percent": 100, "raw": self.use_total, "display": "number"},
                        {"text": "Downloads", "value":"downloads", "percent": 100*self.downloads_total/self.use_total, "raw": self.downloads_total, "display": "number"},
                        {"text": "Citations to papers", "value":"citations", "percent": self.num_citations_weight_percent, "raw": self.num_citations, "display": "float1"},
                        {"text": "Authored papers", "value":"authorships", "percent": self.num_authorships_weight_percent, "raw": self.num_authorships, "display": "float1"},

                ],
                "journals": [j.to_dict_table() for j in self.journals_sorted_ncppu[0:pagesize]],
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response


    def to_dict_overview(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "name": "Overview",
                "_summary": self.to_dict_summary_dict(),
                "description": "Understand your scenario at the journal level.",
                "figure": [],
                "headers": [
                        {"text": "Net cost per paid use", "value": "ncppu", "percent": None, "raw": self.ncppu, "display": "currency"},
                        {"text": "Cost", "value": "cost", "percent": None, "raw": self.cost, "display": "currency_int"},
                        {"text": "Usage", "value": "use", "percent": None, "raw": self.use_total, "display": "number"},
                        {"text": "Instant Usage Percent", "value": "instant_usage_percent", "percent": self.use_instant_percent, "raw": self.use_instant_percent, "display": "percent"},
                ],
                "journals": [j.to_dict_overview() for j in self.journals_sorted_ncppu[0:pagesize]],
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_cost(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": self.to_dict_summary_dict(),
                "name": "Subscription Cost",
                "description": "Understand the cost of your subscriptions and ILL requests.",
                "figure": [],
                "headers": [
                        {"text": "Net cost per paid use (NCPPU)", "value": "ncppu", "percent": None, "raw": self.ncppu, "display": "currency"},
                        {"text": "Scenario Cost", "value": "scenario_cost", "percent": None, "raw": self.cost, "display": "currency_int"},
                        {"text": "Subscription Cost", "value": "subscription_cost", "percent": None, "raw": self.cost_subscription, "display": "currency_int"},
                        {"text": "ILL Cost", "value": "ill_cost", "percent": None, "raw": self.cost_ill, "display": "currency_int"},
                        {"text": "Subscription minus ILL Cost", "value": "real_cost", "percent": None, "raw": self.cost_subscription_minus_ill, "display": "currency_int"},
                ],
                "journals": [j.to_dict_cost() for j in self.journals_sorted_ncppu[0:pagesize]],
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_apc(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": self.to_dict_summary_dict(),
                "name": "APC Cost",
                "description": "Understand how much your institution spends on APCs with this publisher.",
                "figure": [],
                "headers": [
                        {"text": "OA type", "value": "oa_status", "percent": None, "raw": None, "display": "text"},
                        {"text": "APC price", "value": "apc_price", "percent": None, "raw": self.apc_price, "display": "currency_int"},
                        {"text": "Total fractional authorship", "value": "fractional_authorship", "percent": None, "raw": self.fractional_authorships_total, "display": "float1"},
                        {"text": "APC Dollars Spent", "value": "cost_apc", "percent": None, "raw": self.cost_apc_historical, "display": "currency_int"},
                ],
                "journals": [j.to_dict() for j in self.apc_journals_sorted_spend[0:pagesize]],
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_report(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": {
                    "cost_percent": self.cost_spent_percent,
                    "num_journals_subscribed": len(self.subscribed),
                    "num_journals_total": len(self.journals),
                    "use_instant_percent_by_year": self.use_instant_percent_by_year,
                    "use_instant_percent": self.use_instant_percent,
                    },
                "journals": [j.to_dict_report() for j in self.journals_sorted_use_total[0:pagesize]],
                "journals_count": len(self.journals),
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_timeline(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": self.to_dict_summary_dict(),
                "journals": [j.to_dict_timeline() for j in self.journals_sorted_use_total[0:pagesize]],
                "journals_count": len(self.journals),
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_summary(self):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": self.to_dict_summary_dict(),
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_slider(self):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": self.to_dict_summary_dict(),
                "journals": [j.to_dict_slider() for j in self.journals_sorted_ncppu],
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_summary_dict(self):
        response = {
                    "cost_scenario": self.cost,
                    "cost_scenario_ill": self.cost_actual_ill,
                    "cost_scenario_subscription": self.cost_actual_subscription,
                    "cost_bigdeal_projected": self.cost_bigdeal_projected,
                    "cost_percent": self.cost_spent_percent,
                    "num_journals_subscribed": len(self.subscribed),
                    "num_journals_total": len(self.journals),
                    "use_instant_percent": self.use_instant_percent,
                    "use_free_instant_percent": self.use_free_instant_percent,
                    "use_subscription_percent": self.use_subscription_percent,
                    "use_ill_percent": self.use_ill_percent


        }
        return response

    def to_dict(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": self.to_dict_summary_dict(),
                "journals": [j.to_dict() for j in self.journals_sorted_ncppu[0:pagesize]],
                "journals_count": len(self.journals),
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def __repr__(self):
        return u"<{} (n={})>".format(self.__class__.__name__, len(self.journals))


@cache
def get_consortium_package_ids(package_id):
    command = """select package_id from jump_account_package where consortium_package_id = '{}'""".format(package_id)
    rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    package_ids = [row["package_id"] for row in rows]
    return package_ids

@cache
def get_package_specific_scenario_data_from_db(input_package_id):
    timing = []
    section_time = time()

    consortium_package_ids = get_consortium_package_ids(input_package_id)
    if not consortium_package_ids:
        consortium_package_ids = [input_package_id]
    counter_dict = defaultdict(int)
    for package_id in consortium_package_ids:

        command = "select issn_l, total from jump_counter where package_id='{}'".format(package_id)
        rows = None
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()
        for row in rows:
            counter_dict[row["issn_l"]] += row["total"]

    timing.append(("time from db: counter", elapsed(section_time, 2)))
    section_time = time()

    consortium_package_ids_string = ",".join(["'{}'".format(package_id) for package_id in consortium_package_ids])

    command = """select citing.issn_l, citing.year, sum(num_citations) as num_citations
        from jump_citing citing
        join jump_account_grid_id account_grid on citing.grid_id = account_grid.grid_id
        join jump_account_package account_package on account_grid.account_id = account_package.account_id
        where citing.year < 2019 
        and package_id in ({})
        group by issn_l, year""".format(consortium_package_ids_string)
    citation_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        citation_rows = cursor.fetchall()
    citation_dict = defaultdict(dict)
    for row in citation_rows:
        citation_dict[row["issn_l"]][round(row["year"])] = round(row["num_citations"])

    timing.append(("time from db: citation_rows", elapsed(section_time, 2)))
    section_time = time()

    command = """
        select authorship.issn_l, authorship.year, sum(num_authorships) as num_authorships
        from jump_authorship authorship
        join jump_account_grid_id account_grid on authorship.grid_id = account_grid.grid_id
        join jump_account_package account_package on account_grid.account_id = account_package.account_id
        where authorship.year < 2019 
        and package_id in ({})
        group by issn_l, year""".format(consortium_package_ids_string)
    authorship_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        authorship_rows = cursor.fetchall()
    authorship_dict = defaultdict(dict)
    for row in authorship_rows:
        authorship_dict[row["issn_l"]][round(row["year"])] = round(row["num_authorships"])

    timing.append(("time from db: authorship_rows", elapsed(section_time, 2)))
    section_time = time()

    data = {
        "timing": timing,
        "counter_dict": counter_dict,
        "citation_dict": citation_dict,
        "authorship_dict": authorship_dict
    }

    return data


@cache
def get_apc_data_from_db(input_package_id):
    consortium_package_ids = get_consortium_package_ids(input_package_id)
    if not consortium_package_ids:
        consortium_package_ids = [input_package_id]
    consortium_package_ids_string = ",".join(["'{}'".format(package_id) for package_id in consortium_package_ids])

    command = """select * from jump_apc_authorships where package_id in ({})
                    """.format(consortium_package_ids_string)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()

    return rows



@cache
def get_embargo_data_from_db():
    command = "select issn_l, embargo from journal_delayed_oa_active"
    embargo_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        embargo_rows = cursor.fetchall()
    embargo_dict = dict((a["issn_l"], round(a["embargo"])) for a in embargo_rows)
    return embargo_dict


@cache
def get_unpaywall_downloads_from_db():
    command = "select * from jump_elsevier_unpaywall_downloads"
    big_view_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        big_view_rows = cursor.fetchall()
    unpaywall_downloads_dict = dict((row["issn_l"], row) for row in big_view_rows)
    return unpaywall_downloads_dict



@cache
def get_oa_recent_data_from_db():
    oa_dict = {}
    for submitted in ["with_submitted", "no_submitted"]:
        for bronze in ["with_bronze", "no_bronze"]:
            key = "{}_{}".format(submitted, bronze)
            command = """select * from jump_oa_recent_{}_elsevier
                            """.format(key)
            with get_db_cursor() as cursor:
                cursor.execute(command)
                rows = cursor.fetchall()
            lookup_dict = defaultdict(list)
            for row in rows:
                lookup_dict[row["issn_l"]] += [row]
            oa_dict[key] = lookup_dict
    return oa_dict

@cache
def get_oa_data_from_db():
    oa_dict = {}
    for submitted in ["with_submitted", "no_submitted"]:
        for bronze in ["with_bronze", "no_bronze"]:
            key = "{}_{}".format(submitted, bronze)
            command = """select * from jump_oa_{}_elsevier
                        where year_int >= 2015
                            """.format(key)
            with get_db_cursor() as cursor:
                cursor.execute(command)
                rows = cursor.fetchall()
            lookup_dict = defaultdict(list)
            for row in rows:
                lookup_dict[row["issn_l"]] += [row]
            oa_dict[key] = lookup_dict
    return oa_dict

@cache
def get_society_data_from_db():
    command = "select issn_l, is_society_journal from jump_society_journals_input where is_society_journal is not null"
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    lookup_dict = defaultdict(list)
    for row in rows:
        lookup_dict[row["issn_l"]] = row["is_society_journal"]
    return lookup_dict



@cache
def get_social_networks_data_from_db():
    command = """select issn_l, asn_only_rate::float from jump_mturk_asn_rates
                    """
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    lookup_dict = {}
    for row in rows:
        lookup_dict[row["issn_l"]] = row["asn_only_rate"]
    return lookup_dict

@cache
def get_oa_adjustment_data_from_db():
    command = """select issn_l, 
            max(mturk.max_oa_rate::float) as mturk_max_oa_rate, 
            count(*) as num_papers_3_years,
            sum(case when u.oa_status = 'closed' then 0 else 1 end) as num_papers_3_years_oa, 
            round(sum(case when u.oa_status = 'closed' then 0 else 1 end)/count(*)::float, 3) as unpaywall_measured_fraction_3_years_oa
            from jump_mturk_oa_rates mturk
            join unpaywall u on mturk.issn_l = u.journal_issn_l
            where year >= 2016 and year <= 2018
            and genre='journal-article'
            group by issn_l
                    """
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    lookup_dict = {}
    for row in rows:
        lookup_dict[row["issn_l"]] = row
    return lookup_dict


@cache
def get_common_package_data(package_id):
    my_timing = TimingMessages()

    # package_id specific
    my_data = {}
    org_package_ids = get_consortium_package_ids(package_id)
    if org_package_ids:
        my_data["org_package_ids"] = org_package_ids
    else:
        my_data["org_package_ids"] = [package_id]
    my_timing.log_timing("get_consortium_package_ids")

    for org_package_id in my_data["org_package_ids"]:
        my_data[org_package_id] = get_package_specific_scenario_data_from_db(org_package_id)
        my_timing.log_timing("get_package_specific_scenario_data_from_db")

    my_data["apc"] = get_apc_data_from_db(package_id)  # gets everything from consortium itself
    my_timing.log_timing("get_apc_data_from_db")

    # not package_id specific

    my_data["embargo_dict"] = get_embargo_data_from_db()
    my_timing.log_timing("get_embargo_data_from_db")

    my_data["unpaywall_downloads_dict"] = get_unpaywall_downloads_from_db()
    my_timing.log_timing("get_unpaywall_downloads_from_db")

    my_data["oa"] = get_oa_data_from_db()
    my_timing.log_timing("get_oa_data_from_db")

    my_data["oa_recent"] = get_oa_recent_data_from_db()
    my_timing.log_timing("get_oa_data_from_db")

    my_data["social_networks"] = get_social_networks_data_from_db()
    my_timing.log_timing("get_social_networks_data_from_db")

    my_data["oa_adjustment"] = get_oa_adjustment_data_from_db()
    my_timing.log_timing("get_oa_adjustment_data_from_db")

    my_data["society"] = get_society_data_from_db()
    my_timing.log_timing("get_society_data_from_db")

    my_data["_timing"] = my_timing.to_dict()

    return my_data


@cache
def get_common_package_data_from_cache(package_id):
    package_id_in_cache = package_id
    if package_id.startswith("demo") or package_id==DEMO_PACKAGE_ID:
        package_id_in_cache = DEMO_PACKAGE_ID

    r = requests.get("https://cdn.unpaywalljournals.org/data/common/{}?secret={}".format(
        package_id_in_cache, os.getenv("JWT_SECRET_KEY")))
    if r.status_code == 200:
        return r.json()
    return None
