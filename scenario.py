# coding: utf-8

from cached_property import cached_property
import numpy as np
import pandas as pd
from collections import defaultdict
import weakref
from kids.cache import cache

from app import use_groups
from app import get_db_cursor
from time import time
from util import elapsed
from util import for_sorting

from journal import Journal
from apc_journal import ApcJournal
from assumptions import Assumptions

def get_fresh_journal_list(issn_ls):
    journals = [Journal(issn_l) for issn_l in issn_ls]
    return journals

class Scenario(object):
    years = range(0, 5)
    
    def log_timing(self, message):
        self.timing_messages.append("{: <30} {: >6}s".format(message, elapsed(self.section_time, 2)))
        self.section_time = time()
        
    def __init__(self, package, http_request_args=None):
        self.timing_messages = []; 
        self.section_time = time()        
        self.settings = Assumptions(http_request_args)
        self.starting_subscriptions = []
        if http_request_args:
            self.starting_subscriptions += http_request_args.get("subrs", []) + http_request_args.get("customSubrs", [])

        self.data = get_scenario_data_from_db(package)
        self.log_timing("get_scenario_data_from_db")

        self.data["apc"] = get_apc_data_from_db(package)
        self.log_timing("get_apc_data_from_db")

        # self.data["oa"] = get_oa_data_from_db(package)
        # self.log_timing("get_oa_data_from_db")

        self.log_timing("mint apc journals")

        # self.journals = [Journal(issn_l, self.data, self) for issn_l in self.data["big_view_dict"]]

        self.journals = get_fresh_journal_list(self.data["big_view_dict"])
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
        return [ApcJournal(issn_l, self.data, self) for issn_l in self.data["apc"]["df"].issn_l.unique()]

    @cached_property
    def journals_sorted_cppu(self):
        self.journals.sort(key=lambda k: for_sorting(k.cppu_weighted), reverse=False)
        return self.journals

    @cached_property
    def journals_sorted_cppu_delta(self):
        self.journals.sort(key=lambda k: for_sorting(k.cppu_delta_weighted), reverse=False)
        return self.journals

    @cached_property
    def journals_sorted_use_total(self):
        self.journals.sort(key=lambda k: for_sorting(k.use_total_weighted), reverse=True)
        return self.journals

    @cached_property
    def subscribed(self):
        return [j for j in self.journals_sorted_cppu if j.subscribed]

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
    def use_total_weighted_by_year(self):
        return [np.sum([journal.use_total_weighted_by_year[year] for journal in self.journals]) for year in range(0, 5)]

    @cached_property
    def use_total_unweighted_by_year(self):
        return [np.sum([journal.use_total_by_year[year] for journal in self.journals]) for year in range(0, 5)]

    @cached_property
    def use_total_weighted(self):
        return round(np.mean(self.use_total_weighted_by_year), 4)

    @cached_property
    def use_total_unweighted(self):
        return round(np.mean(self.use_total_unweighted_by_year), 4)

    @cached_property
    def use_actual_unweighted_by_year(self):
        use = {}
        for group in use_groups:
            use[group] = [np.sum([journal.use_actual_unweighted_by_year[group][year] for journal in self.journals]) for year in range(0, 5)]
        return use

    @cached_property
    def use_actual_weighted_by_year(self):
        use = {}
        for group in use_groups:
            use[group] = [np.sum([journal.use_actual_weighted_by_year[group][year] for journal in self.journals]) for year in range(0, 5)]
        return use

    @cached_property
    def use_unweighted(self):
        use = {}
        for group in use_groups:
            use[group] = int(np.mean(self.use_actual_unweighted_by_year[group]))
        return use

    @cached_property
    def use_actual_weighted(self):
        use = {}
        for group in use_groups:
            use[group] = int(np.mean(self.use_actual_weighted_by_year[group]))
        return use

    @cached_property
    def cost_by_group(self):
        # TODO this needs to be redone by year
        cost = {}
        for group in use_groups:
            cost[group] = 0
        # now overwrite for ILL
        cost["ill"] = round(self.use_unweighted["ill"] * self.settings.cost_ill, 2)
        return cost

    @cached_property
    def cost(self):
        return round(sum([j.cost_actual for j in self.journals_sorted_cppu]), 2)

    @cached_property
    def cost_bigdeal_projected_by_year(self):
        return [int(((1+self.settings.cost_bigdeal_increase/float(100))**year) * self.settings.cost_bigdeal )
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
        return [self.use_actual_weighted_by_year["social_networks"][year] +
                self.use_actual_weighted_by_year["backfile"][year] +
                self.use_actual_weighted_by_year["subscription"][year] +
                self.use_actual_weighted_by_year["oa"][year]
                for year in self.years]

    @cached_property
    def use_instant_percent(self):
        if not self.use_total_weighted:
            return None
        return round(100 * float(self.use_instant) / self.use_total_weighted, 4)

    @cached_property
    def use_instant_percent_by_year(self):
        if not self.use_total_weighted:
            return None
        return [100 * round(float(self.use_instant_by_year[year]) / self.use_total_weighted_by_year[year], 4) if self.use_total_weighted_by_year[year] else None for year in self.years]

    def get_journal(self, issn_l):
        for journal in self.journals:
            if journal.issn_l == issn_l:
                return journal
        return None


    def do_wizardly_things(self, spend):
        my_max = spend/100.0 * self.cost_bigdeal_projected

        my_spend_so_far = np.sum([j.cost_ill for j in self.journals])

        for journal in self.journals_sorted_cppu_delta:
            if journal.cost_subscription_minus_ill < 0:
                my_spend_so_far += journal.cost_subscription_minus_ill
                journal.set_subscribe()

        for journal in self.journals_sorted_cppu_delta:
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
    def cost_apc_historical_by_year(self):
        return [round(np.sum([j.cost_apc_historical_by_year[year] for j in self.apc_journals]), 4) for year in self.years]

    @cached_property
    def cost_apc_historical(self):
        return round(np.mean(self.cost_apc_historical_by_year), 4)

    @cached_property
    def cost_apc_historical_hybrid_by_year(self):
        return [round(np.sum([j.cost_apc_historical_by_year[year] for j in self.apc_journals if j.oa_status=="hybrid"]), 4) for year in self.years]

    @cached_property
    def cost_apc_historical_hybrid(self):
        return round(np.mean(self.cost_apc_historical_hybrid_by_year), 4)

    @cached_property
    def cost_apc_historical_gold_by_year(self):
        return [round(np.sum([j.cost_apc_historical_by_year[year] for j in self.apc_journals if j.oa_status=="gold"]), 4) for year in self.years]

    @cached_property
    def cost_apc_historical_gold(self):
        return round(np.mean(self.cost_apc_historical_gold_by_year), 4)

    @cached_property
    def num_apc_papers_historical_by_year(self):
        return [np.sum([j.num_apc_papers_historical_by_year[year] for j in self.apc_journals]) for year in self.years]

    @cached_property
    def num_apc_papers_historical(self):
        return np.mean(self.num_apc_papers_historical_by_year)

    @cached_property
    def fractional_authorships_total_by_year(self):
        return [round(np.sum([j.fractional_authorships_total_by_year[year] for j in self.apc_journals]), 4) for year in self.years]

    @cached_property
    def fractional_authorships_total(self):
        return round(np.mean(self.fractional_authorships_total_by_year), 4)

    def to_dict_apc(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": {
                    "cost_apc_historical_by_year": self.cost_apc_historical_by_year,
                    "cost_apc_historical": self.cost_apc_historical,
                    "cost_apc_historical_gold_by_year": self.cost_apc_historical_gold_by_year,
                    "cost_apc_historical_gold": self.cost_apc_historical_gold,
                    "cost_apc_historical_hybrid_by_year": self.cost_apc_historical_hybrid_by_year,
                    "cost_apc_historical_hybrid": self.cost_apc_historical_hybrid,
                    "num_apc_papers_historical_by_year": self.num_apc_papers_historical_by_year,
                    "num_apc_papers_historical": self.num_apc_papers_historical,
                    "fractional_authorships_total_by_year": self.fractional_authorships_total_by_year,
                    "fractional_authorships_total": self.fractional_authorships_total,
                    "year_historical": self.historical_years_by_year
                    },
                "journals": [j.to_dict() for j in self.apc_journals_sorted_fractional_authorship]
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_fulfillment(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": {
                    "use_unweighted": self.use_actual_unweighted_by_year,
                    "use_weighted": self.use_actual_weighted_by_year,
                    },
                "journals": [j.to_dict_fulfillment() for j in self.journals_sorted_use_total[0:pagesize]],
                "journals_count": len(self.journals),
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_impact(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": {},
                "journals": [j.to_dict_impact() for j in self.journals_sorted_use_total[0:pagesize]],
                "journals_count": len(self.journals),
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_journals(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": {},
                "journals": [j.to_dict() for j in self.journals_sorted_use_total[0:pagesize]],
                "journals_count": len(self.journals),
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_cost(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": {
                    "cost_scenario": self.cost,
                    "cost_bigdeal_projected": self.cost_bigdeal_projected,
                    "cost_percent": self.cost_spent_percent
                },
                "journals": [j.to_dict_cost() for j in self.journals_sorted_use_total[0:pagesize]],
                "journals_count": len(self.journals),
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
                    "use_instant_percent": self.use_instant_percent
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
                "_summary": {
                    "use_instant_percent_by_year": self.use_instant_percent_by_year,
                    "use_instant_percent": self.use_instant_percent
                     },
                "journals": [j.to_dict_timeline() for j in self.journals_sorted_use_total[0:pagesize]],
                "journals_count": len(self.journals),
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_summary(self):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": {
                    "cost_scenario": self.cost,
                    "cost_bigdeal_projected": self.cost_bigdeal_projected,
                    "cost_percent": self.cost_spent_percent,
                    "num_journals_subscribed": len(self.subscribed),
                    "num_journals_total": len(self.journals),
                    "use_instant_percent": self.use_instant_percent
                }
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict_slider(self):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": {
                    "cost_scenario": self.cost,
                    "cost_bigdeal_projected": self.cost_bigdeal_projected,
                    "cost_percent": self.cost_spent_percent,
                    "num_journals_subscribed": len(self.subscribed),
                    "num_journals_total": len(self.journals),
                    "use_instant_percent": self.use_instant_percent,
                },
                "journals": [j.to_dict_slider() for j in self.journals_sorted_cppu_delta],
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def to_dict(self, pagesize):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": {
                    "cost_scenario": self.cost,
                    "cost_bigdeal_projected": self.cost_bigdeal_projected,
                    "cost_percent": self.cost_spent_percent,
                    "num_journals_subscribed": len(self.subscribed),
                    "num_journals_total": len(self.journals),
                    "use_instant_percent_by_year": self.use_instant_percent_by_year,
                    "use_instant_percent": self.use_instant_percent,
                    "use_unweighted": self.use_actual_unweighted_by_year,
                    "use_weighted": self.use_actual_weighted_by_year,
                },
                "journals": [j.to_dict() for j in self.journals_sorted_cppu[0:pagesize]],
                "journals_count": len(self.journals),
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def __repr__(self):
        return u"<{} (n={})>".format(self.__class__.__name__, len(self.journals))



@cache
def get_issn_ls_for_package(package):
    command = "select issn_l from unpaywall_journals_package_issnl_view"
    if package:
        command += " where package='{}'".format(package)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    package_issn_ls = [row["issn_l"] for row in rows]
    return package_issn_ls


@cache
def get_scenario_data_from_db(package):
    timing = []
    section_time = time()

    package_issn_ls = get_issn_ls_for_package(package)

    command = "select issn_l, total from jump_counter where package='{}'".format(package)
    counter_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        counter_rows = cursor.fetchall()
    counter_dict = dict((a["issn_l"], a["total"]) for a in counter_rows)

    timing.append(("time from db: counter", elapsed(section_time, 2)))
    section_time = time()

    command = "select issn_l, embargo from journal_delayed_oa_active"
    embargo_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        embargo_rows = cursor.fetchall()
    embargo_dict = dict((a["issn_l"], int(a["embargo"])) for a in embargo_rows)

    timing.append(("time from db: journal_delayed_oa_active", elapsed(section_time, 2)))
    section_time = time()

    command = """select *
        from jump_citing
        where citing_org = 'University of Virginia' and year < 2019""".format(package)
    citation_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        citation_rows = cursor.fetchall()
    citation_dict = defaultdict(dict)
    for row in citation_rows:
        citation_dict[row["issn_l"]][int(row["year"])] = int(row["num_citations"])

    timing.append(("time from db: citation_rows", elapsed(section_time, 2)))
    section_time = time()

    command = """select *
        from jump_authorship
        where org = 'University of Virginia' and year < 2019""".format(package)
    authorship_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        authorship_rows = cursor.fetchall()
    authorship_dict = defaultdict(dict)
    for row in authorship_rows:
        authorship_dict[row["issn_l"]][int(row["year"])] = int(row["num_authorships"])

    timing.append(("time from db: authorship_rows", elapsed(section_time, 2)))
    section_time = time()

    command = "select * from jump_elsevier_unpaywall_downloads"
    big_view_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        big_view_rows = cursor.fetchall()
    big_view_dict = dict((row["issn_l"], row) for row in big_view_rows)

    timing.append(("time from db: download_rows", elapsed(section_time, 2)))
    section_time = time()

    data = {
        "timing": timing,
        "package_issn_ls": package_issn_ls,
        "counter_dict": counter_dict,
        "embargo_dict": embargo_dict,
        "citation_dict": citation_dict,
        "authorship_dict": authorship_dict,
        "big_view_dict": big_view_dict
    }

    return data


@cache
def get_oa_data_from_db(package):
    command = """select issn_l, year::numeric, fixed.oa_status, count(*) as num_articles 
                    from unpaywall u
                    join unpaywall_updates_view fixed on fixed.doi=u.doi
                    join jump_counter counter on u.journal_issn_l = counter.issn_l
                    where package='{}'
                    and year >= 2014 and year < 2019
                    group by issn_l, year, fixed.oa_status
                    """.format(package)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    my_dict = defaultdict(list)
    for row in rows:
        my_dict[row["issn_l"]] += [row]
    return my_dict

@cache
def get_apc_data_from_db(package):
    command = """select * from jump_apc_authorships
                    """.format(package)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()

    df = pd.DataFrame(rows)
    # df["apc"] = df["apc"].astype(float)
    df["year"] = df["year"].astype(int)
    df["authorship_fraction"] = df.num_authors_from_uni/df.num_authors_total
    df["apc_fraction"] = df["apc"].astype(float) * df["authorship_fraction"]
    df_by_issn_l_and_year = df.groupby(["issn_l", "year"]).apc_fraction.agg([np.size, np.sum]).reset_index().rename(columns={'size': 'num_papers', "sum": "dollars"})

    my_dict = {"df": df, "df_by_issn_l_and_year": df_by_issn_l_and_year}
    return my_dict