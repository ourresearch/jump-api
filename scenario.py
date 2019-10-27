# coding: utf-8

from cached_property import cached_property
import numpy as np
import pandas as pd
from collections import defaultdict
import weakref

from app import use_groups
from app import file_cache
from app import get_db_cursor
from time import time
from util import elapsed
from util import for_sorting

from journal import Journal
from assumptions import Assumptions


class ScenarioSummary(object):

    def __init__(self, scenario):
        self.scenario = weakref.proxy(scenario)

    @cached_property
    def use_unweighted_by_year(self):
        use = {}
        for group in use_groups:
            use[group] = [np.sum([journal.use_unweighted_by_year[group][year] for journal in self.scenario.journals]) for year in range(0, 5)]
        return use

    @cached_property
    def use_weighted_by_year(self):
        # TODO fix
        return self.use_unweighted_by_year

    @cached_property
    def use_unweighted(self):
        use = {}
        for group in use_groups:
            use[group] = int(np.mean(self.use_unweighted_by_year[group]))
        return use

    @cached_property
    def use_weighted(self):
        # TODO finish
        return self.use_unweighted

    @cached_property
    def cost_by_group(self):
        # TODO this needs to be redone by year
        cost = {}
        for group in use_groups:
            cost[group] = 0
        # now overwrite for ILL
        cost["ill"] = round(self.use_unweighted["ill"] * self.scenario.settings.cost_ill, 2)
        return cost

    @property
    def cost(self):
        return round(sum([j.cost_subscription for j in self.scenario.journals_sorted_cpu if j.subscribed]), 2)


class Scenario(object):
    def __init__(self, package, http_request_args=None):
        self.settings = Assumptions(http_request_args)
        self.data = get_scenario_data_from_db(package)
        self.journals = [Journal(issn_l, self.data, self) for issn_l in self.data["big_view_dict"]]
        self.summary = ScenarioSummary(self)
        self.timing_messages = []

    @property
    def journals_sorted_cpu(self):
        return sorted(self.journals, key=lambda k: for_sorting(k.cpu_weighted), reverse=False)

    @property
    def journals_sorted_use_total(self):
        return sorted(self.journals, key=lambda k: for_sorting(k.use_total), reverse=True)

    @property
    def subscribed(self):
        return [j for j in self.journals_sorted_cpu if j.subscribed]

    @cached_property
    def num_citations_fuzzed_lookup(self):
        df = pd.DataFrame({"issn_l": [j.issn_l for j in self.journals], "lookup_value": [j.num_citations for j in self.journals]})
        df.ranked = df.lookup_value.rank(method='first')
        return dict(zip(df.issn_l, pd.qcut(df.ranked,  3, labels=["low", "medium", "high"])))

    @cached_property
    def num_authorships_fuzzed_lookup(self):
        df = pd.DataFrame({"issn_l": [j.issn_l for j in self.journals], "lookup_value": [j.num_authorships for j in self.journals]})
        df.ranked = df.lookup_value.rank(method='first')
        return dict(zip(df.issn_l, pd.qcut(df.ranked,  3, labels=["low", "medium", "high"])))

    @cached_property
    def use_total_fuzzed_lookup(self):
        df = pd.DataFrame({"issn_l": [j.issn_l for j in self.journals], "lookup_value": [j.use_total for j in self.journals]})
        df.ranked = df.lookup_value.rank(method='first')
        return dict(zip(df.issn_l, pd.qcut(df.ranked,  3, labels=["low", "medium", "high"])))


    def get_journal(self, issn_l):
        for journal in self.journals:
            if journal.issn_l == issn_l:
                return journal
        return None

    def do_wizardly_things(self, spend):
        my_max = spend/100.0 * self.settings.cost_bigdeal
        my_spend_so_far = 0
        for journal in self.journals_sorted_cpu:
            my_spend_so_far += journal.cost_subscription
            if my_spend_so_far > my_max:
                return
            journal.set_subscribe()

    def to_dict_impact(self, pagesize):
        return {"_timing": self.timing_messages,
                "_settings": self.settings.to_dict(),
                "journals": [j.to_dict_impact() for j in self.journals_sorted_use_total[0:pagesize]],
                "journals_count": len(self.journals),
            }

    def to_dict_report(self, pagesize):
        return {"_timing": self.timing_messages,
                "_settings": self.settings.to_dict(),
                "journals": [j.to_dict_report() for j in self.journals_sorted_use_total[0:pagesize]],
                "journals_count": len(self.journals),
            }

    def to_dict_timeline(self, pagesize):
        return {"_timing": self.timing_messages,
                "_settings": self.settings.to_dict(),
                "journals": [j.to_dict_timeline() for j in self.journals_sorted_use_total[0:pagesize]],
                "journals_count": len(self.journals),
            }

    def to_dict(self, pagesize):
        return {"_timing": self.timing_messages,
                "_settings": self.settings.to_dict(),
                "_summary": {"cost": self.summary.cost,
                            "num_journals_subscribed": len(self.subscribed),
                            "num_journals_total": len(self.journals)},
                "journals": [j.to_dict() for j in self.journals_sorted_cpu[0:pagesize]],
                "by_year": {"use_unweighted": self.summary.use_unweighted_by_year, "use_weighted": self.summary.use_weighted_by_year},
                "annual": {"use_unweighted": self.summary.use_unweighted, "use_weighted": self.summary.use_weighted, "cost": self.summary.cost},
                "journals_count": len(self.journals),
            }

    def __repr__(self):
        return u"<{} (n={})>".format(self.__class__.__name__, len(self.journals))



@file_cache.cache
def get_issn_ls_for_package(package):
    command = "select issn_l from unpaywall_journals_package_issnl_view"
    if package:
        command += " where package='{}'".format(package)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    package_issn_ls = [row["issn_l"] for row in rows]
    return package_issn_ls


@file_cache.cache
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

