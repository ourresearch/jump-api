# coding: utf-8

import os
from cached_property import cached_property
import numpy as np
import pandas as pd
from collections import defaultdict
from collections import OrderedDict
from kids.cache import cache
import simplejson as json

from app import use_groups
from app import get_db_cursor
from app import DEMO_PACKAGE_ID
from app import JISC_PACKAGE_ID
from app import db
from app import logger
from app import memorycache
from app import s3_client

from time import time
from util import elapsed
from util import for_sorting
from util import TimingMessages
from util import get_sql_answer

from journal import Journal
from assumptions import Assumptions
from sqlite_cache import SqLite

sqlite_conn = SqLite(os.getenv("SQLITE_PATH", "sqlite_cache.db"), "sqlitecache.zip")

def get_clean_package_id(http_request_args):
    if not http_request_args:
        return DEMO_PACKAGE_ID
    package_id = http_request_args.get("package", DEMO_PACKAGE_ID)
    if package_id == "demo":
        package_id = DEMO_PACKAGE_ID
    return package_id



def get_fresh_journal_list(scenario, my_jwt):

    from package import Package
    my_package = Package.query.filter(Package.package_id == scenario.package_id).scalar()

    journals_to_exclude = ["0370-2693"]
    issn_ls = list(scenario.data["unpaywall_downloads_dict"].keys())
    issnls_to_build = [issn_l for issn_l in issn_ls if issn_l not in journals_to_exclude]

    # only include things in the counter file
    if my_package.is_demo:
        issnls_to_build = [issn_l for issn_l in issnls_to_build if issn_l in list(scenario.data[DEMO_PACKAGE_ID]["counter_dict"].keys())]
        package_id = DEMO_PACKAGE_ID
    else:
        issnls_to_build = [issn_l for issn_l in issnls_to_build if issn_l in list(scenario.data[scenario.package_id]["counter_dict"].keys())]
        package_id = scenario.package_id

    journals = [Journal(issn_l, package=my_package) for issn_l in issnls_to_build if issn_l]

    for my_journal in journals:
        my_journal.set_scenario(scenario)

    return journals



class Scenario(object):
    years = list(range(0, 5))
    
    def log_timing(self, message):
        self.timing_messages.append("{: <30} {: >6}s".format(message, elapsed(self.section_time, 2)))
        self.section_time = time()
        
    def __init__(self, package_id, http_request_args=None, my_jwt=None):
        self.timing_messages = []
        self.section_time = time()        
        self.package_id = get_clean_package_id({"package": package_id})
        self.package_id_for_db = self.package_id
        if self.package_id.startswith("demo"):
            self.package_id_for_db = DEMO_PACKAGE_ID

        self.log_timing("setup")

        from package import Package
        my_package = Package.query.filter(Package.package_id == self.package_id_for_db).first()
        my_package.unique_issns
        self.publisher_name = my_package.publisher
        self.package_name = my_package.package_name
        my_institution = my_package.institution
        self.institution_name = my_institution.display_name
        self.institution_short_name = my_institution.old_username
        self.institution_id = my_institution.id
        self.my_package = my_package

        self.settings = Assumptions(http_request_args, self.my_package.currency)

        # from app import USE_PAPER_GROWTH
        # if USE_PAPER_GROWTH:
        #     self.data = get_common_package_data(self.package_id_for_db)
        #     self.log_timing("get_common_package_data_ NOT FROM from_cache")
        #     logger.debug("get_common_package_data_NOT FROM from_cache")
        # else:
        #     self.data = get_common_package_data_from_cache(self.package_id_for_db)
        #     self.log_timing("get_common_package_data_from_cache")
        #     # logger.debug("get_common_package_data_from_cache")

        package_id_in_cache = self.package_id_for_db
        if not my_package or my_package.is_demo or package_id == DEMO_PACKAGE_ID:
            package_id_in_cache = DEMO_PACKAGE_ID

        self.data = get_common_package_data(package_id_in_cache, my_package.unique_issns)
        self.log_timing("get_common_package_data from_cache")

        self.set_clean_data()  #order for this one matters, after get common, before build journals
        self.log_timing("set_clean_data")

        self.journals = get_fresh_journal_list(self, my_jwt)
        self.log_timing("mint regular journals")

        [j.set_scenario_data(self.data) for j in self.journals]
        self.log_timing("set data in journals")

        if http_request_args:
            for journal in self.journals:
                if journal.issn_l in http_request_args.get("subrs", []):
                    journal.set_subscribe_bulk()
                if journal.issn_l in http_request_args.get("customSubrs", []):
                    journal.set_subscribe_custom()
        self.log_timing("subscribing to all journals")


    def set_clean_data(self):
        prices_dict = {}
        self.data["prices"] = {}

        use_high_price_if_unknown = False
        if self.package_id.startswith("package-jisc") or self.package_id.startswith("package-n8") or (self.package_id == JISC_PACKAGE_ID):
            use_high_price_if_unknown = True

        from package import get_custom_prices

        prices_dict = {}
        prices_uploaded_raw = get_custom_prices(self.package_id)

        for my_issn_l, my_meta in self.my_package.journal_metadata.items():
            prices_dict[my_issn_l] = prices_uploaded_raw.get(my_issn_l, None)
            if prices_dict[my_issn_l] == None:
                prices_dict[my_issn_l] = my_meta.get_subscription_price(self.my_package.currency, use_high_price_if_unknown=use_high_price_if_unknown)
        self.data["prices"] = prices_dict

        clean_dict = {}
        for issn_l, price_row in self.data["prices"].items():
            include_this_journal = True

            if self.data["prices"][issn_l] == None:
                include_this_journal = False

            if "core_list" in self.data and self.data["core_list"]:
                if issn_l not in list(self.data["core_list"].keys()):
                    include_this_journal = False

            if include_this_journal:
                if issn_l in self.data["unpaywall_downloads_dict_raw"]:
                    clean_dict[issn_l] = self.data["unpaywall_downloads_dict_raw"][issn_l]
                else:
                    clean_dict[issn_l] = None

        self.data["unpaywall_downloads_dict"] = clean_dict

        # remove this
        self.data["perpetual_access"] = get_perpetual_access_from_cache(self.package_id)

        self.data["concepts"] = openalex_best_concepts(self.my_package.unique_issns)


    @property
    def has_custom_perpetual_access(self):
        # perpetual_access_rows = get_perpetual_access_from_cache([self.package_id])
        perpetual_access_rows = get_perpetual_access_from_cache(self.package_id)
        if perpetual_access_rows:
            return True
        from app import suny_consortium_package_ids
        if self.package_id in suny_consortium_package_ids or self.consortium_package_id in suny_consortium_package_ids:
            return True
        return False



    @cached_property
    def journals_sorted_cpu(self):
        self.journals.sort(key=lambda k: for_sorting(k.cpu), reverse=False)
        return self.journals

    @cached_property
    def journals_sorted_use_total(self):
        self.journals.sort(key=lambda k: for_sorting(k.use_total), reverse=True)
        return self.journals

    @cached_property
    def subscribed(self):
        return [j for j in self.journals_sorted_cpu if j.subscribed]

    @cached_property
    def subscribed_bulk(self):
        return [j for j in self.journals_sorted_cpu if j.subscribed_bulk]

    @cached_property
    def subscribed_custom(self):
        return [j for j in self.journals_sorted_cpu if j.subscribed_custom]

    def fuzzed_lookup(self, var):
        df = pd.DataFrame({"issn_l": [j.issn_l for j in self.journals], "lookup_value": [getattr(j, var) for j in self.journals]})
        df["ranked"] = df.lookup_value.rank(method='first', na_option="keep")
        if (len(df) == 1):
            return dict(list(zip(df["issn_l"], "-")))
        return dict(list(zip(df.issn_l, pd.qcut(df.ranked,  3, labels=["low", "medium", "high"]))))

    @cached_property
    def cost_subscription_fuzzed_lookup(self):
        return self.fuzzed_lookup('subscription_cost')

    @cached_property
    def cost_subscription_minus_ill_fuzzed_lookup(self):
        return self.fuzzed_lookup('cost_subscription_minus_ill')

    @cached_property
    def num_citations_fuzzed_lookup(self):
        return self.fuzzed_lookup('num_citations')

    @cached_property
    def num_authorships_fuzzed_lookup(self):
        return self.fuzzed_lookup('num_authorships')

    @cached_property
    def use_total_fuzzed_lookup(self):
        return self.fuzzed_lookup('use_total')

    @cached_property
    def downloads_fuzzed_lookup(self):
        return self.fuzzed_lookup('downloads_total')

    @cached_property
    def cpu_fuzzed_lookup(self):
        return self.fuzzed_lookup('cpu')

    @cached_property
    def cpu_rank_lookup(self):
        df = pd.DataFrame({"issn_l": [j.issn_l for j in self.journals], "lookup_value": [j.cpu for j in self.journals]})
        df["rank"] = df.lookup_value.rank(method='first', na_option="keep")
        return dict(list(zip(df.issn_l, df["rank"])))

    @cached_property
    def old_school_cpu_rank_lookup(self):
        df = pd.DataFrame({"issn_l": [j.issn_l for j in self.journals], "lookup_value": [j.old_school_cpu for j in self.journals]})
        df["rank"] = df.lookup_value.rank(method='first', na_option="keep")
        return dict(list(zip(df.issn_l, df["rank"])))


    @cached_property
    def use_total_by_year(self):
        return [np.sum([journal.use_total_by_year[year] for journal in self.journals]) for year in range(0, 5)]

    @cached_property
    def downloads_total_by_year(self):
        return [np.sum([journal.downloads_total_by_year[year] for journal in self.journals]) for year in range(0, 5)]

    @cached_property
    def use_total(self):
        return 1 + np.sum([journal.use_total for journal in self.journals])

    @cached_property
    def downloads_total(self):
        return np.sum([journal.downloads_total for journal in self.journals])

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
        response = round(sum([j.use_paywalled for j in self.journals if j.use_paywalled]))
        response = max(0, response)
        response = min(response, self.use_total)
        return response

    @cached_property
    def cpu(self):
        if self.use_paywalled:
            return round(self.cost_subscription_minus_ill / self.use_paywalled, 2)
        return None

    @cached_property
    def ill_cost(self):
        return round(sum([j.ill_cost for j in self.journals]))

    @cached_property
    def subscription_cost(self):
        return round(sum([j.subscription_cost for j in self.journals]))

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
    def cost_bigdeal_raw(self):
        if self.my_package and self.my_package.big_deal_cost:
            return float(self.my_package.big_deal_cost)

        big_deal_cost = self.settings.cost_bigdeal
        if isinstance(big_deal_cost, str):
            big_deal_cost = big_deal_cost.replace(",", "")
            big_deal_cost = float(big_deal_cost)

        # from assumptions import DEFAULT_COST_BIGDEAL
        # if big_deal_cost != float(DEFAULT_COST_BIGDEAL):
        #     if self.my_package and self.my_package.big_deal_cost:
        #         return float(self.my_package.big_deal_cost)

        return float(big_deal_cost)


    @cached_property
    def cost_bigdeal_increase_raw(self):
        if self.my_package and self.my_package.big_deal_cost_increase:
            return float(self.my_package.big_deal_cost_increase)

        return self.settings.cost_bigdeal_increase

    @cached_property
    def cost_bigdeal_projected_by_year(self):
        return [round(((1+self.cost_bigdeal_increase_raw/float(100))**year) * self.cost_bigdeal_raw )
                                            for year in self.years]

    @cached_property
    def cost_bigdeal_projected(self):
        response = round(np.mean(self.cost_bigdeal_projected_by_year), 4)
        if response < 1:
            response = 1.0  # avoid div 0 errors
        return response

    @cached_property
    def cost_saved_percent(self):
        return round(100 * float(self.cost_bigdeal_projected - self.cost) / self.cost_bigdeal_projected, 4)

    @cached_property
    def cost_spent_percent(self):
        return round(100 * float(self.cost) / self.cost_bigdeal_projected, 4)

    @cached_property
    def use_instant(self):
        return 1 + np.sum([journal.use_instant for journal in self.journals])

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

        my_spend_so_far = np.sum([j.ill_cost for j in self.journals])

        for journal in self.journals_sorted_cpu:
            if journal.cost_subscription_minus_ill < 0:
                my_spend_so_far += journal.cost_subscription_minus_ill
                journal.set_subscribe_bulk()

        for journal in self.journals_sorted_cpu:
            my_spend_so_far += journal.cost_subscription_minus_ill
            if my_spend_so_far > my_max:
                return
            journal.set_subscribe_bulk()

    @cached_property
    def historical_years_by_year(self):
        return list(range(2014, 2019))

    @cached_property
    def num_citations(self):
        return round(np.sum([j.num_citations for j in self.journals]), 4)

    @cached_property
    def num_authorships(self):
        return round(np.sum([j.num_authorships for j in self.journals]), 4)

    @cached_property
    def num_citations_weight_percent(self):
        return (100*self.settings.weight_citation*self.num_citations)/self.use_total

    @cached_property
    def num_authorships_weight_percent(self):
        return (100*self.settings.weight_authorship*self.num_authorships)/self.use_total

    @cached_property
    def use_social_networks(self):
        return round(np.sum([j.use_social_networks for j in self.journals]))

    @cached_property
    def use_oa(self):
        return round(np.sum([j.use_actual["oa_plus_social_networks"] for j in self.journals]))

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

    def to_dict_details(self):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": self.to_dict_summary_dict(),
                "journals": [j.to_dict_details() for j in self.journals_sorted_cpu],
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

    def to_dict_summary_dict(self):
        response = OrderedDict()
        response["num_journals_subscribed"] =  len(self.subscribed)

        response["cost_scenario_subscription"] = self.cost_actual_subscription

        response["downloads_total"] = self.downloads_total
        response["usage"] = self.use_total
        response["use_oa"] = self.use_oa
        response["use_backfile"] = self.use_backfile
        response["use_subscription"] = self.use_subscription
        response["use_turnaway_and_ill"] = self.use_ill + self.use_other_delayed

        response["use_oa_percent"] = round(float(100)*self.use_oa/self.use_total, 1)
        response["use_backfile_percent"] = round(float(100)*self.use_backfile/self.use_total, 1)
        response["use_subscription_percent"] = self.use_subscription_percent
        response["use_ill_percent"] = self.use_ill_percent

        response["cost_scenario"] = self.cost
        response["cost_bigdeal_projected"] = self.cost_bigdeal_projected
        response["cost_percent"] = self.cost_spent_percent
        response["cost_scenario_ill"] = self.cost_actual_ill
        response["num_journals_total"] = len(self.journals)
        response["use_instant_percent"] = self.use_instant_percent
        response["use_free_instant_percent"] = self.use_free_instant_percent

        return response

    def to_dict(self):
        response = {
                "_settings": self.settings.to_dict(),
                "_summary": self.to_dict_summary_dict(),
                "journals": [j.to_dict() for j in self.journals_sorted_cpu],
                "journals_count": len(self.journals),
            }
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def __repr__(self):
        return "<{} (n={})>".format(self.__class__.__name__, len(self.journals))


@cache
def get_parent_consortium_package_id(package_id):
    q = """select consortium_package_id from jump_account_package where package_id = '{}'""".format(package_id)
    return get_sql_answer(db, q)

@cache
def get_consortium_package_ids(package_id):
    command = "select package_id from jump_account_package where consortium_package_id=%s"
    rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command, (package_id,))
        rows = cursor.fetchall()
    package_ids = [row["package_id"] for row in rows]
    return package_ids

# don't cache because called after loading to get fresh data
def get_counter_journals_by_report_name_from_db(package_id):
    command = """select report_version, report_name, count(distinct issn_l) as num_journals 
        from jump_counter 
        where package_id=%s
        group by report_version, report_name
        """
    with get_db_cursor() as cursor:
        cursor.execute(command, (package_id,))
        rows = cursor.fetchall()
    return rows

# don't cache because called after loading to get fresh data
def get_counter_totals_from_db(package_id):
    counter_dict = defaultdict(int)
    command = """select issn_l, total::float, report_version, report_name, metric_type 
        from jump_counter 
        where package_id=%s
        and (report_name is null or report_name != 'trj4')
        """
    rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command, (package_id,))
        rows = cursor.fetchall()
    if rows:
        is_counter5 = (rows[0]["report_version"] == "5")
        for row in rows:
            if is_counter5:
                if row["report_name"] in ["trj2", "trj3"]:
                    if row["metric_type"] in ["Unique_Item_Requests", "No_License"]:
                        counter_dict[row["issn_l"]] += row.get("total", 0.0)
                # else don't do anything with it for now
            else:
                counter_dict[row["issn_l"]] += row.get("total")
    return counter_dict

# don't cache because called after loading to get fresh data
def get_package_specific_scenario_data_from_db(package_id):
    timing = []
    section_time = time()


    counter_dict = get_counter_totals_from_db(package_id)

    timing.append(("time from db: counter", elapsed(section_time, 2)))
    section_time = time()

    command = """select citing.issn_l, citing.year::int, sum(num_citations) as num_citations
        from jump_citing citing
        join jump_grid_id institution_grid on citing.grid_id = institution_grid.grid_id
        join jump_account_package institution_package on institution_grid.institution_id = institution_package.institution_id
        where citing.year < 2019 
        and institution_package.package_id=%(package_id)s
        and (citing.issn_l in (select distinct issn_l from jump_counter where package_id=%(package_id)s))
        group by citing.issn_l, year"""
    citation_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command, {'package_id': package_id})
        citation_rows = cursor.fetchall()
    citation_dict = defaultdict(dict)
    for row in citation_rows:
        citation_dict[row["issn_l"]][row["year"]] = round(row["num_citations"])

    timing.append(("time from db: citation_rows", elapsed(section_time, 2)))
    section_time = time()

    command = """
        select authorship.issn_l, authorship.year::int, sum(num_authorships) as num_authorships
        from jump_authorship authorship
        join jump_grid_id institution_grid on authorship.grid_id = institution_grid.grid_id
        join jump_account_package institution_package on institution_grid.institution_id = institution_package.institution_id
        where authorship.year < 2019 
        and institution_package.package_id=%(package_id)s
        and (authorship.issn_l in (select distinct issn_l from jump_counter where package_id=%(package_id)s))
        group by authorship.issn_l, year"""
    authorship_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command, {'package_id': package_id})
        authorship_rows = cursor.fetchall()
    authorship_dict = defaultdict(dict)
    for row in authorship_rows:
        authorship_dict[row["issn_l"]][row["year"]] = round(row["num_authorships"])

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
    if input_package_id == DEMO_PACKAGE_ID or input_package_id.startswith("demo"):
        input_package_id = DEMO_PACKAGE_ID
    consortium_package_ids = get_consortium_package_ids(input_package_id)
    if not consortium_package_ids:
        consortium_package_ids = [input_package_id]

    command = "select * from jump_apc_authorships where package_id in %s"
    consortium_package_ids = tuple(consortium_package_ids) # values have to be in a tuple beforehand
    with get_db_cursor(use_realdictcursor=True) as cursor:
        cursor.execute(command, (consortium_package_ids,))
        rows = cursor.fetchall()

    return rows



def get_perpetual_access_from_cache(package_id):
    command = "select * from jump_perpetual_access where package_id=%s"
    with get_db_cursor() as cursor:
        cursor.execute(command, (package_id,))
        rows = cursor.fetchall()
    package_dict = dict([(a["issn_l"], a) for a in rows])
    return package_dict


@cache
def get_core_list_from_db(input_package_id):
    command = "select issn_l, baseline_access from jump_core_journals where package_id=%s"
    with get_db_cursor() as cursor:
        cursor.execute(command, (input_package_id,))
        rows = cursor.fetchall()
    my_dict = dict([(a["issn_l"], a) for a in rows])
    return my_dict

@cache
def load_openalex_best_concepts_from_db(issns):
    start_time = time()
    concepts = {}

    with get_db_cursor() as cursor:
        cursor.execute('select * from openalex_concepts_best where issn_l in %s', (issns,))
        rows = cursor.fetchall()

    for row in rows:
        concepts[row['issn_l']] = {'best': row['best']}

    print(f"loaded openalex best concepts in {elapsed(start_time)} seconds")

    return concepts

def openalex_best_concepts(issns):
    return load_openalex_best_concepts_from_db(tuple(issns))

@cache
def load_openalex_export_concepts_from_db(concepts, issns):
    start_time = time()

    # get top three concepts by score
    with get_db_cursor() as cursor:
        cursor.execute('select * from openalex_concepts_top_three where issn_l in %s', (issns,))
        rows = cursor.fetchall()

    for row in rows:
        concepts[row['issn_l']].update({'top_three': row['top_three']})

    # get all the concepts and their openalex IDs
    with get_db_cursor() as cursor:
        cursor.execute('select issn_l,id_concept_all from openalex_concepts_agg_view where issn_l in %s', (issns,))
        aggrows = cursor.fetchall()

    for aggrow in aggrows:
        concepts[aggrow["issn_l"]].update({'all': aggrow["id_concept_all"]})

    print(f"loaded openalex export concepts in {elapsed(start_time)} seconds")

    return concepts

def openalex_export_concepts(concepts, issns):
    return load_openalex_export_concepts_from_db(concepts, tuple(issns))


def get_embargo_data_from_db(issns):
    qry = "select issn_l, embargo from journal_delayed_oa_active where issn_l in (%s)"
    query = qry % ','.join('?'*len(issns))
    rows = sqlite_conn.select(query, issns)
    embargo_dict = dict((a["issn_l"], round(a["embargo"])) for a in rows)
    return embargo_dict

def get_unpaywall_downloads_from_db(issns):
    qry = "select * from jump_unpaywall_downloads where issn_l in (%s)"
    query = qry % ','.join('?'*len(issns))
    rows = sqlite_conn.select(query, issns)
    unpaywall_downloads_dict = dict((row["issn_l"], dict(row)) for row in rows)
    return unpaywall_downloads_dict

def get_num_papers_from_db(issns):
    qry = "select issn_l, year, num_papers from jump_num_papers where issn_l in (%s)"
    query = qry % ','.join('?'*len(issns))
    rows = sqlite_conn.select(query, issns)
    lookup_dict = defaultdict(dict)
    for row in rows:
        lookup_dict[row["issn_l"]][row["year"]] = row["num_papers"]
    return lookup_dict

def get_oa_recent_data_from_db(issns):
    oa_dict = {}
    for submitted in ["with_submitted", "no_submitted"]:
        for bronze in ["with_bronze", "no_bronze"]:
            key = "{}_{}".format(submitted, bronze)
            qry = f"select * from jump_oa_recent_{key}_precovid where issn_l in (%s)"
            query = qry % ','.join('?'*len(issns))
            rows = sqlite_conn.select(query, issns)
            lookup_dict = defaultdict(list)
            for row in rows:
                lookup_dict[row["issn_l"]] += [dict(row)]
            oa_dict[key] = lookup_dict
    return oa_dict

def get_oa_data_from_db(issns):
    oa_dict = {}
    for submitted in ["with_submitted", "no_submitted"]:
        for bronze in ["with_bronze", "no_bronze"]:
            key = "{}_{}".format(submitted, bronze)

            qry = """select * from jump_oa_{}_precovid	
                        where year_int >= 2015	
                        and issn_l in (%s)
                            """.format(key)
            query = qry % ','.join('?'*len(issns))
            rows = sqlite_conn.select(query, issns)
            lookup_dict = defaultdict(list)
            for row in rows:
                lookup_dict[row["issn_l"]] += [dict(row)]
            oa_dict[key] = lookup_dict
    return oa_dict

def get_society_data_from_db(issns):
    qry = """
        select issn_l, is_society_journal from jump_society_journals_input 
        where is_society_journal is not null
        and issn_l in (%s)
    """
    query = qry % ','.join('?'*len(issns))
    rows = sqlite_conn.select(query, issns)
    lookup_dict = defaultdict(list)
    for row in rows:
        lookup_dict[row["issn_l"]] = row["is_society_journal"]
    return lookup_dict

def get_social_networks_data_from_db(issns):
    qry = "select issn_l, asn_only_rate from jump_mturk_asn_rates where issn_l in (%s)"
    query = qry % ','.join('?'*len(issns))
    rows = sqlite_conn.select(query, issns)
    lookup_dict = {}
    for row in rows:
        lookup_dict[row["issn_l"]] = row["asn_only_rate"]
    return lookup_dict

# not cached on purpose, because components are cached to save space
def get_common_package_data(package_id, issns):
    my_timing = TimingMessages()
    my_data = {}

    (my_data_specific, timing_specific) = get_common_package_data_specific(package_id)
    my_timing.log_timing("LIVE get_common_package_data_specific")
    my_data.update(my_data_specific)
    # my_timing.messages += timing_specific.messages

    if not sqlite_conn.exists():
        sqlite_conn.create()
    (my_data_common, timing_common) = get_common_package_data_for(issns)
    my_timing.log_timing("LIVE get_common_package_data_for")
    my_data.update(my_data_common)
    # my_timing.messages += timing_common.messages

    return my_data

# not cached on purpose, because components are cached to save space
def get_common_package_data_specific(package_id):
    my_timing = TimingMessages()
    my_data = {}

    # package_id specific
    member_package_ids = get_consortium_package_ids(package_id)
    if member_package_ids:
        my_data["member_package_ids"] = member_package_ids
    else:
        my_data["member_package_ids"] = [package_id]
    my_timing.log_timing("get_consortium_package_ids")

    for member_package_id in my_data["member_package_ids"]:
        my_data[member_package_id] = get_package_specific_scenario_data_from_db(member_package_id)
        my_timing.log_timing("get_package_specific_scenario_data_from_db")

    my_data["core_list"] = get_core_list_from_db(package_id)
    my_timing.log_timing("get_core_list_from_db")

    return (my_data, my_timing)

@memorycache
def get_common_package_data_for(issns = None):
    my_timing = TimingMessages()

    start_time = time()
    my_data = {}

    my_data["embargo_dict"] = get_embargo_data_from_db(issns)
    my_timing.log_timing("get_embargo_data_from_db")

    my_data["unpaywall_downloads_dict_raw"] = get_unpaywall_downloads_from_db(issns)
    my_timing.log_timing("get_unpaywall_downloads_from_db")

    my_data["social_networks"] = get_social_networks_data_from_db(issns)
    my_timing.log_timing("get_social_networks_data_from_db")

    my_data["oa_recent"] = get_oa_recent_data_from_db(issns)
    my_timing.log_timing("get_oa_recent_data_from_db")

    my_data["oa"] = get_oa_data_from_db(issns)
    my_timing.log_timing("get_oa_data_from_db")

    my_data["society"] = get_society_data_from_db(issns)
    my_timing.log_timing("get_society_data_from_db")

    my_data["num_papers"] = get_num_papers_from_db(issns)
    my_timing.log_timing("get_num_papers_from_db")

    my_data["_timing_common"] = my_timing.to_dict()
    print("my timing")
    print(my_timing.to_dict())
    print("get_common_package_data_for took {} seconds".format(elapsed(start_time)))

    return (my_data, my_timing)
