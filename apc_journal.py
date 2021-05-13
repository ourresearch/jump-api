# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
from collections import OrderedDict
import pandas as pd

class ApcJournal(object):
    years = range(0, 5)

    def __init__(self, issn_l, scenario_data, df_dict, scenario=None):
        self.issn_l = issn_l
        self.have_data = False
        self.is_in_package = False
        self.subscribed = False
        self.scenario = None
        self.package_id = None
        self.package_currency = None
        if scenario:
            self.scenario = weakref.proxy(scenario)
            self.package_id = self.scenario.package_id
            self.package_currency = self.scenario.my_package.currency
            matching_journal = self.scenario.get_journal(self.issn_l)
            if matching_journal:
                self.is_in_package = True
                self.subscribed = matching_journal.subscribed
        self._scenario_data = scenario_data

        self.my_df_dict = {
            "df_by_issn_l_and_year": df_dict["df_by_issn_l_and_year"].copy(deep=True),
            "df": df_dict["df"].copy(deep=True)
        }

        if self.issn_l in [issn_dict["issn_l"] for issn_dict in self._scenario_data["apc"]]:
            self.have_data = True


    @cached_property
    def my_data_dict(self):
        my_df = self.my_df_dict["df_by_issn_l_and_year"]
        matching_rows_df = my_df.loc[my_df.issn_l == self.issn_l]
        matching_rows_df.set_index("year", inplace=True)
        return matching_rows_df.to_dict('index')

    @cached_property
    def first_df(self):
        my_df = self.my_df_dict["df"]
        matching_rows_df = my_df.loc[my_df.issn_l == self.issn_l]
        return matching_rows_df.iloc[0].to_dict()

    @cached_property
    def title(self):
        return self.first_df["journal_name"]

    @cached_property
    def oa_status(self):
        return self.first_df["oa_status"]

    @cached_property
    def apc_price_display(self):
        if not self.have_data:
            return "unknown"
        return self.apc_price

    @cached_property
    def apc_price(self):
        from journalsdb import get_journal_metadata
        my_journal_metadata = get_journal_metadata(self.issn_l)

        response = None
        if my_journal_metadata:
            response = my_journal_metadata.get_apc_price(self.package_currency)

        return response

    @cached_property
    def num_apc_papers_historical_by_year(self):
        return [round(self.my_data_dict.get(year, defaultdict(int))["num_papers"], 4) for year in self.historical_years_by_year]

    @cached_property
    def cost_apc_historical_by_year(self):
        if not self.apc_price:
            return [None for year in self.historical_years_by_year]
        return [round(self.apc_price * self.my_data_dict.get(year, defaultdict(int))["authorship_fraction"], 4) for year in self.historical_years_by_year]

    @cached_property
    def num_apc_papers_historical(self):
        if not self.have_data:
            return 0
        return round(np.mean(self.num_apc_papers_historical_by_year), 4)

    @cached_property
    def cost_apc_historical(self):
        if not self.have_data:
            return 0
        return round(np.mean(self.cost_apc_historical_by_year), 4)

    @cached_property
    def fractional_authorships_total_by_year(self):
        my_df = self.my_df_dict["df"]
        matching_rows_df = my_df.loc[my_df.issn_l == self.issn_l]
        my_dict_rows = matching_rows_df.to_dict('records')
        by_year = defaultdict(float)
        for my_dict in my_dict_rows:
            by_year[my_dict["year"]] += my_dict["authorship_fraction"]
        return [round(by_year.get(year, 0), 4) for year in self.historical_years_by_year]

    @cached_property
    def fractional_authorships_total(self):
        if not self.have_data:
            return 0
        return round(np.mean(self.fractional_authorships_total_by_year), 4)

    @cached_property
    def historical_years_by_year(self):
        return range(2014, 2019)

    def to_dict(self):
        response = OrderedDict()
        response["meta"] = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": None,
                    "subscribed": self.subscribed,
                    "is_in_package": self.is_in_package
            }
        table_row = OrderedDict()
        table_row["oa_status"] = self.oa_status
        if self.apc_price:
            table_row["apc_price"] = self.apc_price
        else:
            table_row["apc_price"] = None
        table_row["num_apc_papers"] = round(self.num_apc_papers_historical, 1)
        table_row["fractional_authorship"] = round(self.fractional_authorships_total, 1)
        table_row["cost_apc"] = round(self.cost_apc_historical)
        response["table_row"] = table_row
        return response

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.issn_l, self.title)


