# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
from collections import OrderedDict

class ApcJournal(object):
    years = list(range(0, 5))

    def __init__(self, issn_l, apc_data, df_dict, currency, package):
        self.issn_l = issn_l
        self.have_data = False
        self.scenario = None
        self.package = package
        self.package_id = None
        self.package_currency = currency

        self.my_df_dict = {
            "df_by_issn_l_and_year": df_dict["df_by_issn_l_and_year"].copy(deep=True),
            "df": df_dict["df"].copy(deep=True)
        }

        if self.issn_l in [issn_dict["issn_l"] for issn_dict in apc_data]:
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
    def journal_metadata(self):
        return self.package.get_journal_metadata(self.issn_l)

    @cached_property
    def issns(self):
        return self.journal_metadata.issns

    @cached_property
    def title(self):
        return self.journal_metadata.title

    @cached_property
    def oa_status(self):
        if self.journal_metadata.is_hybrid:
            return "hybrid"
        if self.journal_metadata.is_gold_journal_in_most_recent_year:
            return "gold"
        return "unknown"

    @cached_property
    def apc_price_display(self):
        if not self.have_data:
            return "unknown"
        return self.apc_price

    @cached_property
    def apc_price(self):
        response = None
        if self.journal_metadata:
            response = self.journal_metadata.get_apc_price(self.package_currency)

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
        return list(range(2014, 2019))

    def to_dict(self):
        response = OrderedDict()
        response["meta"] = {
            "issn_l": self.issn_l,
            "issn_l_prefixed": self.journal_metadata.display_issn_l,
             "title": self.title,
             "issns": self.journal_metadata.display_issns,
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
        return "<{} ({}) {}>".format(self.__class__.__name__, self.issn_l, self.title)


