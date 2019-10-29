# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref

class ApcJournal(object):
    years = range(0, 5)

    def __init__(self, issn_l, scenario_data, scenario):
        self.scenario = weakref.proxy(scenario)
        self.settings = self.scenario.settings
        self.issn_l = issn_l
        self._scenario_data = scenario_data
        self.subscribed = False
        dummy = self.to_dict()  # instantiate everything


    @cached_property
    def my_scenario_data_row(self):
        return self._scenario_data["big_view_dict"].get(self.issn_l, defaultdict(int))

    @cached_property
    def title(self):
        return self.my_scenario_data_row["title"]

    @cached_property
    def subject(self):
        return self.my_scenario_data_row["subject"]

    @cached_property
    def publisher(self):
        return self.my_scenario_data_row["publisher"]

    @cached_property
    def cost_subscription_2018(self):
        return float(self.my_scenario_data_row["usa_usd"]) * (1 + self.settings.cost_content_fee_percent)

    @cached_property
    def papers_2018(self):
        return self.my_scenario_data_row["num_papers_2018"]

    @cached_property
    def num_citations_historical_by_year(self):
        my_dict = self._scenario_data["citation_dict"][self.issn_l]
        return [my_dict.get(year, 0) for year in self.historical_years_by_year]

    @cached_property
    def num_citations(self):
        return round(np.mean(self.num_citations_historical_by_year), 4)

    @cached_property
    def historical_years_by_year(self):
        return range(2014, 2019)

    def to_dict(self):
        return {"issn_l": self.issn_l,
                "title": self.title,
                # "apc_price": self.apc_2019,
                # "num_hybrid_articles_historical_by_year": self.num_hybrid_articles_historical_by_year,
                # "num_gold_articles_historical_by_year": self.num_gold_articles_historical_by_year,
                # "fractional_authorships_total_by_year": self.fractional_authorships_total_by_year,
                # "apc_costs_historical_by_year": self.apc_costs_historical_by_year,
                # "apc_costs_historical": self.apc_costs_historical,
                # "year_historical": self.historical_years_by_year
        }

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.issn_l, self.title)


