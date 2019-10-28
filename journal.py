# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref

from app import use_groups
from app import file_cache
from app import get_db_cursor




class Journal(object):
    years = range(0, 5)
    oa_recall_scaling_factor = 1.3
    social_networks_proportion_of_downloads = 0.1
    growth_scaling = {
        "downloads": [1.10, 1.21, 1.34, 1.49, 1.65],
        "oa": [1.16, 1.24, 1.57, 1.83, 2.12]
    }
    
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
        return float(self.my_scenario_data_row["usa_usd"])

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
    def num_authorships_historical_by_year(self):
        my_dict = self._scenario_data["authorship_dict"][self.issn_l]
        return [my_dict.get(year, 0) for year in self.historical_years_by_year]

    @cached_property
    def num_authorships(self):
        return round(np.mean(self.num_authorships_historical_by_year), 4)

    @cached_property
    def oa_embargo_months(self):
        return self._scenario_data["embargo_dict"].get(self.issn_l, None)

    def set_subscribe(self):
        self.subscribed = True

    @cached_property
    def years_by_year(self):
        return [2019 + year_index for year_index in self.years]

    @cached_property
    def historical_years_by_year(self):
        # used for citation, authorship lookup
        return range(2014, 2019)

    @cached_property
    def cost_subscription_2018(self):
        return float(self.my_scenario_data_row["usa_usd"])


    @cached_property
    def cost_actual_by_year(self):
        if self.subscribed:
            return self.cost_subscription_by_year
        return self.cost_ill_by_year

    @cached_property
    def cost_actual(self):
        if self.subscribed:
            return self.cost_subscription
        return self.cost_ill


    @cached_property
    def cost_subscription_by_year(self):
        response = [int(((1+self.settings.cost_alacart_increase)**year) * self.cost_subscription_2018)
                                            for year in range(0,5)]
        return response

    @cached_property
    def cost_subscription(self):
        return round(np.mean(self.cost_subscription_by_year), 4)

    @cached_property
    def cpu_unweighted(self):
        if not self.use_paywalled:
            return None
        return round(self.cost_subscription/self.use_paywalled, 6)

    @cached_property
    def cpu_weighted(self):
        # TODO fix
        return self.cpu_unweighted

    @cached_property
    def use_weighted(self):
        # TODO fix
        return self.use_unweighted

    @cached_property
    def use_unweighted(self):
        response = defaultdict(int)
        for group in self.use_unweighted_by_year:
            response[group] = round(np.mean(self.use_unweighted_by_year[group]), 4)
        return response

    @cached_property
    def use_weighted_by_year(self):
        # TODO fix
        return self.use_unweighted_by_year

    @cached_property
    def use_paywalled(self):
        return round(np.mean(self.use_paywalled_by_year), 4)

    @cached_property
    def use_total(self):
        return round(np.mean(self.use_total_by_year), 4)

    @cached_property
    def use_instant(self):
        return round(np.mean(self.use_instant_by_year), 4)

    @cached_property
    def use_instant_by_year(self):
        return [self.use_social_networks_by_year[year] +
                self.use_backfile_by_year[year] +
                self.use_subscription_by_year[year] +
                self.use_oa_by_year[year]
                for year in self.years]

    @cached_property
    def use_subscription_by_year(self):
        # TODO
        return [0 for year in self.years]

    @cached_property
    def use_social_networks_by_year(self):
        return [int(self.social_networks_proportion_of_downloads * self.use_total_by_year[year]) for year in self.years]

    @cached_property
    def use_ill_by_year(self):
        return [int(self.settings.ill_request_percent * self.use_paywalled_by_year[year]) for year in self.years]

    @cached_property
    def use_other_delayed_by_year(self):
        return [self.use_paywalled_by_year[year] - self.use_ill_by_year[year] for year in self.years]

    @cached_property
    def use_backfile_by_year(self):
        scaled = [self.use_total_by_year[year]
              - (self.use_paywalled_by_year[year] + self.use_oa_by_year[year] + self.use_social_networks_by_year[year])
          for year in self.years]
        scaled = [int(max(0, num)) for num in scaled]
        return scaled

    @cached_property
    def use_oa_by_year(self):
        use_oa_before_counter_correction = [self.my_scenario_data_row["downloads_total_oa"] for year in self.years]
        use_oa_before_counter_correction = [val if val else 0 for val in use_oa_before_counter_correction]
        use_oa_scaled_by_counter = [num * self.use_multiplier_from_counter for num in use_oa_before_counter_correction]
        scaled = [int(self.oa_recall_scaling_factor * use_oa_scaled_by_counter[year] * self.growth_scaling["oa"][year]) for year in self.years]
        scaled = [min(self.use_total_by_year[year], scaled[year]) for year in self.years]
        return scaled

    @cached_property
    def use_total_by_year(self):
        use_total_before_counter_correction = [self.my_scenario_data_row["downloads_total"] for year in self.years]
        use_total_before_counter_correction = [val if val else 0 for val in use_total_before_counter_correction]
        use_total_scaled_by_counter = [num * self.use_multiplier_from_counter for num in use_total_before_counter_correction]
        scaled = [int(use_total_scaled_by_counter[year] * self.growth_scaling["downloads"][year]) for year in self.years]
        return scaled

    @cached_property
    def use_paywalled_by_year(self):
        total_use_by_age_before_counter_correction = [self.my_scenario_data_row["downloads_{}y".format(age)] for age in self.years]
        total_use_by_age_before_counter_correction = [val if val else 0 for val in total_use_by_age_before_counter_correction]
        total_use_by_age = [num * self.use_multiplier_from_counter for num in total_use_by_age_before_counter_correction]
        oa_use_by_age_before_counter_correction = [self.my_scenario_data_row["downloads_{}y_oa".format(age)] for age in self.years]
        oa_use_by_age_before_counter_correction = [val if val else 0 for val in oa_use_by_age_before_counter_correction]
        oa_use_by_age = [num * self.use_multiplier_from_counter for num in oa_use_by_age_before_counter_correction]

        scaled = [0 for year in self.years]
        for year in self.years:
            scaled[year] = (1 - self.social_networks_proportion_of_downloads) *\
                sum([(total_use_by_age[age] * self.growth_scaling["downloads"][year] - oa_use_by_age[age] * self.growth_scaling["oa"][year])
                     for age in range(0, year+1)])
        scaled = [int(max(0, num)) for num in scaled]
        return scaled

    @cached_property
    def use_unweighted_by_year(self):
        my_dict = {}
        for group in use_groups:
            my_dict[group] = self.__getattribute__("use_{}_by_year".format(group))
        return my_dict

    @cached_property
    def use_total_before_counter_correction(self):
        return self.my_scenario_data_row["downloads_total"]

    @cached_property
    def use_multiplier_from_counter(self):
        try:
            counter_for_this_journal = self._scenario_data["counter_dict"][self.issn_l]
            counter_multiplier = float(counter_for_this_journal) / self.use_total_before_counter_correction
        except:
            counter_multiplier = float(0)
        return counter_multiplier


    @cached_property
    def cost_ill(self):
        return round(np.mean(self.cost_ill_by_year), 4)

    @cached_property
    def cost_ill_by_year(self):
        return [round(self.use_ill_by_year[year] * self.settings.cost_ill, 4) for year in self.years]

    @cached_property
    def cost_subscription_minus_ill(self):
        return round(self.cost_subscription - self.cost_ill, 4)

    @cached_property
    def oa_status_history(self):
        return get_oa_history_from_db(self.issn_l)

    @cached_property
    def use_total_fuzzed(self):
        return self.scenario.use_total_fuzzed_lookup[self.issn_l]

    @cached_property
    def num_authorships_fuzzed(self):
        return self.scenario.num_authorships_fuzzed_lookup[self.issn_l]

    @cached_property
    def num_citations_fuzzed(self):
        return self.scenario.num_citations_fuzzed_lookup[self.issn_l]

    @cached_property
    def num_papers(self):
        return self.papers_2018

    @cached_property
    def use_instant_percent(self):
        if not self.use_total:
            return None
        return round(float(self.use_instant) / self.use_total, 4)

    @cached_property
    def use_instant_percent_by_year(self):
        if not self.use_total:
            return None
        return [round(float(self.use_instant_by_year[year]) / self.use_total_by_year[year], 4) if self.use_total_by_year[year] else None for year in self.years]

    def to_dict_report(self):
        response = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed,
                    "use_total_fuzzed": self.use_total_fuzzed,
                    "num_authorships_fuzzed": self.num_authorships_fuzzed,
                    "num_citations_fuzzed": self.num_citations_fuzzed,
                    "num_papers": self.num_papers,
                    "use_instant_percent": self.use_instant_percent,
                    "use_instant_percent_by_year": self.use_instant_percent_by_year,
                    "oa_embargo_months": self.oa_embargo_months,
        }
        return response

    def to_dict_impact(self):
        response = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed,
                    "year": self.years_by_year,
                    "year_historical": self.historical_years_by_year,
                    "num_citations_historical_by_year": self.num_citations_historical_by_year,
                    "num_authorships_historical_by_year": self.num_authorships_historical_by_year,
                    "use_total_by_year": self.use_total_by_year
        }
        return response


    def to_dict_timeline(self):
        response = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed,
                    "year": self.years_by_year,
                    "year_historical": self.historical_years_by_year,
                    "oa_embargo_months": self.oa_embargo_months,
                    "cost_actual_by_year": self.cost_actual_by_year
        }
        for k, v in self.__dict__.iteritems():
            if k.endswith("by_year") and k not in ["use_unweighted_by_year", "years_by_year", "historical_years_by_year"]:
                response[k] = v
        # make sure we don't miss these because they haven't been initialized
        for group in use_groups:
            field = "use_{}_by_year".format(group)
            response[field] = self.__getattribute__(field)
        return response

    def to_dict_details(self):
        response = self.to_dict()
        response["oa_status_history"] = self.oa_status_history
        response["use_unweighted_by_year"] = self.use_unweighted_by_year
        response["use_unweighted"] = self.use_unweighted
        response["oa_embargo_months"] = self.oa_embargo_months
        return response


    def to_dict(self):
        return {"issn_l": self.issn_l,
                "title": self.title,
                "subject": self.subject,
                "num_authorships": self.num_authorships,
                "num_citations": self.num_citations,
                "use_paywalled_unweighted": self.use_paywalled,
                "cost_subscription": self.cost_subscription,
                "cost_ill": self.cost_ill,
                "cost_subscription_minus_ill": self.cost_subscription_minus_ill,
                "cpu_unweighted": self.cpu_unweighted,
                "subscribed": self.subscribed
                }

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.issn_l, self.title)




@file_cache.cache
def get_oa_history_from_db(issn_l):
    command = """select year::numeric, oa_status, count(*) as num_articles from unpaywall 
        where journal_issn_l = '{}'
        and year > 2015
        group by year, oa_status""".format(issn_l)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    return rows



# observation_year 	total views 	total views percent of 2018 	total oa views 	total oa views percent of 2018
# 2018 	25,565,054.38 	1.00 	12,664,693.62 	1.00
# 2019 	28,162,423.76 	1.10 	14,731,000.96 	1.16
# 2020 	30,944,070.68 	1.21 	17,033,520.59 	1.34
# 2021 	34,222,756.60 	1.34 	19,830,049.25 	1.57
# 2022 	38,000,898.80 	1.49 	23,092,284.75 	1.82
# 2023 	42,304,671.82 	1.65 	26,895,794.03 	2.12

