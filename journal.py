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
    
    def __init__(self, issn_l, scenario_data, settings):
        self.settings = weakref.proxy(settings)
        self.issn_l = issn_l
        self._scenario_data = scenario_data
        self.subscribed = False

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
    def subscription_cost_2018(self):
        return float(self.my_scenario_data_row["usa_usd"])

    @cached_property
    def papers_2018(self):
        return self.my_scenario_data_row["num_papers_2018"]

    @cached_property
    def num_citations(self):
        return self._scenario_data["citation_dict"].get(self.issn_l, 0)

    @cached_property
    def num_authorships(self):
        return self._scenario_data["authorship_dict"].get(self.issn_l, 0)

    @cached_property
    def oa_embargo_months(self):
        return self._scenario_data["embargo_dict"].get(self.issn_l, None)

    def set_subscribe(self):
        self.subscribed = True

    @cached_property
    def years_by_year(self):
        return [2019 + year_index for year_index in self.years]
    
    @cached_property
    def subscription_cost_2018(self):
        return float(self.my_scenario_data_row["usa_usd"])


    @cached_property
    def subscription_cost_by_year(self):
        response = [((1+self.settings.alacart_cost_increase)**year) *
                                            self.subscription_cost_2018
                                            for year in range(0,5)]
        return response

    @cached_property
    def subscription_cost(self):
        return np.mean(self.subscription_cost_by_year)

    @cached_property
    def subscription_cpu_unweighted(self):
        if not self.use_paywalled:
            return None
        return self.subscription_cost/self.use_paywalled

    @cached_property
    def subscription_cpu_weighted(self):
        # TODO fix
        return self.subscription_cpu_unweighted

    @cached_property
    def use_weighted(self):
        # TODO fix
        return self.use_unweighted

    @cached_property
    def use_unweighted(self):
        response = defaultdict(int)
        for group in self.use_unweighted_by_year:
            response[group] = np.mean(self.use_unweighted_by_year[group])
        return response

    @cached_property
    def use_weighted_by_year(self):
        # TODO fix
        return self.use_unweighted_by_year

    @cached_property
    def use_paywalled(self):
        return np.mean(self.use_paywalled_by_year)

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
        scaled = [max(0, num) for num in scaled]
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
        scaled = [use_total_scaled_by_counter[year] * self.growth_scaling["downloads"][year] for year in self.years]
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
        scaled = [max(0, num) for num in scaled]
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
    def ill_cost(self):
        # TODO make this by year
        return self.use_unweighted["ill"] * self.settings.ill_cost

    @cached_property
    def subscription_minus_ill_cost(self):
        return self.subscription_cost - self.ill_cost

    @cached_property
    def oa_status_history(self):
        return get_oa_history_from_db(self.issn_l)

    def to_dict_details(self):
        response = self.to_dict()
        response["oa_status_history"] = self.oa_status_history
        response["use_unweighted_by_year"] = self.use_unweighted_by_year
        response["use_unweighted"] = self.use_unweighted
        return response


    def to_dict(self):
        return {"issn_l": self.issn_l,
                "title": self.title,
                "num_authorships": self.num_authorships,
                "num_citations": self.num_citations,
                "paywalled_use_unweighted": self.use_paywalled,
                "subscription_cost": self.subscription_cost,
                "ill_cost": self.ill_cost,
                "subject": self.subject,
                "subscription_minus_ill_cost": self.subscription_minus_ill_cost,
                "subscription_cpu_unweighted": self.subscription_cpu_unweighted,
                "subscribed": self.subscribed}

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

