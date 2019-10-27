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
    def paywalled_use_unweighted(self):
        return self.use_unweighted["paywalled"]

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
        if not self.paywalled_use_unweighted:
            return None
        return self.subscription_cost/self.paywalled_use_unweighted

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
    def use_unweighted_by_year(self):
        use_unweighted = {}
        total_use = self.my_scenario_data_row["downloads_total"]
        total_use = total_use if total_use else 0
        use_unweighted["total"] = [total_use*self.growth_scaling["downloads"][year] for year in self.years]

        total_oa = self.my_scenario_data_row["downloads_total_oa"]
        total_oa = total_oa if total_oa else 0

        use_unweighted["oa"] = [int(self.oa_recall_scaling_factor * total_oa * self.growth_scaling["oa"][year]) for year in self.years]
    
        use_unweighted["oa"] = [min(a, b) for a, b in zip(use_unweighted["total"], use_unweighted["oa"])]
    
        use_unweighted["social_networks"] = [int(self.social_networks_proportion_of_downloads * use_unweighted["total"][projected_year]) for projected_year in self.years]
    
        total_downloads_by_age = [self.my_scenario_data_row["downloads_{}y".format(age)] for age in self.years]
        total_downloads_by_age = [val if val else 0 for val in total_downloads_by_age]
        oa_downloads_by_age = [self.my_scenario_data_row["downloads_{}y_oa".format(age)] for age in self.years]
        oa_downloads_by_age = [val if val else 0 for val in oa_downloads_by_age]

        use_unweighted["paywalled"] = [0 for year in self.years]
        for year in self.years:
            use_unweighted["paywalled"][year] = (1 - self.social_networks_proportion_of_downloads) *\
                sum([(total_downloads_by_age[age]*self.growth_scaling["downloads"][year] - oa_downloads_by_age[age]*self.growth_scaling["oa"][year])
                     for age in range(0, year+1)])
        use_unweighted["paywalled"] = [max(0, num) for num in use_unweighted["paywalled"]]
    
        use_unweighted["oa"] = [min(use_unweighted["total"][year] - use_unweighted["paywalled"][year], use_unweighted["oa"][year]) for year in self.years]
    
        use_unweighted["backfile"] = [use_unweighted["total"][projected_year]\
                                                        - (use_unweighted["paywalled"][projected_year]
                                                           + use_unweighted["oa"][projected_year]
                                                           + use_unweighted["social_networks"][projected_year])\
                                                        for projected_year in self.years]
        use_unweighted["backfile"] = [max(0, num) for num in use_unweighted["backfile"]]
    
        use_unweighted["ill"] = [int(paywalled*self.settings.ill_request_percent) for paywalled in use_unweighted["paywalled"]]
        use_unweighted["other_delayed"] = [use_unweighted["paywalled"][year] - use_unweighted["ill"][year] for year in self.years]
    
        # now scale for the org
        try:
            total_org_downloads = self._scenario_data["counter_dict"][self.issn_l]
            total_org_downloads_multiple = total_org_downloads / total_use
        except:
            total_org_downloads_multiple = 0
    
        for group in use_groups:
            for projected_year in self.years:
                use_unweighted[group][projected_year] *= float(total_org_downloads_multiple)
                use_unweighted[group][projected_year] = int(use_unweighted[group][projected_year])
        return use_unweighted

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
                "paywalled_use_unweighted": self.paywalled_use_unweighted,
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

