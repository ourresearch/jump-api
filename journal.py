# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
from kids.cache import cache
from collections import OrderedDict

from app import use_groups
from app import use_groups_free_instant
from app import use_groups_lookup
from app import get_db_cursor
from util import format_currency
from util import format_percent
from util import format_with_commas


class Journal(object):
    years = range(0, 5)
    growth_scaling = {
        "downloads": [1.0 for year in range(0, 5)],
        "oa": [1.0 for year in range(0, 5)],
        # "downloads": [1.10, 1.21, 1.34, 1.49, 1.65],
        # "oa": [1.16, 1.24, 1.57, 1.83, 2.12]
    }
    
    def __init__(self, issn_l, scenario_data=None, scenario=None):
        self.set_scenario(scenario)
        self.set_scenario_data(scenario_data)
        self.issn_l = issn_l
        self.subscribed = False
        self.use_default_download_curve = False

    def set_scenario(self, scenario):
        if scenario:
            self.scenario = weakref.proxy(scenario)
            self.settings = self.scenario.settings
        else:
            self.scenario = None
            self.settings = None

    def set_scenario_data(self, scenario_data):
        self._scenario_data = scenario_data

    @property
    def my_scenario_data_row(self):
        return self._scenario_data["big_view_dict"][self.issn_l]

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
        return float(self.my_scenario_data_row["usa_usd"]) * (1 + self.settings.cost_content_fee_percent/float(100))

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
        # invalidate cache
        for key in self.__dict__:
            if "actual" in key:
                del self.__dict__[key]

    def set_unsubscribe(self):
        self.subscribed = False
        # invalidate cache
        for key in self.__dict__:
            if "actual" in key:
                del self.__dict__[key]

    @cached_property
    def years_by_year(self):
        return [2019 + year_index for year_index in self.years]

    @cached_property
    def historical_years_by_year(self):
        # used for citation, authorship lookup
        return range(2015, 2019+1)

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
        response = [round(((1+self.settings.cost_alacart_increase/float(100))**year) * self.cost_subscription_2018 )
                                            for year in self.years]
        return response

    @cached_property
    def cost_subscription(self):
        return round(np.mean(self.cost_subscription_by_year), 4)

    @cached_property
    def cppu_downloads(self):
        if not self.downloads_paywalled:
            return None
        return round(self.cost_subscription/self.downloads_paywalled, 6)

    @cached_property
    def cppu_use(self):
        if not self.use_paywalled:
            return None
        return round(self.cost_subscription/self.use_paywalled, 6)

    @cached_property
    def cppu_use_delta(self):
        if not self.use_paywalled:
            return None
        return round(self.cost_subscription_minus_ill/self.use_paywalled, 6)


    @cached_property
    def use_weight_multiplier(self):
        if not self.downloads_total:
            return 1.0
        return float(self.use_total) / self.downloads_total


    @cached_property
    def use_instant_by_year(self):
        response = [0 for year in self.years]
        for group in use_groups_free_instant + ["subscription"]:
            for year in self.years:
                response[year] += self.use_actual_by_year[group][year]
        return response

    @cached_property
    def use_instant(self):
        return round(np.mean(self.use_instant_by_year), 4)


    @cached_property
    def downloads_subscription_by_year(self):
        return self.downloads_paywalled_by_year

    @cached_property
    def downloads_subscription(self):
        return self.downloads_paywalled

    @cached_property
    def use_subscription(self):
        return self.use_paywalled

    @cached_property
    def downloads_social_network_multiplier(self):
        if self.settings.include_social_networks:
            return self._scenario_data["social_networks"].get(self.issn_l, 0)
        else:
            return 0.0

    @cached_property
    def downloads_social_networks_by_year(self):
        response = [self.downloads_total_by_year[year] * self.downloads_social_network_multiplier for year in self.years]
        response = [min(response[year], self.downloads_total_by_year[year] - self.downloads_oa_by_year[year]) for year in self.years]
        response = [max(response[year], 0) for year in self.years]
        return response

    @cached_property
    def downloads_social_networks(self):
        return round(np.mean(self.downloads_social_networks_by_year), 4)


    @cached_property
    def use_social_networks(self):
        return round(round(self.downloads_social_networks * self.use_weight_multiplier), 4)


    @cached_property
    def downloads_ill_by_year(self):
        response = [round(self.settings.ill_request_percent_of_delayed/float(100) * self.downloads_paywalled_by_year[year]) for year in self.years]
        response = [num if num else 0 for num in response]
        return response


    @cached_property
    def downloads_ill(self):
        return round(np.mean(self.downloads_ill_by_year), 4)

    @cached_property
    def use_ill(self):
        return round(round(self.downloads_ill * self.use_weight_multiplier), 4)


    @cached_property
    def downloads_other_delayed_by_year(self):
        return [self.downloads_paywalled_by_year[year] - self.downloads_ill_by_year[year] for year in self.years]

    @cached_property
    def downloads_other_delayed(self):
        return round(np.mean(self.downloads_other_delayed_by_year), 4)

    @cached_property
    def use_other_delayed(self):
        return round(round(self.downloads_other_delayed * self.use_weight_multiplier), 4)


    @cached_property
    def downloads_backfile_by_year(self):
        if self.settings.include_backfile:
            scaled = [0 for year in self.years]
            for year in self.years:
                age = year
                new = 0.5 * ((self.downloads_by_age[age] * self.growth_scaling["downloads"][year]) - (self.downloads_oa_by_age[year][age] * self.growth_scaling["oa"][year]))
                scaled[year] = max(new, 0)
                for age in range(year+1, 5):
                    by_age = (self.downloads_by_age[age] * self.growth_scaling["downloads"][year]) - (self.downloads_oa_by_age[year][age] * self.growth_scaling["oa"][year])
                    by_age += max(new, 0)
                scaled[year] += by_age
                if scaled[year]:
                    scaled[year] += self.downloads_total_older_than_five_years
                scaled[year] -= self.downloads_social_networks_by_year[year]
            scaled = [round(max(0, num)) for num in scaled]
            return scaled
        else:
            return [0 for year in self.years]


    @cached_property
    def downloads_backfile(self):
        return round(np.mean(self.downloads_backfile_by_year), 4)

    @cached_property
    def use_backfile(self):
        return round(round(self.downloads_backfile * self.use_weight_multiplier), 4)

    @cached_property
    def num_oa_historical_by_year(self):
        # print "num_oa_historical_by_year", self.num_papers, [self.num_green_historical_by_year[year]+self.num_bronze_historical_by_year[year]+self.num_hybrid_historical_by_year[year] for year in self.years]
        # print "parts", self.num_papers
        # print "green", self.num_green_historical_by_year
        # print "bronze", self.num_bronze_historical_by_year
        # print "hybrid", self.num_hybrid_historical_by_year

        return [self.num_green_historical_by_year[year]+self.num_bronze_historical_by_year[year]+self.num_hybrid_historical_by_year[year] for year in self.years]

    @cached_property
    def num_oa_historical(self):
        return round(np.mean(self.num_oa_historical_by_year), 4)

    @cached_property
    def num_oa_by_year(self):
        # TODO add some growth
        return [self.num_oa_historical for year in self.years]

    @cached_property
    def downloads_oa(self):
        return round(np.sum([self.num_oa_for_convolving[age] * self.downloads_per_paper_by_age[age] for age in self.years]), 4)

    @cached_property
    def downloads_oa_by_year(self):
        # TODO add some growth by using num_oa_by_year instead of num_oa_by_year_historical
        response = [self.downloads_oa for year in self.years]
        return response

    @cached_property
    def use_oa(self):
        # return round(self.downloads_oa * self.use_weight_multiplier, 4)
        return self.use_oa_green + self.use_oa_bronze + self.use_oa_hybrid

    @cached_property
    def use_oa_by_year(self):
        # just making this stable prediction over next years
        # TODO fix
        return [self.use_oa for year in self.years]


    @cached_property
    def downloads_total_by_year(self):
        scaled = [round(self.downloads_scaled_by_counter_by_year[year] * self.growth_scaling["downloads"][year]) for year in self.years]

        return scaled

    @cached_property
    def downloads_total(self):
        return round(np.mean(self.downloads_total_by_year), 4)



    # used to calculate use_weight_multiplier so it can't use it
    @cached_property
    def use_total_by_year(self):
        return [round(self.downloads_total_by_year[year] + self.use_addition_from_weights) for year in self.years]

    @cached_property
    def use_total(self):
        response = round(np.mean(self.use_total_by_year), 4)
        if response == 0:
            response = 0.0001
        return response



    @cached_property
    def downloads_by_age(self):
        use_default_curve = False

        total_downloads_by_age_before_counter_correction = [self.my_scenario_data_row["downloads_{}y".format(age)] for age in self.years]
        total_downloads_by_age_before_counter_correction = [val if val else 0 for val in total_downloads_by_age_before_counter_correction]

        sum_total_downloads_by_age_before_counter_correction = np.sum(total_downloads_by_age_before_counter_correction)

        download_curve_diff = np.array(total_downloads_by_age_before_counter_correction)

        # should mostly be strictly negative slope.  if it is very positive, use default instead
        if np.max(download_curve_diff) > 0.075:
            self.use_default_download_curve = True

        if sum_total_downloads_by_age_before_counter_correction < 25:
            self.use_default_download_curve = True

        if self.use_default_download_curve:
            # from future of OA paper, modified to be just elsevier, all colours
            default_download_by_age = [0.371269, 0.137739, 0.095896, 0.072885, 0.058849]
            total_downloads_by_age_before_counter_correction = [num*sum_total_downloads_by_age_before_counter_correction for num in default_download_by_age]

        downloads_by_age = [num * self.downloads_counter_multiplier for num in total_downloads_by_age_before_counter_correction]
        return downloads_by_age

    @cached_property
    def downloads_total_older_than_five_years(self):
        return self.downloads_total - np.sum(self.downloads_by_age)

    @cached_property
    def downloads_per_paper_by_age(self):
        # TODO do separately for each type of OA
        # print [[float(num), self.num_papers, self.num_oa_historical] for num in self.downloads_by_age]

        return [float(num)/self.num_papers for num in self.downloads_by_age]

    @cached_property
    def downloads_scaled_by_counter_by_year(self):
        # TODO is flat right now
        downloads_total_before_counter_correction_by_year = [max(1.0, self.my_scenario_data_row["downloads_total"]) for year in self.years]
        downloads_total_before_counter_correction_by_year = [val if val else 0.0 for val in downloads_total_before_counter_correction_by_year]
        downloads_total_scaled_by_counter = [num * self.downloads_counter_multiplier for num in downloads_total_before_counter_correction_by_year]
        return downloads_total_scaled_by_counter

    @cached_property
    def downloads_per_paper(self):
        per_paper = float(self.downloads_scaled_by_counter_by_year)/self.num_papers
        return per_paper



    @cached_property
    def num_oa_for_convolving(self):
        oa_in_order = self.num_oa_historical_by_year
        # oa_in_order.reverse()
        # print "\nself.num_oa_historical_by_year", self.num_papers, oa_in_order
        return [min(self.num_papers, self.num_oa_historical_by_year[year]) for year in self.years]

    @cached_property
    def downloads_oa_by_age(self):
        # TODO do separately for each type of OA and each year
        response = {}
        for year in self.years:
            response[year] = [(float(self.downloads_per_paper_by_age[age])*self.num_oa_for_convolving[age]) for age in self.years]
            if self.oa_embargo_months:
                for age in self.years:
                    if age*12 >= self.oa_embargo_months:
                        response[year][age] = self.downloads_by_age[age]
        return response

    @cached_property
    def downloads_paywalled_by_year(self):
        scaled = [self.downloads_total_by_year[year]
              - (self.downloads_backfile_by_year[year] + self.downloads_oa_by_year[year] + self.downloads_social_networks_by_year[year])
          for year in self.years]
        scaled = [round(max(0, num)) for num in scaled]
        return scaled

    @cached_property
    def downloads_paywalled(self):
        return round(np.mean(self.downloads_paywalled_by_year), 4)

    @cached_property
    def use_paywalled(self):
        return round(round(self.downloads_paywalled * self.use_weight_multiplier), 4)

    @cached_property
    def downloads_counter_multiplier_normalized(self):
        return round(self.downloads_counter_multiplier / self.scenario.downloads_counter_multiplier, 4)

    @cached_property
    def use_weight_multiplier_normalized(self):
        return round(self.use_weight_multiplier / self.scenario.use_weight_multiplier, 4)

    @cached_property
    def downloads_actual(self):
        response = defaultdict(int)
        for group in self.downloads_actual_by_year:
            response[group] = round(np.mean(self.downloads_actual_by_year[group]), 4)
        return response

    @cached_property
    def use_actual(self):
        response = defaultdict(int)
        for group in self.use_actual_by_year:
            response[group] = round(np.mean(self.use_actual_by_year[group]), 4)
        return response

    @cached_property
    def downloads_actual_by_year(self):
        #initialize
        my_dict = {}
        for group in use_groups:
            my_dict[group] = [0 for year in self.years]

        # true regardless
        for group in use_groups_free_instant + ["total"]:
            my_dict[group] = self.__getattribute__("downloads_{}_by_year".format(group))

        # depends
        if self.subscribed:
            my_dict["subscription"] = self.downloads_subscription_by_year
        else:
            my_dict["ill"] = self.downloads_ill_by_year
            my_dict["other_delayed"] = self.downloads_other_delayed_by_year

        return my_dict

    @cached_property
    def use_actual_by_year(self):
        my_dict = {}
        for group in self.downloads_actual_by_year:
            my_dict[group] = [round(num * self.use_weight_multiplier) for num in self.downloads_actual_by_year[group]]
        return my_dict

    @cached_property
    def downloads_total_before_counter_correction(self):
        return max(1, self.my_scenario_data_row["downloads_total"])

    @cached_property
    def use_addition_from_weights(self):
        # using the average on purpose... by year too rough
        weights_addition = float(self.settings.weight_citation) * self.num_citations
        weights_addition += float(self.settings.weight_authorship) * self.num_authorships
        weights_addition = round(weights_addition, 4)
        return weights_addition

    @cached_property
    def downloads_counter_multiplier(self):
        try:
            counter_for_this_journal = self._scenario_data["counter_dict"][self.issn_l]
            counter_multiplier = float(counter_for_this_journal) / self.downloads_total_before_counter_correction
        except:
            counter_multiplier = float(0)
        return counter_multiplier


    @cached_property
    def cost_ill(self):
        return round(np.mean(self.cost_ill_by_year), 4)

    @cached_property
    def cost_ill_by_year(self):
        return [round(self.downloads_ill_by_year[year] * self.settings.cost_ill, 4) for year in self.years]

    @cached_property
    def cost_subscription_minus_ill_by_year(self):
        return [self.cost_subscription_by_year[year] - self.cost_ill_by_year[year] for year in self.years]

    @cached_property
    def cost_subscription_minus_ill(self):
        return round(self.cost_subscription - self.cost_ill, 4)

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
            return 0
        return round(100 * float(self.use_instant) / self.use_total, 4)

    @cached_property
    def use_instant_percent_by_year(self):
        if not self.downloads_total:
            return 0
        return [round(100 * float(self.use_instant_by_year[year]) / self.use_total_by_year[year], 4) if self.use_total_by_year[year] else None for year in self.years]



    @cached_property
    def num_oa_papers_multiplier(self):
        oa_adjustment_dict = self._scenario_data["oa_adjustment"].get(self.issn_l, None)
        if not oa_adjustment_dict:
            return 1.0
        if not oa_adjustment_dict["unpaywall_measured_fraction_3_years_oa"]:
            return 1.0
        response = float(oa_adjustment_dict["mturk_max_oa_rate"]) / (oa_adjustment_dict["unpaywall_measured_fraction_3_years_oa"])
        # print "num_oa_papers_multiplier", response, float(oa_adjustment_dict["mturk_max_oa_rate"]), (oa_adjustment_dict["unpaywall_measured_fraction_3_years_oa"])
        return response

    def get_oa_data(self, only_peer_reviewed=False):
        if only_peer_reviewed:
            submitted = "no_submitted"
        else:
            if self.settings.include_submitted_version:
                submitted = "with_submitted"
            else:
                submitted = "no_submitted"

        if self.settings.include_bronze:
            bronze = "with_bronze"
        else:
            bronze = "no_bronze"

        my_dict = defaultdict(dict)

        key = u"{}_{}".format(submitted, bronze)
        my_rows = self._scenario_data["oa"][key][self.issn_l]
        my_recent_rows = self._scenario_data["oa_recent"][key][self.issn_l]

        for row in my_rows:
            my_dict[row["fresh_oa_status"]][round(row["year_int"])] = round(row["count"])
            # my_dict[row["fresh_oa_status"]][round(row["year_int"])] = round(row["count"]) * self.num_oa_papers_multiplier

        for row in my_recent_rows:
            my_dict[row["fresh_oa_status"]][2019] = round(row["count"])
            # my_dict[row["fresh_oa_status"]][round(row["year_int"])] = round(row["count"]) * self.num_oa_papers_multiplier

        # print my_dict
        return my_dict


    @cached_property
    def num_green_historical_by_year(self):
        my_dict = self.get_oa_data()["green"]
        return [my_dict.get(year, 0) for year in self.historical_years_by_year]

    @cached_property
    def num_green_historical(self):
        return round(np.mean(self.num_green_historical_by_year), 4)

    @cached_property
    def downloads_oa_green(self):
        return round(np.sum([self.num_green_historical * self.downloads_per_paper_by_age[age] for age in self.years]), 4)

    @cached_property
    def use_oa_green(self):
        return round(self.downloads_oa_green * self.use_weight_multiplier, 4)


    @cached_property
    def num_hybrid_historical_by_year(self):
        my_dict = self.get_oa_data()["hybrid"]
        return [my_dict.get(year, 0) for year in self.historical_years_by_year]

    @cached_property
    def num_hybrid_historical(self):
        return round(np.mean(self.num_hybrid_historical_by_year), 4)

    @cached_property
    def downloads_oa_hybrid(self):
        return round(np.sum([self.num_hybrid_historical * self.downloads_per_paper_by_age[age] for age in self.years]), 4)

    @cached_property
    def use_oa_hybrid(self):
        return round(self.downloads_oa_hybrid * self.use_weight_multiplier, 4)


    @cached_property
    def num_bronze_historical_by_year(self):
        my_dict = self.get_oa_data()["bronze"]
        response = [my_dict.get(year, 0) for year in self.historical_years_by_year]
        if self.oa_embargo_months:
            for age in self.years:
                if age*12 < self.oa_embargo_months:
                    response[age] = 0
        return response

    @cached_property
    def num_bronze_historical(self):
        return round(np.mean(self.num_bronze_historical_by_year), 4)

    @cached_property
    def downloads_oa_bronze(self):
        return round(np.sum([self.num_bronze_historical * self.downloads_per_paper_by_age[age] for age in self.years]), 4)


    @cached_property
    def use_oa_bronze(self):
        return round(self.downloads_oa_bronze * self.use_weight_multiplier, 4)


    @cached_property
    def num_peer_reviewed_historical_by_year(self):
        my_dict = self.get_oa_data(only_peer_reviewed=True)
        response = defaultdict(int)
        for oa_type in my_dict:
            for year in self.historical_years_by_year:
                response[year] += my_dict[oa_type].get(year, 0)
        return [response[year] for year in self.historical_years_by_year]

    @cached_property
    def num_peer_reviewed_historical(self):
        return round(np.mean(self.num_peer_reviewed_historical_by_year), 4)

    @cached_property
    def downloads_oa_peer_reviewed(self):
        return round(np.sum([self.num_peer_reviewed_historical * self.downloads_per_paper_by_age[age] for age in self.years]), 4)

    @cached_property
    def use_oa_peer_reviewed(self):
        return round(self.downloads_oa_peer_reviewed * self.use_weight_multiplier, 4)



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
        response = OrderedDict()
        response["meta"] = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed}
        table_row = {}
        table_row["total_usage"] = round(self.use_total)
        table_row["downloads"] = round(self.downloads_total)
        table_row["citations"] = round(self.num_citations, 1)
        table_row["authorships"] = round(self.num_authorships, 1)
        response["table_row"] = table_row
        return response

    def to_dict_overview(self):
        response = OrderedDict()
        response["meta"] = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed}
        table_row = {}
        if self.cppu_use:
            table_row["cppu"] = self.cppu_use
        else:
            table_row["cppu"] = "no paywalled usage"
        table_row["use"] = self.use_total
        table_row["value"] = self.use_instant_percent
        table_row["cost"] = self.cost_actual
        response["table_row"] = table_row
        return response

    def to_dict_cost(self):
        response = OrderedDict()
        response["meta"] = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed}
        table_row = {}
        table_row["scenario_cost"] = round(self.cost_actual)
        table_row["real_cost"] = round(self.cost_subscription_minus_ill)
        table_row["ill_cost"] = round(self.cost_ill)
        table_row["subscription_cost"] = round(self.cost_subscription)
        if self.cppu_use:
            table_row["cppu"] = round(self.cppu_use, 2)
        else:
            table_row["cppu"] = "no paywalled usage"
        response["table_row"] = table_row
        return response


    def to_dict_timeline(self):
        response = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed,
                    "year": self.years_by_year,
                    "year_historical": self.historical_years_by_year,
                    "oa_embargo_months": self.oa_embargo_months,
                    "cost_actual_by_year": self.cost_actual_by_year,
                    "use_total_by_year": self.use_total_by_year
        }
        for k, v in vars(self).iteritems():
            if k.endswith("by_year") and not k.endswith("years_by_year") and ("weighted_by_year" not in k):
                response[k] = v
        # make sure we don't miss these because they haven't been initialized
        for group in use_groups:
            field = "use_{}_by_year".format(group)
            response[field] = self.__getattribute__(field)
        return response

    def to_dict_details(self):
        response = OrderedDict()

        response["top"] = {
                "issn_l": self.issn_l,
                "title": self.title,
                "subject": self.subject,
                "publisher": self.publisher,
                "is_society_journal": False,  ## todo
                "subscribed": self.subscribed,
                "num_papers": self.num_papers,
                "cost_subscription": format_currency(self.cost_subscription),
                "cost_ill": format_currency(self.cost_ill),
                "cost_actual": format_currency(self.cost_actual),
                "cost_subscription_minus_ill": format_currency(self.cost_subscription_minus_ill),
                "cppu": format_currency(self.cppu_use, True),
                "use_instant_percent": self.use_instant_percent,
                "api_journal_raw_default_settings": "https://unpaywall-jump-api.herokuapp.com/journal/issn_l/{}?email=YOUR_EMAIL_ADDRESS".format(self.issn_l)
        }

        group_list = []
        for group in use_groups:
            group_dict = OrderedDict()
            group_dict["group"] = use_groups_lookup[group]["display"]
            group_dict["usage"] = format_with_commas(round(self.use_actual[group]))
            group_dict["usage_percent"] = format_percent(round(float(100)*self.use_actual[group]/self.use_total))
            # group_dict["timeline"] = u",".join(["{:>7}".format(self.use_actual_by_year[group][year]) for year in self.years])
            for year in self.years:
                group_dict["year_"+str(2020 + year)] = format_with_commas(round(self.use_actual_by_year[group][year]))
            group_list += [group_dict]
        response["fulfillment"] = {
            "headers": [
                {"text": "Type", "value": "group"},
                {"text": "Usage (projected annual)", "value": "usage"},
                {"text": "Usage (percent)", "value": "usage_percent"},
                {"text": "Usage projected 2020", "value": "year_2020"},
                {"text": "2021", "value": "year_2021"},
                {"text": "2022", "value": "year_2022"},
                {"text": "2023", "value": "year_2023"},
                {"text": "2024", "value": "year_2024"},
            ],
            "data": group_list
            }
        response["fulfillment"]["use_actual_by_year"] = self.use_actual_by_year
        response["fulfillment"]["downloads_per_paper_by_age"] = self.downloads_per_paper_by_age

        oa_list = []
        for oa_type in ["green", "hybrid", "bronze"]:
            oa_dict = OrderedDict()
            use = self.__getattribute__("use_oa_{}".format(oa_type))
            oa_dict["oa_status"] = oa_type.title()
            oa_dict["num_papers"] = round(self.__getattribute__("num_{}_historical".format(oa_type)))
            oa_dict["usage"] = format_with_commas(use)
            oa_dict["usage_percent"] = format_percent(round(float(100)*use/self.use_total))
            oa_list += [oa_dict]
        oa_list += [OrderedDict([("oa_status", "*Total*"),
                                ("num_papers", round(self.num_oa_historical)),
                                ("usage", format_with_commas(self.use_oa)),
                                ("usage_percent", format_percent(round(100*float(self.use_oa)/self.use_total)))])]
        response["oa"] = {
            "oa_embargo_months": self.oa_embargo_months,
            "headers": [
                {"text": "OA Type", "value": "oa_status"},
                {"text": "Number of papers (annual)", "value": "num_papers"},
                {"text": "Usage (projected annual)", "value": "usage"},
                {"text": "Percent of all usage", "value": "usage_percent"},
            ],
            "data": oa_list
            }

        impact_list = [
            OrderedDict([("impact", "Downloads"),
                         ("raw", format_with_commas(self.downloads_total)),
                         ("weight", 1),
                         ("contribution", format_with_commas(self.downloads_total))]),
            OrderedDict([("impact", "Citations to papers in this journal"),
                         ("raw", format_with_commas(self.num_citations, 1)),
                         ("weight", self.settings.weight_citation),
                         ("contribution", format_with_commas(self.num_citations * self.settings.weight_citation))]),
            OrderedDict([("impact", "Authored papers in this journal"),
                         ("raw", format_with_commas(self.num_authorships, 1)),
                         ("weight", self.settings.weight_authorship),
                         ("contribution", format_with_commas(self.num_authorships * self.settings.weight_authorship))]),
            OrderedDict([("impact", "*Total*"),
                         ("raw", "-"),
                         ("weight", "-"),
                         ("contribution", format_with_commas(self.use_total))])
            ]
        response["impact"] = {
            "usage_total": self.use_total,
            "headers": [
                {"text": "Impact", "value": "impact"},
                {"text": "Raw (projected annual)", "value": "raw"},
                {"text": "Weight", "value": "weight"},
                {"text": "Usage contribution", "value": "contribution"},
            ],
            "data": impact_list
            }

        cost_list = []
        for cost_type in ["cost_actual_by_year", "cost_subscription_by_year", "cost_ill_by_year", "cost_subscription_minus_ill_by_year"]:
            cost_dict = OrderedDict()
            if cost_type == "cost_actual_by_year":
                cost_dict["cost_type"] = "*Your scenario cost*"
            else:
                cost_dict["cost_type"] = cost_type.replace("cost_", "").replace("_", " ").title()
                cost_dict["cost_type"] = cost_dict["cost_type"].replace("Ill", "ILL")
            costs = self.__getattribute__(cost_type)
            for year in self.years:
                cost_dict["year_"+str(2020 + year)] = format_currency(costs[year])
            cost_list += [cost_dict]
            cost_dict["cost_avg"] = format_currency(self.__getattribute__(cost_type.replace("_by_year", "")))
            if self.use_paywalled:
                cost_dict["cost_per_use"] = format_currency(self.__getattribute__(cost_type.replace("_by_year", "")) / float(self.use_paywalled), True)
            else:
                cost_dict["cost_per_use"] = "no paywalled usage"
        response["cost"] = {
            "subscribed": self.subscribed,
            "cppu": format_currency(self.cppu_use, True),
            "cppu_delta": format_currency(self.cppu_use_delta, True),
            "headers": [
                {"text": "Cost Type", "value": "cost_type"},
                {"text": "Cost (projected annual)", "value": "cost_avg"},
                {"text": "Cost per paid use (CPPU)", "value": "cost_per_use"},
                {"text": "Cost projected 2020", "value": "year_2020"},
                {"text": "2021", "value": "year_2021"},
                {"text": "2022", "value": "year_2022"},
                {"text": "2023", "value": "year_2023"},
                {"text": "2024", "value": "year_2024"},
            ],
            "data": cost_list
            }

        from apc_journal import ApcJournal
        my_apc_journal = ApcJournal(self.issn_l, self._scenario_data)
        response["apc"] = {
            "apc_price": my_apc_journal.apc_price_display,
            "annual_projected_cost": my_apc_journal.cost_apc_historical,
            "annual_projected_fractional_authorship": my_apc_journal.fractional_authorships_total,
            "annual_projected_num_papers": my_apc_journal.num_apc_papers_historical,
        }

        response_debug = {}
        response_debug["scenario_settings"] = self.settings.to_dict()
        response_debug["use_instant_percent"] = self.use_instant_percent
        response_debug["use_instant_percent_by_year"] = self.use_instant_percent_by_year
        response_debug["oa_embargo_months"] = self.oa_embargo_months
        response_debug["num_papers"] = self.num_papers
        response_debug["use_weight_multiplier"] = self.use_weight_multiplier_normalized
        response_debug["downloads_counter_multiplier"] = self.downloads_counter_multiplier_normalized
        response_debug["use_instant_by_year"] = self.use_instant_by_year
        response_debug["use_instant_percent_by_year"] = self.use_instant_percent_by_year
        response_debug["use_actual_by_year"] = self.use_actual_by_year
        response_debug["use_actual"] = self.use_actual
        response_debug["use_oa_green"] = self.use_oa_green
        response_debug["use_oa_hybrid"] = self.use_oa_hybrid
        response_debug["use_oa_bronze"] = self.use_oa_bronze
        response_debug["use_oa_peer_reviewed"] = self.use_oa_peer_reviewed
        response_debug["use_oa"] = self.use_oa
        response_debug["downloads_scaled_by_counter_by_year"] = self.downloads_scaled_by_counter_by_year
        response_debug["use_default_download_curve"] = self.use_default_download_curve
        response["debug"] = response_debug

        return response

    def to_dict_oa(self):
        response = OrderedDict()
        response["meta"] = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed}
        table_row = {}
        table_row["use_oa_percent"] = round(float(100)*self.use_actual["oa"]/self.use_total)
        table_row["use_green_percent"] = round(float(100)*self.use_oa_green/self.use_total)
        table_row["use_hybrid_percent"] = round(float(100)*self.use_oa_hybrid/self.use_total)
        table_row["use_bronze_percent"] = round(float(100)*self.use_oa_bronze/self.use_total)
        table_row["use_peer_reviewed_percent"] =  round(float(100)*self.use_oa_peer_reviewed/self.use_total)
        response["table_row"] = table_row
        response["bin"] = int(float(100)*self.use_actual["oa"]/self.use_total)/10
        return response

    def to_dict_fulfillment(self):
        response = OrderedDict()
        response["meta"] = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed}
        table_row = {}
        table_row["instant_use_percent"] = round(self.use_instant_percent)
        table_row["use_asns"] = round(float(100)*self.use_actual["social_networks"]/self.use_total)
        table_row["use_oa"] = round(float(100)*self.use_actual["oa"]/self.use_total)
        table_row["use_backfile"] = round(float(100)*self.use_actual["backfile"]/self.use_total)
        table_row["use_subscription"] = round(float(100)*self.use_actual["subscription"]/self.use_total)
        table_row["use_ill"] = round(float(100)*self.use_actual["ill"]/self.use_total)
        table_row["use_other_delayed"] =  round(float(100)*self.use_actual["other_delayed"]/self.use_total)
        table_row["table_row"] = table_row
        response["bin"] = int(self.use_instant_percent)/10

        return response


    def to_dict_slider(self):
        response = {"issn_l": self.issn_l,
                "title": self.title,
                "subject": self.subject,
                "use_instant": self.use_instant,
                "downloads_total": self.use_total,
                "use_total": self.use_total, # replace with above
                "cost_subscription": self.cost_subscription,
                "cost_ill": self.cost_ill,
                "cost_subscription_minus_ill": self.cost_subscription_minus_ill,
                "cppu_use_delta": self.cppu_use_delta,
                "cppu_delta_weighted": self.cppu_use_delta, # replace with above

                "cppu_use": self.cppu_use,
                "cppu_weighted": self.cppu_use, # replace with above

                "subscribed": self.subscribed,
                "use_actual_by_year": self.use_actual_by_year,
                "use_actual_weighted_by_year": self.use_actual_by_year, #replace with above
                "use_instant_by_year": self.use_instant_by_year,
                "use_instant": self.use_instant,
                "use_instant_percent_by_year": self.use_instant_percent_by_year,
                "use_instant_percent": self.use_instant_percent,
                "oa_embargo_months": self.oa_embargo_months,
                }
        response["use_groups_free_instant"] = {}
        for group in use_groups_free_instant:
            response["use_groups_free_instant"][group] = self.use_actual[group]
        response["use_groups_if_subscribed"] = {"subscription": self.downloads_subscription}
        response["use_groups_if_not_subscribed"] = {"ill": self.downloads_ill, "other_delayed": self.downloads_other_delayed}
        return response


    def to_dict(self):
        return {"issn_l": self.issn_l,
                "title": self.title,
                "subject": self.subject,
                "num_authorships": self.num_authorships,
                "num_citations": self.num_citations,
                "downloads_paywalled": self.downloads_paywalled,
                "use_paywalled": self.use_paywalled,
                "use_paywalled_weighted": self.use_paywalled, # replace with above
                "use_instant": self.use_instant,
                "downloads_total": self.downloads_total,
                "cost_subscription": self.cost_subscription,
                "cost_ill": self.cost_ill,
                "cost_subscription_minus_ill": self.cost_subscription_minus_ill,
                "cppu_use_delta": self.cppu_use_delta,
                "cppu_downloads": self.cppu_downloads,
                "cppu_use": self.cppu_use,
                "subscribed": self.subscribed
                }

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.issn_l, self.title)



# observation_year 	total views 	total views percent of 2018 	total oa views 	total oa views percent of 2018
# 2018 	25,565,054.38 	1.00 	12,664,693.62 	1.00
# 2019 	28,162,423.76 	1.10 	14,731,000.96 	1.16
# 2020 	30,944,070.68 	1.21 	17,033,520.59 	1.34
# 2021 	34,222,756.60 	1.34 	19,830,049.25 	1.57
# 2022 	38,000,898.80 	1.49 	23,092,284.75 	1.82
# 2023 	42,304,671.82 	1.65 	26,895,794.03 	2.12

