# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref

from app import use_groups
from app import use_groups_free_instant
from journal import Journal


class ConsortiumJournal(Journal):

    
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
        return self._scenario_data["unpaywall_downloads_dict"][self.issn_l]

    @cached_property
    def cost_subscription_2018(self):
        return float(self.my_scenario_data_row["usa_usd"]) * (1 + self.settings.cost_content_fee_percent/float(100))

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
    def ncppu(self):
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
    def use_social_networks_by_year(self):
        return [round(round(self.downloads_social_networks * self.use_weight_multiplier), 4) for year in self.years]

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
                scaled[year] *= (1 - self.downloads_social_network_multiplier)
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
        return min(100.0, round(100 * float(self.use_instant) / self.use_total, 4))

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

    def to_dict_details(self):
        response = super(ConsortiumJournal, self).to_dict_details()
        return response