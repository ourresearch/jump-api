# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
import inspect

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

    def sum_attribute_by_year(self):
        attribute_name = inspect.currentframe().f_code.co_name
        return [np.sum([getattr(j, attribute_name)[year] for j in self.consortium_journals]) for year in self.years]

    def sum_attribute(self):
        attribute_name = inspect.currentframe().f_code.co_name
        return np.sum([getattr(j, attribute_name) for j in self.consortium_journals])

    @cached_property
    def cost_subscription_2018(self):
        return self.sum_attribute()

    @cached_property
    def num_citations_historical_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_citations(self):
        return self.sum_attribute()

    @cached_property
    def num_authorships_historical_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_authorships(self):
        return self.sum_attribute()


    @cached_property
    def cost_actual_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def cost_actual(self):
        return self.sum_attribute()



    @cached_property
    def cost_subscription_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def cost_subscription(self):
        return self.sum_attribute()

    @cached_property
    def cppu_downloads(self):
        return self.sum_attribute()

    @cached_property
    def cppu_use(self):
        return self.sum_attribute()

    @cached_property
    def ncppu(self):
        #todo
        pass

    @cached_property
    def use_weight_multiplier(self):
        if not self.downloads_total:
            return 1.0
        return float(self.use_total) / self.downloads_total


    @cached_property
    def use_instant_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def use_instant(self):
        return round(np.mean(self.use_instant_by_year), 4)


    @cached_property
    def downloads_subscription_by_year(self):
        return self.sum_attribute_by_year()


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
        return self.sum_attribute_by_year()


    @cached_property
    def downloads_social_networks(self):
        return round(np.mean(self.downloads_social_networks_by_year), 4)

    @cached_property
    def use_social_networks_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def use_social_networks(self):
        return round(round(self.downloads_social_networks * self.use_weight_multiplier), 4)


    @cached_property
    def downloads_ill_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def downloads_ill(self):
        return round(np.mean(self.downloads_ill_by_year), 4)

    @cached_property
    def use_ill(self):
        return round(round(self.downloads_ill * self.use_weight_multiplier), 4)


    @cached_property
    def downloads_other_delayed_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_other_delayed(self):
        return round(np.mean(self.downloads_other_delayed_by_year), 4)

    @cached_property
    def use_other_delayed(self):
        return round(round(self.downloads_other_delayed * self.use_weight_multiplier), 4)


    @cached_property
    def downloads_backfile_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def downloads_backfile(self):
        return round(np.mean(self.downloads_backfile_by_year), 4)

    @cached_property
    def use_backfile(self):
        return round(round(self.downloads_backfile * self.use_weight_multiplier), 4)

    @cached_property
    def num_oa_historical_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_oa_historical(self):
        return round(np.mean(self.num_oa_historical_by_year), 4)

    @cached_property
    def num_oa_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_oa(self):
        return round(np.sum([self.num_oa_for_convolving[age] * self.downloads_per_paper_by_age[age] for age in self.years]), 4)

    @cached_property
    def downloads_oa_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def use_oa(self):
        # return round(self.downloads_oa * self.use_weight_multiplier, 4)
        return self.use_oa_green + self.use_oa_bronze + self.use_oa_hybrid

    @cached_property
    def use_oa_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def downloads_total_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_total(self):
        return round(np.mean(self.downloads_total_by_year), 4)



    # used to calculate use_weight_multiplier so it can't use it
    @cached_property
    def use_total_by_year(self):
        return self.sum_attribute_by_year()

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
    def downloads_oa_by_age(self):
        # TODO do separately for each type of OA and each year
        pass

    @cached_property
    def downloads_paywalled_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def downloads_paywalled(self):
        return round(np.mean(self.downloads_paywalled_by_year), 4)

    @cached_property
    def use_paywalled(self):
        return round(round(self.downloads_paywalled * self.use_weight_multiplier), 4)


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
        return self.sum_attribute_by_year()

    @cached_property
    def use_actual_by_year(self):
        # todo
        pass
        my_dict = {}
        for group in self.downloads_actual_by_year:
            my_dict[group] = [round(num * self.use_weight_multiplier) for num in self.downloads_actual_by_year[group]]
        return my_dict


    @cached_property
    def cost_ill(self):
        return self.sum_attribute()

    @cached_property
    def cost_ill_by_year(self):
        return self.sum_attribute_by_year()


    # @cached_property
    # def cost_subscription_minus_ill_by_year(self):
    #     return [self.cost_subscription_by_year[year] - self.cost_ill_by_year[year] for year in self.years]
    #
    # @cached_property
    # def cost_subscription_minus_ill(self):
    #     return round(self.cost_subscription - self.cost_ill, 4)
    #
    # @cached_property
    # def use_total_fuzzed(self):
    #     return self.scenario.use_total_fuzzed_lookup[self.issn_l]
    #
    # @cached_property
    # def num_authorships_fuzzed(self):
    #     return self.scenario.num_authorships_fuzzed_lookup[self.issn_l]
    #
    # @cached_property
    # def num_citations_fuzzed(self):
    #     return self.scenario.num_citations_fuzzed_lookup[self.issn_l]

    @cached_property
    def num_papers(self):
        #todo
        pass

    @cached_property
    def use_instant_percent(self):
        #todo
        pass

    @cached_property
    def use_instant_percent_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_green_historical_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_green_historical(self):
        return self.sum_attribute()

    @cached_property
    def downloads_oa_green(self):
        return self.sum_attribute()

    @cached_property
    def use_oa_green(self):
        return self.sum_attribute()


    @cached_property
    def num_hybrid_historical_by_year(self):
        return self.sum_attribute_by_year()

    @cached_property
    def num_hybrid_historical(self):
        return self.sum_attribute()

    @cached_property
    def downloads_oa_hybrid(self):
        return self.sum_attribute()

    @cached_property
    def use_oa_hybrid(self):
        return self.sum_attribute()


    @cached_property
    def num_bronze_historical_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def num_bronze_historical(self):
        return self.sum_attribute()

    @cached_property
    def downloads_oa_bronze(self):
        return self.sum_attribute()


    @cached_property
    def use_oa_bronze(self):
        return self.sum_attribute()


    @cached_property
    def num_peer_reviewed_historical_by_year(self):
        return self.sum_attribute_by_year()


    @cached_property
    def num_peer_reviewed_historical(self):
        return self.sum_attribute()

    @cached_property
    def downloads_oa_peer_reviewed(self):
        return self.sum_attribute()

    @cached_property
    def use_oa_peer_reviewed(self):
        return self.sum_attribute()

    def to_dict_details(self):
        response = super(ConsortiumJournal, self).to_dict_details()
        return response
