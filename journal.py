# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
from kids.cache import cache
from collections import OrderedDict

from app import use_groups
from app import use_groups_free_instant
from app import get_db_cursor


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
        return range(2014, 2019)

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
        response = [int(((1+self.settings.cost_alacart_increase/float(100))**year) * self.cost_subscription_2018 )
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
        response = [min(response[year], self.downloads_total_by_year[year] - self.downloads_oa_by_year[year] - self.downloads_backfile_by_year[year]) for year in self.years]
        response = [max(response[year], 0) for year in self.years]
        return response

    @cached_property
    def downloads_social_networks(self):
        return round(np.mean(self.downloads_social_networks_by_year), 4)


    @cached_property
    def use_social_networks(self):
        return round(int(self.downloads_social_networks * self.use_weight_multiplier), 4)


    @cached_property
    def downloads_ill_by_year(self):
        return [int(self.settings.ill_request_percent_of_delayed/float(100) * self.downloads_paywalled_by_year[year]) for year in self.years]

    @cached_property
    def downloads_ill(self):
        return round(np.mean(self.downloads_ill_by_year), 4)

    @cached_property
    def use_ill(self):
        return round(int(self.downloads_ill * self.use_weight_multiplier), 4)


    @cached_property
    def downloads_other_delayed_by_year(self):
        return [self.downloads_paywalled_by_year[year] - self.downloads_ill_by_year[year] for year in self.years]

    @cached_property
    def downloads_other_delayed(self):
        return round(np.mean(self.downloads_other_delayed_by_year), 4)

    @cached_property
    def use_other_delayed(self):
        return round(int(self.downloads_other_delayed * self.use_weight_multiplier), 4)


    @cached_property
    def downloads_backfile_by_year(self):
        if self.settings.include_backfile:
            scaled = [0 for year in self.years]
            for year in self.years:
                scaled[year] = 0
                for age in range(year, 5):
                    scaled[year] += (self.downloads_total_by_age[age] * self.growth_scaling["downloads"][year]) - (self.downloads_oa_by_age[age] * self.growth_scaling["oa"][year])
                scaled[year] += self.downloads_total_older_than_five_years
            scaled = [int(max(0, num)) for num in scaled]
            return scaled
        else:
            return [0 for year in self.years]


    @cached_property
    def downloads_backfile(self):
        return round(np.mean(self.downloads_backfile_by_year), 4)

    @cached_property
    def use_backfile(self):
        return round(int(self.downloads_backfile * self.use_weight_multiplier), 4)

    @cached_property
    def num_oa_historical_by_year(self):
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
        return round(np.sum([self.num_oa_historical * self.downloads_total_per_paper_by_age[age] for age in self.years]), 4)

    @cached_property
    def downloads_oa_by_year(self):
        # TODO add some growth by using num_oa_by_year instead of num_oa_by_year_historical
        response = [self.downloads_oa for year in self.years]
        return response

    @cached_property
    def use_oa(self):
        return round(self.downloads_oa * self.use_weight_multiplier, 4)

    @cached_property
    def use_oa_by_year(self):
        return [self.use_oa for year in self.years]


    @cached_property
    def downloads_total_by_year(self):
        downloads_total_before_counter_correction_by_year = [max(1.0, self.my_scenario_data_row["downloads_total"]) for year in self.years]
        downloads_total_before_counter_correction_by_year = [val if val else 0.0 for val in downloads_total_before_counter_correction_by_year]
        downloads_total_scaled_by_counter = [num * self.downloads_counter_multiplier for num in downloads_total_before_counter_correction_by_year]
        scaled = [int(downloads_total_scaled_by_counter[year] * self.growth_scaling["downloads"][year]) for year in self.years]
        return scaled

    @cached_property
    def downloads_total(self):
        return round(np.mean(self.downloads_total_by_year), 4)



    # used to calculate use_weight_multiplier so it can't use it
    @cached_property
    def use_total_by_year(self):
        return [int(self.downloads_total_by_year[year] + self.use_addition_from_weights) for year in self.years]

    @cached_property
    def use_total(self):
        response = round(np.mean(self.use_total_by_year), 4)
        if response == 0:
            response = 0.0001
        return response



    @cached_property
    def downloads_total_by_age(self):
        total_downloads_by_age_before_counter_correction = [self.my_scenario_data_row["downloads_{}y".format(age)] for age in self.years]
        total_downloads_by_age_before_counter_correction = [val if val else 0 for val in total_downloads_by_age_before_counter_correction]
        downloads_total_by_age = [num * self.downloads_counter_multiplier for num in total_downloads_by_age_before_counter_correction]
        return downloads_total_by_age

    @cached_property
    def downloads_total_older_than_five_years(self):
        return self.downloads_total - np.sum(self.downloads_total_by_age)

    @cached_property
    def downloads_total_per_paper_by_age(self):
        # TODO do separately for each type of OA
        return [float(num)/self.num_papers for num in self.downloads_total_by_age]

    @cached_property
    def downloads_oa_by_age(self):
        # TODO do separately for each type of OA and each year
        response = [(float(self.downloads_total_per_paper_by_age[year])*self.num_oa_historical) for year in self.years]
        if self.oa_embargo_months:
            for age in self.years:
                if age*12 >= self.oa_embargo_months:
                    response[age] = self.downloads_total_by_age[age]
        return response

    @cached_property
    def downloads_paywalled_by_year(self):
        scaled = [self.downloads_total_by_year[year]
              - (self.downloads_backfile_by_year[year] + self.downloads_oa_by_year[year] + self.downloads_social_networks_by_year[year])
          for year in self.years]
        scaled = [int(max(0, num)) for num in scaled]
        return scaled

    @cached_property
    def downloads_paywalled(self):
        return round(np.mean(self.downloads_paywalled_by_year), 4)

    @cached_property
    def use_paywalled(self):
        return round(int(self.downloads_paywalled * self.use_weight_multiplier), 4)

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
            my_dict[group] = [int(num * self.use_weight_multiplier) for num in self.downloads_actual_by_year[group]]
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

        key = u"{}_{}".format(submitted, bronze)
        my_rows = self._scenario_data["oa"][key][self.issn_l]

        my_dict = defaultdict(dict)
        for row in my_rows:
            my_dict[row["fresh_oa_status"]][int(row["year_int"])] = int(row["count"]) * self.num_oa_papers_multiplier

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
        return round(np.sum([self.num_green_historical * self.downloads_total_per_paper_by_age[age] for age in self.years]), 4)

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
        return round(np.sum([self.num_hybrid_historical * self.downloads_total_per_paper_by_age[age] for age in self.years]), 4)

    @cached_property
    def use_oa_hybrid(self):
        return round(self.downloads_oa_hybrid * self.use_weight_multiplier, 4)


    @cached_property
    def num_bronze_historical_by_year(self):
        my_dict = self.get_oa_data()["bronze"]
        response = [my_dict.get(year, 0) for year in self.historical_years_by_year]
        return response

    @cached_property
    def num_bronze_historical(self):
        return round(np.mean(self.num_bronze_historical_by_year), 4)

    @cached_property
    def downloads_oa_bronze(self):
        return round(np.sum([self.num_bronze_historical * self.downloads_total_per_paper_by_age[age] for age in self.years]), 4)


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
        return round(np.sum([self.num_peer_reviewed_historical * self.downloads_total_per_paper_by_age[age] for age in self.years]), 4)

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
        response["total_usage"] = int(self.use_total)
        response["downloads"] = int(self.downloads_total)
        response["citations"] = int(self.num_citations)
        if self.num_authorships > 1:
            response["authorships"] = int(self.num_authorships)
        else:
            response["authorships"] = round(self.num_authorships, 1)
        response["use_weight_multiplier"] = self.use_weight_multiplier_normalized
        response["downloads_counter_multiplier"] = self.downloads_counter_multiplier_normalized
        return response

    def to_dict_overview(self):
        response = OrderedDict()
        response["meta"] = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed}
        if self.cppu_use:
            response["cppu"] = round(self.cppu_use, 2)
        else:
            response["cppu"] = None
        response["use"] = int(self.use_total)
        response["value"] = int(self.use_instant_percent)
        response["cost"] = int(self.cost_actual)
        return response

    def to_dict_cost(self):
        response = OrderedDict()
        response["meta"] = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed}
        response["scenario_cost"] = int(self.cost_actual)
        response["real_cost"] = int(self.cost_subscription_minus_ill)
        response["ill_cost"] = int(self.cost_ill)
        response["subscription_cost"] = int(self.cost_subscription)
        if self.cppu_use:
            response["cppu"] = round(self.cppu_use, 2)
        else:
            response["cppu"] = None
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
                "publisher": "Elsevier",
                "is_society_journal": False,
                "num_papers": self.num_papers,
                "cost_subscription": self.cost_subscription,
                "cost_ill": self.cost_ill,
                "cost_actual": self.cost_actual
        }

        group_list = []
        for group in use_groups:
            group_dict = OrderedDict()
            group_dict["group"] = group
            group_dict["use"] = self.use_actual[group]
            group_dict["use_percent"] = int(float(100)*self.use_actual["group"]/self.use_total)
            group_dict["timeline"] = self.use_actual_by_year[group]
            group_list += [group_dict]
        response["fulfillment"] = {
            "headers": [
                {"text": "Type", "value": "group"},
                {"text": "Use", "value": "use"},
                {"text": "Use percent", "value": "use_percent"},
                {"text": "Timeline", "value": "timeline"},
            ],
            "data": group_list
            }

        oa_list = []
        for oa_type in ["green", "hybrid", "bronze"]:
            oa_dict = OrderedDict()
            use = self.__getattribute__("use_oa_{}".format(oa_type))
            oa_dict["oa_status"] = oa_type
            oa_dict["num_papers"] = self.__getattribute__("num_{}_historical".format(oa_type))
            oa_dict["use"] = use
            oa_dict["use_percent"] = int(float(100)*use/self.use_total)
            oa_list += [oa_dict]
        response["oa"] = {
            "oa_embargo_months": self.oa_embargo_months,
            "headers": [
                {"text": "OA Type", "value": "oa_status"},
                {"text": "Number of papers", "value": "num_papers"},
                {"text": "Use", "value": "use"},
                {"text": "OA percent", "value": "use_percent"},
            ],
            "data": oa_list
            }

        impact_list = [
            OrderedDict([("impact", "downloads"),
                         ("raw", self.downloads_total),
                         ("weight", 1),
                         ("contribution", self.downloads_total)]),
            OrderedDict([("impact", "citations"),
                         ("raw", self.num_citations),
                         ("weight", self.settings.weight_citation),
                         ("contribution", self.num_citations * self.settings.weight_citation)]),
            OrderedDict([("impact", "authorships"),
                         ("raw", self.num_authorships),
                         ("weight", self.settings.weight_authorship),
                         ("contribution", self.num_authorships * self.settings.weight_authorship)])
            ]
        response["impact"] = {
            "use_total": self.use_total,
            "headers": [
                {"text": "Impact", "value": "impact"},
                {"text": "Raw", "value": "raw"},
                {"text": "Weight", "value": "weight"},
                {"text": "Contribution", "value": "contribution"},
            ],
            "data": impact_list
            }

        cost_list = []
        for cost_type in ["cost_actual_by_year", "cost_subscription_by_year", "cost_ill_by_year", "cost_subscription_minus_ill_by_year"]:
            cost_dict = OrderedDict()
            cost_dict["cost_type"] = cost_type
            costs = self.__getattribute__(cost_type)
            for year in self.years:
                cost_dict["year_"+str(2020 + year)] = costs[year]
            cost_list += [cost_dict]
        response["cost"] = {
            "subscribed": self.subscribed,
            "headers": [
                {"text": "Cost Type", "value": "cost_type"},
                {"text": "2020", "value": "year_2020"},
                {"text": "2021", "value": "year_2021"},
                {"text": "2022", "value": "year_2022"},
                {"text": "2023", "value": "year_2023"},
                {"text": "2024", "value": "year_2024"},
            ],
            "data": cost_list
            }

        response["apc"] = {
            "apc_price": None, #self.apc_price,
            "cost_apc_historical": None, # self.cost_apc_historical,
            "fractional_authorship": None, # self.fractional_authorship,

        }

        response_debug = {}
        response_debug["use_instant_percent"] = self.use_instant_percent
        response_debug["use_instant_percent_by_year"] = self.use_instant_percent_by_year
        response_debug["oa_embargo_months"] = self.oa_embargo_months
        response_debug["num_papers"] = self.num_papers
        response_debug["use_weight_multiplier"] = self.use_weight_multiplier_normalized
        response_debug["downloads_counter_multiplier"] = self.downloads_counter_multiplier_normalized
        response_debug["downloads_total_per_paper_by_age"] = self.downloads_total_per_paper_by_age
        response_debug["use_instant_by_year"] = self.use_instant_by_year
        response_debug["use_instant_percent_by_year"] = self.use_instant_percent_by_year
        response_debug["use_actual_by_year"] = self.use_actual_by_year
        response_debug["use_actual"] = self.use_actual
        response_debug["use_oa_green"] = self.use_oa_green
        response_debug["use_oa_hybrid"] = self.use_oa_hybrid
        response_debug["use_oa_bronze"] = self.use_oa_bronze
        response_debug["use_oa_peer_reviewed"] = self.use_oa_peer_reviewed
        response_debug["use_oa"] = self.use_oa
        response["debug"] = response_debug

        return response

    def to_dict_oa(self):
        response = OrderedDict()
        response["meta"] = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed}
        response["use_oa_percent"] = int(float(100)*self.use_actual["oa"]/self.use_total)
        response["use_green_percent"] = int(float(100)*self.use_oa_green/self.use_total)
        response["use_hybrid_percent"] = int(float(100)*self.use_oa_hybrid/self.use_total)
        response["use_bronze_percent"] = int(float(100)*self.use_oa_bronze/self.use_total)
        response["use_peer_reviewed_percent"] =  int(float(100)*self.use_oa_peer_reviewed/self.use_total)
        # response["num_papers"] = self.num_papers
        return response

    def to_dict_fulfillment(self):
        response = OrderedDict()
        response["meta"] = {"issn_l": self.issn_l,
                    "title": self.title,
                    "subject": self.subject,
                    "subscribed": self.subscribed}
        response["instant_use_percent"] = int(self.use_instant_percent)
        response["use_asns"] = int(float(100)*self.use_actual["social_networks"]/self.use_total)
        response["use_oa"] = int(float(100)*self.use_actual["oa"]/self.use_total)
        response["use_backfile"] = int(float(100)*self.use_actual["backfile"]/self.use_total)
        response["use_subscription"] = int(float(100)*self.use_actual["subscription"]/self.use_total)
        response["use_ill"] = int(float(100)*self.use_actual["ill"]/self.use_total)
        response["use_other_delayed"] =  int(float(100)*self.use_actual["other_delayed"]/self.use_total)
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

