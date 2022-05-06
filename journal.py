# coding: utf-8

import datetime
import weakref
from collections import OrderedDict
from collections import defaultdict
from threading import Lock

import numpy as np
import scipy
from cached_property import cached_property
from scipy.optimize import curve_fit

from app import DEMO_PACKAGE_ID
from app import use_groups
from app import use_groups_free_instant
from app import use_groups_lookup
from util import format_currency
from util import format_percent
from util import format_with_commas

scipy_lock = Lock()

# from future of OA paper, modified to be just elsevier, all colours
default_download_by_age = [0.371269, 0.137739, 0.095896, 0.072885, 0.058849]
default_download_older_than_five_years = 1.0 - sum(default_download_by_age)

def display_cpu(value):
    if value and str(value).lower() != "nan":
        return value
    else:
        return "-"

class Journal(object):
    years = list(range(0, 5))

    def __init__(self, issn_l, scenario=None, scenario_data=None, package=None):
        self.set_scenario(scenario)
        self.set_scenario_data(scenario_data)
        self.issn_l = issn_l
        self.package_id = package.package_id
        self.package_id_for_db = self.package_id
        if self.package_id.startswith("demo"):
            self.package_id_for_db = DEMO_PACKAGE_ID
        self.subscribed_bulk = False
        self.subscribed_custom = False
        self.use_default_download_curve = False
        self.use_default_num_papers_curve = False
        self.my_package = package

    def set_scenario(self, scenario):
        if scenario:
            self.scenario = weakref.proxy(scenario)
            self.settings = self.scenario.settings
        else:
            self.scenario = None
            self.settings = None

    def set_scenario_data(self, scenario_data):
        self._scenario_data = scenario_data

    @cached_property
    def subscribed(self):
        return self.subscribed_bulk or self.subscribed_custom

    @cached_property
    def my_scenario_data_row(self):
        return self._scenario_data["unpaywall_downloads_dict"][self.issn_l] or {}

    @cached_property
    def title(self):
        return self.journal_metadata.title


    @cached_property
    def subject(self):
        return self._scenario_data['concepts'][self.issn_l].get("best", "")

    @cached_property
    def subject_top_three(self):
        return self._scenario_data['concepts'][self.issn_l].get("top_three", "")

    @cached_property
    def subjects_all(self):
        return self._scenario_data["concepts"][self.issn_l].get("all", [])

    @cached_property
    def journal_metadata(self):
        return self.my_package.get_journal_metadata(self.issn_l)

    @cached_property
    def issns(self):
        return self.journal_metadata.issns

    @cached_property
    def institution_id(self):
        return self.scenario.institution_id

    @cached_property
    def institution_name(self):
        return self.scenario.institution_name

    @cached_property
    def institution_short_name(self):
        return self.scenario.institution_short_name

    @cached_property
    def cost_first_year_including_content_fee(self):
        # return float(self.my_scenario_data_row.get("price", 0)) * (1 + self.settings.cost_content_fee_percent/float(100))
        my_lookup = self._scenario_data["prices"]
        if my_lookup.get(self.issn_l, None) is None:
            print("no price for {}".format(self.issn_l))
            return None
        # print "my price", self.issn_l, float(my_lookup.get(self.issn_l)) * (1 + self.settings.cost_content_fee_percent/float(100))
        return float(my_lookup.get(self.issn_l)) * (1 + self.settings.cost_content_fee_percent/float(100))

    @cached_property
    def papers_2018(self):
        response = self.my_scenario_data_row.get("num_papers_2018", 0)
        if not response:
            return 0
        return response

    @cached_property
    def num_citations_historical_by_year(self):
        try:
            my_dict = self._scenario_data[self.package_id_for_db]["citation_dict"].get(self.issn_l, {})
        except KeyError:
            # print "key error in num_citations_historical_by_year for {}".format(self.issn_l)
            return [0 for year in self.years]
        # the year is a string key alas
        if my_dict and isinstance(list(my_dict.keys())[0], int):
            return [my_dict.get(year, 0) for year in self.historical_years_by_year]
        else:
            return [my_dict.get(str(year), 0) for year in self.historical_years_by_year]

    @cached_property
    def num_citations(self):
        return round(np.mean(self.num_citations_historical_by_year), 4)

    @cached_property
    def num_authorships_historical_by_year(self):
        try:
            my_dict = self._scenario_data[self.package_id_for_db]["authorship_dict"].get(self.issn_l, {})
        except KeyError:
            # print "key error in num_authorships_historical_by_year for {}".format(self.issn_l)
            return [0 for year in self.years]

        # the year is a string key alas
        if my_dict and isinstance(list(my_dict.keys())[0], int):
            return [my_dict.get(year, 0) for year in self.historical_years_by_year]
        else:
            return [my_dict.get(str(year), 0) for year in self.historical_years_by_year]

    @cached_property
    def num_authorships(self):
        return round(np.mean(self.num_authorships_historical_by_year), 4)

    @cached_property
    def bronze_oa_embargo_months(self):
        return self._scenario_data["embargo_dict"].get(self.issn_l, None)

    def set_subscribe_bulk(self):
        self.subscribed_bulk = True
        # invalidate cache
        for key in self.__dict__:
            if "actual" in key:
                del self.__dict__[key]

    def set_unsubscribe_bulk(self):
        self.subscribed_bulk = False
        # invalidate cache
        for key in self.__dict__:
            if "actual" in key:
                del self.__dict__[key]

    def set_subscribe_custom(self):
        self.subscribed_custom = True
        # invalidate cache
        for key in self.__dict__:
            if "actual" in key:
                del self.__dict__[key]

    def set_unsubscribe_custom(self):
        self.subscribed_custom = False
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
        return list(range(2015, 2019+1))

    @cached_property
    def cost_actual_by_year(self):
        if self.subscribed:
            return self.subscription_cost_by_year
        return self.ill_cost_by_year

    @cached_property
    def cost_actual(self):
        if self.subscribed:
            return self.subscription_cost
        return self.ill_cost


    @cached_property
    def subscription_cost_by_year(self):
        if self.cost_first_year_including_content_fee is not None:
            response = [round(((1+self.settings.cost_alacart_increase/float(100))**year) * self.cost_first_year_including_content_fee )
                                                for year in self.years]
        else:
            # will cause errors further down the line
            response = [None for year in self.years]
        return response

    @cached_property
    def subscription_cost(self):
        return round(np.mean(self.subscription_cost_by_year), 4)


    @cached_property
    def cpu(self):
        if not self.use_paywalled or self.use_paywalled < 1:
            return None
        return round(self.cost_subscription_minus_ill/self.use_paywalled, 6)

    @cached_property
    def old_school_cpu(self):
        if not self.downloads_total or self.downloads_total < 1:
            return None
        return round(float(self.subscription_cost)/self.downloads_total, 6)

    @cached_property
    def use_weight_multiplier(self):
        if not self.downloads_total:
            return 1.0
        return float(self.use_total) / self.downloads_total


    @cached_property
    def use_free_instant_by_year(self):
        response = [0 for year in self.years]
        for group in use_groups_free_instant:
            for year in self.years:
                response[year] += self.__getattribute__("use_{}_by_year".format(group))[year]
        return response

    @cached_property
    def use_instant_by_year(self):
        response = [0 for year in self.years]
        for group in use_groups_free_instant:
            for year in self.years:
                response[year] += self.__getattribute__("use_{}_by_year".format(group))[year]
        if self.subscribed:
            group = "subscription"
            for year in self.years:
                response[year] += self.__getattribute__("use_{}_by_year".format(group))[year]
        return response

    @cached_property
    def use_instant(self):
        # return round(np.mean(self.use_instant_by_year), 4)
        response = 0
        for group in use_groups_free_instant:
            response += self.__getattribute__("use_{}".format(group))
        if self.subscribed:
            group = "subscription"
            response += self.__getattribute__("use_{}".format(group))
        return response

    @cached_property
    def use_free_instant(self):
        # return round(np.mean(self.use_free_instant_by_year), 4)
        response = 0
        for group in use_groups_free_instant:
            response += self.__getattribute__("use_{}".format(group))
        return response

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
    def use_subscription_by_year(self):
        return [self.use_paywalled_by_year[year] for year in self.years]

    @cached_property
    def downloads_social_network_multiplier(self):
        if not self.settings.include_social_networks:
            return 0.0
        return self._scenario_data["social_networks"].get(self.issn_l, 0.06)

    @cached_property
    def downloads_social_networks_by_year(self):
        if not self.downloads_social_network_multiplier:
            return [0.0 for year in self.years]

        response = [0.0 for year in self.years]
        for year in self.years:
            social_network = self.downloads_total_by_year[year] * self.downloads_social_network_multiplier
            if social_network:
                overlap_with_backfile = (social_network * self.downloads_backfile_by_year[year]) / self.downloads_total_by_year[year]
                # print social_network, self.downloads_backfile_by_year[year], self.downloads_total_by_year[year], overlap_with_backfile
                social_network = social_network - overlap_with_backfile
            response[year] = social_network

        # response = [self.downloads_total_by_year[year] * self.downloads_social_network_multiplier for year in self.years]

        response = [min(response[year], self.downloads_total_by_year[year] - self.downloads_oa_by_year[year]) for year in self.years]
        response = [max(response[year], 0) for year in self.years]
        return response

    @cached_property
    def downloads_social_networks(self):
        return round(np.mean(self.downloads_social_networks_by_year), 4)

    @cached_property
    def use_social_networks_by_year(self):
        response = [max(0, round(self.downloads_social_networks_by_year[year] * self.use_weight_multiplier, 4)) for year in self.years]
        response = [min(response[year], self.use_total_by_year[year] - self.use_oa_by_year[year]) for year in self.years]
        response = [max(response[year], 0) for year in self.years]
        return response

    @cached_property
    def use_social_networks(self):
        # return round(self.downloads_social_networks * self.use_weight_multiplier, 4)
        response = min(np.mean(self.use_social_networks_by_year), self.use_total - self.use_oa)
        return response


    @cached_property
    def downloads_ill_by_year(self):
        response = [self.settings.ill_request_percent_of_delayed/float(100) * self.downloads_paywalled_by_year[year] for year in self.years]
        response = [num if num else 0 for num in response]
        return response


    @cached_property
    def downloads_ill(self):
        return round(np.mean(self.downloads_ill_by_year), 4)

    @cached_property
    def use_ill(self):
        return self.settings.ill_request_percent_of_delayed/float(100) * self.use_paywalled

    @cached_property
    def use_ill_by_year(self):
        return [self.settings.ill_request_percent_of_delayed/float(100) * self.use_paywalled_by_year[year] for year in self.years]

    @cached_property
    def downloads_other_delayed_by_year(self):
        return [self.downloads_paywalled_by_year[year] - self.downloads_ill_by_year[year] for year in self.years]

    @cached_property
    def downloads_other_delayed(self):
        return round(np.mean(self.downloads_other_delayed_by_year), 4)

    @cached_property
    def use_other_delayed(self):
        return self.use_paywalled - self.use_ill

    @cached_property
    def use_other_delayed_by_year(self):
        return [self.use_paywalled_by_year[year] - self.use_ill_by_year[year] for year in self.years]

    @cached_property
    def display_perpetual_access_years(self):
        if not self.perpetual_access_years:
            return ""
        if min(self.perpetual_access_years) < min(self.year_by_perpetual_access_years):
            return "<{}-{}".format(min(self.perpetual_access_years), max(self.perpetual_access_years))
        return "{}-{}".format(min(self.perpetual_access_years), max(self.perpetual_access_years))

    @cached_property
    def has_perpetual_access(self):
        # print "has_perpetual_access", self.perpetual_access_years

        if not self.perpetual_access_years:
            return False
        return True

    @cached_property
    def year_by_perpetual_access_years(self):
        return list(range(min(self.historical_years_by_year)-5, max(self.historical_years_by_year)+1))

    @cached_property
    def perpetual_access_years(self):
        # if no perpetual access data for any journals in this scenario, then we are acting like it has perpetual access to everything
        # else, for this journal
        #   if two dates, that is the perpetual access range
        #   if a start date and no end date, then has perpetual access till the model says it doesn't
        #   if no start date, then no perpetual access
        #   if not there, then no perpetual access

        data_dict = self._scenario_data["perpetual_access"]

        # if not data_dict:
        #     return self.year_by_perpetual_access_years

        if self.issn_l not in data_dict:
            return []

        start_date = data_dict[self.issn_l]["start_date"]
        end_date = data_dict[self.issn_l]["end_date"]

        #   if no dates, then no perpetual access
        if not start_date:
            start_date = datetime.datetime(1850, 1, 1)  # far in the past

        #   if no end date, then has perpetual access till the model says it doesn't
        if not end_date:
            end_date = datetime.datetime(2042, 1, 2)  # far in the future, let's really hope we have universal OA by then

        #   if two dates, that is the perpetual access range
        response = []
        for year in self.year_by_perpetual_access_years:
            working_date = datetime.datetime(year, 1, 2).isoformat()  # use January 2nd
            try:
                start_date = start_date.isoformat()
            except:
                pass

            try:
                end_date = end_date.isoformat()
            except:
                pass

            in_range = working_date > start_date and working_date < end_date

            if in_range:
                # print year, "yes", data_dict[self.issn_l]
                response.append(year)
            else:
                pass
                # print year, "no", data_dict[self.issn_l]

        return response

    @cached_property
    def downloads_backfile_by_year(self):
        response = self.sum_obs_pub_matrix_by_obs(self.backfile_obs_pub)
        # if self.issn_l == "0271-678X":
        #     print self.backfile_obs_pub
        #     print self.sum_obs_pub_matrix_by_obs(self.backfile_obs_pub)
        response = [min(response[year], self.downloads_total_by_year[year] - self.downloads_oa_by_year[year]) for year in self.years]
        return response

    @cached_property
    def downloads_obs_pub(self):
        by_age = self.downloads_by_age
        by_age_old = self.downloads_total_older_than_five_years/5.0
        growth_scaling = self.growth_scaling_downloads
        my_matrix = self.obs_pub_matrix(by_age, by_age_old, growth_scaling)
        return my_matrix


    @cached_property
    def oa_obs_pub(self):
        by_age = self.downloads_oa_by_age
        if not self.downloads_by_age[4]:
            by_age_old = (self.downloads_total_older_than_five_years/5.0)
        else:
            by_age_old = (self.downloads_total_older_than_five_years/5.0) * (self.downloads_oa_by_age[4]/(self.downloads_by_age[4]))
        growth_scaling = self.growth_scaling_oa_downloads
        my_matrix = self.obs_pub_matrix(by_age, by_age_old, growth_scaling)
        # if self.issn_l == "0271-678X":
        #     print "by_age", by_age
        #     print "by_age_old", by_age_old
        #     print "my_matrix", self.display_obs_pub_matrix(my_matrix)
        return my_matrix

    @cached_property
    def backfile_raw_obs_pub(self):
        response = {}
        for obs_year in range(2020, 2025):
            obs_key = "obs{}".format(obs_year)
            response[obs_key] = {}
            for pub_year in range(2011, 2025):
                pub_key = "pub{}".format(pub_year)

                # modelling subscription ending in 2020, so no backfile beyond that
                if pub_year in self.perpetual_access_years:
                    value = self.downloads_obs_pub[obs_key][pub_key] - self.oa_obs_pub[obs_key][pub_key]
                elif pub_year-1 in self.perpetual_access_years:
                    value = 0.5*(self.downloads_obs_pub[obs_key][pub_key] - self.oa_obs_pub[obs_key][pub_key])
                else:
                    value = 0
                value = max(value, 0)
                response[obs_key][pub_key] = int(round(value))
        return response


    @cached_property
    def backfile_obs_pub(self):
        response = {}
        for obs_year in range(2020, 2025):
            obs_key = "obs{}".format(obs_year)
            response[obs_key] = {}
            for pub_year in range(2011, 2025):
                pub_key = "pub{}".format(pub_year)
                value = self.backfile_raw_obs_pub[obs_key][pub_key]
                # value *= (self.settings.backfile_contribution / 100.0)
                value = max(0, value)
                response[obs_key][pub_key] = int(round(value))

        return response

    def obs_pub_matrix(self, by_age, by_age_old, growth_scaling):
        response = {}
        for obs_index, obs_year in enumerate(range(2020, 2025)):
            response["obs{}".format(obs_year)] = {}
            for pub_year in range(2011, 2025):
                age = obs_year - pub_year
                value = 0
                if age >= 0 and age <= 4:
                    value = int(round(by_age[age]))
                elif age >= 5 and age <= 9:
                    value = int(round(by_age_old))
                value *= growth_scaling[obs_index]
                response["obs{}".format(obs_year)]["pub{}".format(pub_year)] = int(round(value))
        return response

    def display_obs_pub_matrix(self, my_obs_pub_matrix):
        response = []
        obs_keys_ordered = sorted(my_obs_pub_matrix.keys())
        for obs_key in obs_keys_ordered:
            sub_response = []
            pub_row = my_obs_pub_matrix[obs_key]
            pub_keys_ordered = sorted(pub_row.keys())
            for pub_key in pub_keys_ordered:
                sub_response.append(pub_row[pub_key])
            response.append(sub_response)
        return response

    def sum_obs_pub_matrix_by_obs(self, my_obs_pub_matrix):
        response = [0 for year in self.years]
        for i, obs_year in enumerate(range(2020, 2025)):
            obs_key = "obs{}".format(obs_year)
            for pub_year in range(2011, 2025):
                pub_key = "pub{}".format(pub_year)
                response[i] += my_obs_pub_matrix[obs_key][pub_key]
        return response



    @cached_property
    def downloads_backfile(self):
        return round(np.mean(self.downloads_backfile_by_year), 4)

    @cached_property
    def use_backfile_by_year(self):
        response = [max(0, round(self.downloads_backfile_by_year[year] * self.use_weight_multiplier, 4)) for year in self.years]
        response = [min(response[year], self.use_total_by_year[year] - self.use_oa_by_year[year]) for year in self.years]
        return response

    @cached_property
    def use_backfile(self):
        # response = [min(response[year], self.use_total_by_year[year] - self.use_oa_by_year[year]) for year in self.years]
        response = min(np.mean(self.use_backfile_by_year), self.use_total - self.use_oa - self.use_social_networks)
        return round(response, 4)

    @cached_property
    def raw_num_oa_historical_by_year(self):
        return [self.num_green_historical_by_year[year]+self.num_bronze_historical_by_year[year]+self.num_hybrid_historical_by_year[year] for year in self.years]

    @cached_property
    def use_oa_plus_social_networks(self):
        return self.use_oa + self.use_social_networks

    @cached_property
    def use_oa_plus_social_networks_by_year(self):
        return [self.use_oa_by_year[year] + self.use_social_networks_by_year[year] for year in self.years]

    @cached_property
    def downloads_oa_by_year(self):
        # if self.issn_l == "0271-678X":
        #     print "self.oa_obs_pub", self.oa_obs_pub
        #     print "self.sum_obs_pub_matrix_by_obs(self.oa_obs_pub)", self.sum_obs_pub_matrix_by_obs(self.oa_obs_pub)

        return self.sum_obs_pub_matrix_by_obs(self.oa_obs_pub)

    @cached_property
    def downloads_oa_plus_social_networks_by_year(self):
        return [self.downloads_oa_by_year[year] + self.downloads_social_networks_by_year[year] for year in self.years]

    @cached_property
    def use_oa(self):
        # return round(self.downloads_oa * self.use_weight_multiplier, 4)
        # return self.use_oa_green + self.use_oa_bronze + self.use_oa_hybrid
        # if self.issn_l == "0271-678X":
        #     print "self.use_oa_by_year", self.use_oa_by_year
        response = min(np.mean(self.use_oa_by_year), self.use_total)
        return round(response, 4)

    @cached_property
    def use_oa_by_year(self):
        # TODO fix
        response = [max(0, self.downloads_oa_by_year[year] * self.use_weight_multiplier) for year in self.years]
        response = [min(response[year], self.use_total_by_year[year]) for year in self.years]
        return response

    @cached_property
    def use_oa_percent_by_year(self):
        # print self.use_oa_by_year, self.use_total_by_year
        response = [min(100, round(100.0*(self.use_oa_by_year[year]/(1.0+self.use_total_by_year[year])), 1)) for year in self.years]
        return response

    @cached_property
    def downloads_total_by_year(self):
        scaled = [self.downloads_scaled_by_counter_by_year[year] * self.growth_scaling_downloads[year] for year in self.years]
        return scaled

    @cached_property
    def downloads_total(self):
        return round(np.mean(self.downloads_total_by_year), 4)



    # used to calculate use_weight_multiplier so it can't use it
    @cached_property
    def use_total_by_year(self):
        return [self.downloads_total_by_year[year] + self.use_addition_from_weights*self.growth_scaling_downloads[year] for year in self.years]

    @cached_property
    def use_total(self):
        response = round(np.mean(self.use_total_by_year), 4)
        if response == 0:
            response = 0.0001
        return response


    @cached_property
    def raw_downloads_by_age(self):
        # isn't replaced by default if too low or not monotonically decreasing
        total_downloads_by_age_before_counter_correction = [self.my_scenario_data_row.get("downloads_{}y".format(age), 0) for age in self.years]
        total_downloads_by_age_before_counter_correction = [val if val else 0 for val in total_downloads_by_age_before_counter_correction]
        downloads_by_age = [num * self.downloads_counter_multiplier for num in total_downloads_by_age_before_counter_correction]
        return downloads_by_age


    @cached_property
    def curve_fit_for_downloads(self):
        x = np.array(self.years)
        y = np.array(self.downloads_by_age_before_counter_correction)
        initial_guess = (float(np.max(y)), 30.0, -1.0)  # determined empirically

        def func(x, a, b, c):
            try:
                response = b + a * scipy.special.expit( x / c )
            except:
                response = None
            return response


        try:
            pars, pcov = curve_fit(func, x, y, initial_guess)
        except:
            return {}

        y_fit = [func(a, pars[0], pars[1], pars[2]) for a in x]

        residuals = y - y_fit
        ss_res = np.sum(residuals**2) + 0.0001
        ss_tot = np.sum((y - np.mean(y))**2) + 0.0001
        r_squared = 1 - (ss_res / ss_tot)

        return {"y_fit": y_fit,
                "r_squared": r_squared,
                "params": list(pars),
                "input_y": list(y)}



    @cached_property
    def downloads_by_age_before_counter_correction(self):
        downloads_by_age_before_counter_correction = [self.my_scenario_data_row.get("downloads_{}y".format(age), 0) for age in self.years]
        downloads_by_age_before_counter_correction = [val if val else 0 for val in downloads_by_age_before_counter_correction]
        return downloads_by_age_before_counter_correction


    @cached_property
    def downloads_by_age(self):
        self.use_default_download_curve = False

        # although the curve fit is on downloads, download number probably off if there are some years with no papers,
        # so in those cases just use the default
        nonzero_paper_years = [year for year in self.years if self.raw_num_papers_historical_by_year[year]]
        if len(nonzero_paper_years) == 5:
            scipy_lock.acquire()
            my_curve_fit = self.curve_fit_for_downloads
            scipy_lock.release()
            if my_curve_fit and my_curve_fit["r_squared"] >= 0.75:
                # print u"GREAT curve fit for {}, r_squared {}".format(self.issn_l, my_curve_fit.get("r_squared", "no r_squared"))
                downloads_by_age_before_counter_correction_curve_to_use = my_curve_fit["y_fit"]
            else:
                # print u"bad curve fit for {}, r_squared {}".format(self.issn_l, my_curve_fit.get("r_squared", "no r_squared"))
                self.use_default_download_curve = True
        else:
            self.use_default_download_curve = True

        if self.use_default_download_curve:
            sum_total_downloads_by_age_before_counter_correction = np.sum(self.downloads_by_age_before_counter_correction)
            downloads_by_age_before_counter_correction_curve_to_use = [num*sum_total_downloads_by_age_before_counter_correction for num in default_download_by_age]

        downloads_by_age = [num * self.downloads_counter_multiplier for num in downloads_by_age_before_counter_correction_curve_to_use]

        downloads_by_age = [max(val, 0.0) for val in downloads_by_age]

        return downloads_by_age


    @cached_property
    def downloads_total_older_than_five_years(self):
        if self.use_default_download_curve:
            return default_download_older_than_five_years * (self.downloads_total)
        return self.downloads_total - np.sum(self.downloads_by_age)

    @cached_property
    def downloads_per_paper_by_age(self):
        # TODO do separately for each type of OA
        # print [[float(num), self.num_papers, self.num_oa_historical] for num in self.downloads_by_age]

        if self.num_papers:
            return [float(num)/self.num_papers for num in self.downloads_by_age]
        return [0 for num in self.downloads_by_age]

    @cached_property
    def downloads_scaled_by_counter_by_year(self):
        # TODO is flat right now
        downloads_total_before_counter_correction_by_year = [max(1.0, self.my_scenario_data_row.get("downloads_total", 0.0) or 0.0) for year in self.years]
        downloads_total_before_counter_correction_by_year = [val if val else 0.0 for val in downloads_total_before_counter_correction_by_year]
        downloads_total_scaled_by_counter = [num * self.downloads_counter_multiplier for num in downloads_total_before_counter_correction_by_year]
        return downloads_total_scaled_by_counter

    @cached_property
    def downloads_per_paper(self):
        per_paper = float(self.downloads_scaled_by_counter_by_year)/self.num_papers
        return per_paper


    @cached_property
    def proportion_oa_historical_by_year(self):
        response = []
        for year in self.years:
            if self.raw_num_papers_historical_by_year[year]:
                response.append(float(self.raw_num_oa_historical_by_year[year] or 0) / self.raw_num_papers_historical_by_year[year])
            else:
                response.append(None)

        # if self.issn_l == "0031-9406":
        #     print "self.raw_num_oa_historical_by_year", self.raw_num_oa_historical_by_year
        #     print self.raw_num_papers_historical_by_year
        #     print "response", response
        #     print
        return response


    @cached_property
    def num_oa_historical_by_year(self):
        oa_proportion_reversed = self.proportion_oa_historical_by_year[::-1]

        num_scaled_by_num_papers = []
        for year in self.years:
            if oa_proportion_reversed[year] and oa_proportion_reversed[year]:
                num_scaled_by_num_papers.append(oa_proportion_reversed[year]*self.num_papers_by_year[year])
            else:
                num_scaled_by_num_papers.append(0)

        # if self.issn_l == "0031-9406":
        #     print "self.num_papers_growth_from_2018_by_year", self.num_papers_growth_from_2018_by_year
        #     print oa_proportion_reversed, "oa_proportion_reversed"
        #     print num_scaled_by_num_papers, "num_scaled_by_num_papers"
        #     print self.num_papers_by_year, "num_papers_by_year"
        #     print

        return [int(round(min(self.num_papers_by_year[year], num_scaled_by_num_papers[year]))) for year in self.years]


    @cached_property
    def downloads_oa_by_age(self):
        response = [(float(self.downloads_per_paper_by_age[age])*self.num_oa_historical_by_year[age]) for age in self.years]
        if self.bronze_oa_embargo_months:
            for age in self.years:
                if age*12 >= self.bronze_oa_embargo_months:
                    response[age] = self.downloads_by_age[age]

        # if self.issn_l == "0031-9406":
        #     print "self.num_oa_historical_by_year", self.num_oa_historical_by_year
        #     print "downloads_per_paper_by_age", self.downloads_per_paper_by_age
        #     print "downloads_by_age", self.downloads_by_age
        #     print "downloads_by_age_before_counter_correction", self.downloads_by_age_before_counter_correction
        #     print "downloads_oa_by_age", response
        return response


    @cached_property
    def downloads_oa_bronze_by_age(self):
        response = [(float(self.downloads_per_paper_by_age[age])*self.num_bronze_by_year[age]) for age in self.years]
        return response

    @cached_property
    def downloads_oa_green_by_age(self):
        response = [(float(self.downloads_per_paper_by_age[age])*self.num_green_by_year[age]) for age in self.years]
        return response

    @cached_property
    def num_hybrid_by_year(self):
        num_reversed = self.num_hybrid_historical_by_year[::-1]
        return [min(self.num_papers_by_year[year],
                                  num_reversed[year]) for year in self.years]

    @cached_property
    def num_bronze_by_year(self):
        num_reversed = self.num_bronze_historical_by_year[::-1]
        return [min(self.num_papers_by_year[year] - self.num_hybrid_by_year[year],
                                  num_reversed[year]) for year in self.years]

    @cached_property
    def num_green_by_year(self):
        num_reversed = self.num_green_historical_by_year[::-1]
        return [min(self.num_papers_by_year[year] - self.num_hybrid_by_year[year] - self.num_bronze_by_year[year],
                                  num_reversed[year]) for year in self.years]

    @cached_property
    def downloads_oa_hybrid_by_age(self):
        response = [(float(self.downloads_per_paper_by_age[age])*self.num_hybrid_by_year[age]) for age in self.years]
        return response

    @cached_property
    def downloads_oa_peer_reviewed_by_age(self):
        num_reversed = self.num_peer_reviewed_historical_by_year[::-1]
        num_for_convolving = [min(self.num_papers_by_year[year], num_reversed[year]) for year in self.years]

        response = [(float(self.downloads_per_paper_by_age[age])*num_for_convolving[age]) for age in self.years]
        return response

    @cached_property
    def downloads_paywalled_by_year(self):
        scaled = [self.downloads_total_by_year[year]
              - (self.downloads_backfile_by_year[year] + self.downloads_oa_by_year[year] + self.downloads_social_networks_by_year[year])
          for year in self.years]
        scaled = [max(0, num) for num in scaled]
        return scaled

    @cached_property
    def downloads_paywalled(self):
        return round(np.mean(self.downloads_paywalled_by_year), 4)

    @cached_property
    def use_paywalled(self):
        return max(0, self.use_total - self.use_free_instant)

    @cached_property
    def use_paywalled_by_year(self):
        return [max(0, self.use_total_by_year[year] - self.use_free_instant_by_year[year]) for year in self.years]

    @cached_property
    def downloads_counter_multiplier_normalized(self):
        return round(self.downloads_counter_multiplier / self.scenario.downloads_counter_multiplier, 4)

    @cached_property
    def use_weight_multiplier_normalized(self):
        return round(self.use_weight_multiplier / self.scenario.use_weight_multiplier, 4)

    @cached_property
    def downloads_actual(self):
        response = defaultdict(int)
        for group in use_groups:
            response[group] = round(np.mean(self.downloads_actual_by_year[group]), 4)
        return response

    @cached_property
    def use_actual(self):
        response = defaultdict(int)
        for group in use_groups + ["oa_plus_social_networks"]:
            response[group] = self.__getattribute__("use_{}".format(group))
            if self.subscribed:
                response["ill"] = 0
                response["other_delayed"] = 0
            else:
                response["subscription"] = 0
        response["oa_no_social_networks"] = response["oa"]
        return response

    @cached_property
    def downloads_actual_by_year(self):
        #initialize
        my_dict = {}
        # include the if to skip this if no useage
        if self.downloads_total:
            for group in use_groups:
                my_dict[group] = self.__getattribute__("downloads_{}_by_year".format(group))
                if self.subscribed:
                    my_dict["ill"] = [0 for year in self.years]
                    my_dict["other_delayed"] = [0 for year in self.years]
                else:
                    my_dict["subscription"] = [0 for year in self.years]
        return my_dict

    @cached_property
    def use_actual_by_year(self):
        my_dict = {}
        for group in use_groups:
            # defaults
            my_dict[group] = self.__getattribute__("use_{}_by_year".format(group))
            if self.subscribed:
                my_dict["ill"] = [0 for year in self.years]
                my_dict["other_delayed"] = [0 for year in self.years]
            else:
                my_dict["subscription"] = [0 for year in self.years]
        return my_dict

    @cached_property
    def downloads_total_before_counter_correction(self):
        return max(1.0, self.my_scenario_data_row.get("downloads_total", 0.0))

    @cached_property
    def use_addition_from_weights(self):
        # using the average on purpose... by year too rough
        weights_addition = 0
        # the if is to help speed it up
        if self.num_citations or self.num_authorships:
            weights_addition = float(self.settings.weight_citation) * self.num_citations
            weights_addition += float(self.settings.weight_authorship) * self.num_authorships
            weights_addition = round(weights_addition, 4)
        return weights_addition

    @cached_property
    def downloads_counter_multiplier(self):
        try:
            counter_for_this_journal = self._scenario_data[self.package_id_for_db]["counter_dict"][self.issn_l]
            counter_multiplier = float(counter_for_this_journal) / self.downloads_total_before_counter_correction
        except:
            counter_multiplier = float(0)
        return counter_multiplier


    @cached_property
    def ill_cost(self):
        return round(np.mean(self.ill_cost_by_year), 4)

    @cached_property
    def ill_cost_by_year(self):
        return [round(self.downloads_ill_by_year[year] * self.settings.cost_ill, 4) for year in self.years]

    @cached_property
    def cost_subscription_minus_ill_by_year(self):
        return [self.subscription_cost_by_year[year] - self.ill_cost_by_year[year] for year in self.years]

    @cached_property
    def cost_subscription_minus_ill(self):
        return round(self.subscription_cost - self.ill_cost, 4)

    @cached_property
    def cpu_rank(self):
        if self.cpu:
            try:
                return self.scenario.cpu_rank_lookup[self.issn_l]
            except ReferenceError:
                return None
        return None

    @cached_property
    def old_school_cpu_rank(self):
        if self.old_school_cpu:
            return self.scenario.old_school_cpu_rank_lookup[self.issn_l]
        return None

    @cached_property
    def cost_subscription_fuzzed(self):
        return self.scenario.cost_subscription_fuzzed_lookup[self.issn_l]

    @cached_property
    def cost_subscription_minus_ill_fuzzed(self):
        return self.scenario.cost_subscription_minus_ill_fuzzed_lookup[self.issn_l]

    @cached_property
    def cpu_fuzzed(self):
        return self.scenario.cpu_fuzzed_lookup[self.issn_l]

    @cached_property
    def use_total_fuzzed(self):
        return self.scenario.use_total_fuzzed_lookup[self.issn_l]

    @cached_property
    def downloads_fuzzed(self):
        return self.scenario.downloads_fuzzed_lookup[self.issn_l]

    @cached_property
    def num_authorships_fuzzed(self):
        return self.scenario.num_authorships_fuzzed_lookup[self.issn_l]

    @cached_property
    def num_citations_fuzzed(self):
        return self.scenario.num_citations_fuzzed_lookup[self.issn_l]

    @cached_property
    def curve_fit_for_num_papers(self):
        x_list = []
        y_list = []
        threshold = 0.25
        from app import USE_PAPER_GROWTH
        if USE_PAPER_GROWTH:
            threshold = 0.1

        for year in self.years:
            if self.raw_num_papers_historical_by_year[year] >= threshold * self.raw_num_papers_historical_by_year[4]:
                x_list.append(year)
                y_list.append(self.raw_num_papers_historical_by_year[year])
        x = np.array(x_list)
        y = np.array(y_list)

        initial_guess = (float(np.mean(y)), 0.05)  # determined empirically

        def func(x, b, m):
               return b + m * x

        try:
            pars, pcov = curve_fit(func, x, y, initial_guess)
        except:
            return {}

        y_fit = [func(a, pars[0], pars[1]) for a in x]

        residuals = y - y_fit
        ss_res = np.sum(residuals**2) + 0.0001
        ss_tot = np.sum((y - np.mean(y))**2) + 0.0001
        r_squared = 1 - (ss_res / ss_tot)

        y_extrap = [func(a, pars[0], pars[1]) for a in range(5, 10)]

        response = {"y_fit": y_fit,
                "x": x_list,
                "r_squared": r_squared,
                "params": list(pars),
                "y_extrap": y_extrap,
                "input_y": list(y)
                }
        return response

    @cached_property
    def num_papers_slope_percent(self):
        if not self.num_papers_by_year[0]:
            return None
        return int(round(float(100)*(self.num_papers_by_year[4] - self.num_papers_by_year[0])/(5.0 * self.num_papers_by_year[0])))

    @cached_property
    def growth_scaling_downloads(self):
        return self.num_papers_growth_from_2018_by_year

    @cached_property
    def growth_scaling_oa_downloads(self):
        # todo add OA growing faster
        return self.growth_scaling_downloads

    @cached_property
    def num_papers_growth_from_2018_by_year(self):
        num_papers_2018 = self.num_papers_by_year[4]
        return [round(float(self.num_papers_by_year[year])/(num_papers_2018+1), 4) for year in self.years]

    @cached_property
    def num_papers_by_year(self):
        my_curve_fit = None
        nonzero_paper_years = [year for year in self.years if self.raw_num_papers_historical_by_year[year] >= 0.1*self.raw_num_papers_historical_by_year[4]]
        # make sure it includes at least 4 years and the most recent year
        from app import USE_PAPER_GROWTH
        if USE_PAPER_GROWTH:
            if len(nonzero_paper_years) >= 4 and self.papers_2018:
                scipy_lock.acquire()
                my_curve_fit = self.curve_fit_for_num_papers
                scipy_lock.release()
        if my_curve_fit and my_curve_fit["r_squared"] >= -0.1:
            # print u"GREAT curve fit for {}, r_squared {}".format(self.issn_l, my_curve_fit.get("r_squared", "no r_squared"))
            self.use_default_num_papers_curve = False
            # only let it drop down below 25% of the most recent year
            return [max(int(round(self.papers_2018 * 0.5)), num) for num in my_curve_fit["y_extrap"]]
        else:
            # print u"bad curve fit for {}, r_squared {}".format(self.issn_l, my_curve_fit.get("r_squared", "no r_squared"))
            self.use_default_num_papers_curve = True
            return [self.papers_2018 for year in self.years]


    @cached_property
    def raw_num_papers_historical_by_year(self):
        # if self.issn_l == "0271-678X":
        #     print "num_papers", self._scenario_data["num_papers"][self.issn_l]
        if self.issn_l in self._scenario_data["num_papers"]:
            my_raw_numbers = self._scenario_data["num_papers"][self.issn_l]
            # historical goes up to 2019 but we don't have all the data for that yet

            # yeah this is ugly depends on whether cached or not yuck
            from app import USE_PAPER_GROWTH
            if USE_PAPER_GROWTH:
                if isinstance(list(my_raw_numbers.keys())[0], int):
                    response = [my_raw_numbers.get(year, 0) for year in self.historical_years_by_year]
                else:
                    response = [my_raw_numbers.get(str(year), 0) for year in self.historical_years_by_year]
            else:
                if isinstance(list(my_raw_numbers.keys())[0], int):
                    response = [my_raw_numbers.get(year-1, 0) for year in self.historical_years_by_year]
                else:
                    response = [my_raw_numbers.get(str(year-1), 0) for year in self.historical_years_by_year]

        else:
            response = [self.papers_2018 for year in self.years]

        return response

    @cached_property
    def num_papers(self):
        return round(np.mean(self.num_papers_by_year))

    @cached_property
    def use_instant_percent(self):
        if not self.use_total:
            return 0
        return min(100.0, round(100 * float(self.use_instant) / self.use_total, 4))

    @cached_property
    def use_free_instant_percent(self):
        if not self.use_total:
            return 0
        return min(100.0, round(100 * float(self.use_free_instant) / self.use_total, 4))

    @cached_property
    def use_instant_percent_by_year(self):
        if not self.downloads_total:
            return 0
        return [round(100 * float(self.use_instant_by_year[year]) / self.use_total_by_year[year], 4) if self.use_total_by_year[year] else None for year in self.years]


    # @cached_property
    # def num_oa_papers_multiplier(self):
    #     oa_adjustment_dict = self._scenario_data["oa_adjustment"].get(self.issn_l, None)
    #     if not oa_adjustment_dict:
    #         return 1.0
    #     if not oa_adjustment_dict["unpaywall_measured_fraction_3_years_oa"]:
    #         return 1.0
    #     response = float(oa_adjustment_dict["mturk_max_oa_rate"]) / (oa_adjustment_dict["unpaywall_measured_fraction_3_years_oa"])
    #     # print "num_oa_papers_multiplier", response, float(oa_adjustment_dict["mturk_max_oa_rate"]), (oa_adjustment_dict["unpaywall_measured_fraction_3_years_oa"])
    #     return response


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

        key = "{}_{}".format(submitted, bronze)
        my_rows = self._scenario_data["oa"][key].get(self.issn_l, [])
        my_recent_rows = self._scenario_data["oa_recent"][key].get(self.issn_l, [])

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
        # if self.issn_l == "0271-678X":
        #     print "self.get_oa_data()", self.get_oa_data()
        my_dict = self.get_oa_data()["green"]
        return [my_dict.get(year, 0) for year in self.historical_years_by_year]

    @cached_property
    def num_green_historical(self):
        return round(np.mean(self.num_green_historical_by_year), 4)

    @cached_property
    def downloads_oa_green(self):
        return round(np.mean(self.downloads_oa_green_by_year), 4)

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

    # @cached_property
    # def downloads_oa_hybrid_by_year(self):
    #     response = [0 for year in self.years]
    #     for year in self.years:
    #         response[year] = sum([(float(self.downloads_per_paper_by_age[age])*self.num_hybrid_historical_by_year[age]) for age in self.years])
    #     return response

    @cached_property
    def downloads_oa_hybrid(self):
        return round(np.mean(self.downloads_oa_hybrid_by_year), 4)

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

    # @cached_property
    # def downloads_oa_bronze_by_year(self):
    #     response = [0 for year in self.years]
    #     for year in self.years:
    #         response[year] = sum([(float(self.downloads_per_paper_by_age[age])*self.num_bronze_historical_by_year[age]) for age in self.years])
    #     return response

    @cached_property
    def downloads_oa_bronze_by_year(self):
        return self.sum_obs_pub_matrix_by_obs(self.oa_bronze_obs_pub)

    @cached_property
    def downloads_oa_bronze_older(self):
        return (self.downloads_total_older_than_five_years/5.0) * (self.downloads_oa_bronze_by_age[4]/(self.downloads_by_age[4]+1))

    @cached_property
    def downloads_oa_green_older(self):
        return (self.downloads_total_older_than_five_years/5.0) * (self.downloads_oa_green_by_age[4]/(self.downloads_by_age[4]+1))

    @cached_property
    def downloads_oa_hybrid_older(self):
        return (self.downloads_total_older_than_five_years/5.0) * (self.downloads_oa_hybrid_by_age[4]/(self.downloads_by_age[4]+1))

    @cached_property
    def downloads_oa_peer_reviewed_older(self):
        return (self.downloads_total_older_than_five_years/5.0) * (self.downloads_oa_peer_reviewed_by_age[4]/(self.downloads_by_age[4]+1))

    @cached_property
    def oa_bronze_obs_pub(self):
        by_age = self.downloads_oa_bronze_by_age
        by_age_old = self.downloads_oa_bronze_older
        growth_scaling = self.growth_scaling_oa_downloads
        my_matrix = self.obs_pub_matrix(by_age, by_age_old, growth_scaling)
        return my_matrix

    @cached_property
    def downloads_oa_hybrid_by_year(self):
        return self.sum_obs_pub_matrix_by_obs(self.oa_hybrid_obs_pub)

    @cached_property
    def oa_hybrid_obs_pub(self):
        by_age = self.downloads_oa_hybrid_by_age
        by_age_old = self.downloads_oa_hybrid_older
        growth_scaling = self.growth_scaling_oa_downloads
        my_matrix = self.obs_pub_matrix(by_age, by_age_old, growth_scaling)
        return my_matrix

    @cached_property
    def downloads_oa_green_by_year(self):
        return self.sum_obs_pub_matrix_by_obs(self.oa_green_obs_pub)

    @cached_property
    def oa_green_obs_pub(self):
        by_age = self.downloads_oa_green_by_age
        by_age_old = self.downloads_oa_green_older
        growth_scaling = self.growth_scaling_oa_downloads
        my_matrix = self.obs_pub_matrix(by_age, by_age_old, growth_scaling)
        return my_matrix

    @cached_property
    def downloads_oa_peer_reviewed_by_year(self):
        return self.sum_obs_pub_matrix_by_obs(self.oa_peer_reviewed_obs_pub)

    @cached_property
    def oa_peer_reviewed_obs_pub(self):
        by_age = self.downloads_oa_peer_reviewed_by_age
        by_age_old = self.downloads_oa_peer_reviewed_older
        growth_scaling = self.growth_scaling_oa_downloads
        my_matrix = self.obs_pub_matrix(by_age, by_age_old, growth_scaling)
        return my_matrix

    @cached_property
    def downloads_oa_bronze(self):
        return round(np.mean(self.downloads_oa_bronze_by_year), 4)

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
        return round(np.mean(self.downloads_oa_peer_reviewed_by_year), 4)

    @cached_property
    def use_oa_peer_reviewed(self):
        return round(self.downloads_oa_peer_reviewed * self.use_weight_multiplier, 4)

    @cached_property
    def is_society_journal(self):
        is_society_journal = self._scenario_data["society"].get(self.issn_l, None)
        return is_society_journal == "YES"

    @cached_property
    def is_hybrid_2019(self):
        return self.journal_metadata.is_hybrid

    @cached_property
    def baseline_access(self):
        from scenario import get_core_list_from_db
        rows = get_core_list_from_db(self.package_id_for_db)
        if not self.issn_l in rows:
            return None
        return rows[self.issn_l]["baseline_access"]


    def to_dict_journals(self):
        table_row = OrderedDict()

        table_row["issn_l"] = self.issn_l
        table_row["title"] = self.title
        table_row["issns"] = self.issns
        table_row["subject"] = self.subject
        table_row["subject_top_three"] = self.subject_top_three
        table_row["subjects_all"] = self.subjects_all
        table_row["subscribed"] = self.subscribed

        table_row["is_society_journal"] = self.is_society_journal
        table_row["institution_id"] = self.institution_id
        table_row["institution_name"] = self.institution_name
        table_row["institution_short_name"] = self.institution_short_name
        table_row["package_id"] = self.package_id

        # some important ones
        table_row["usage"] = round(self.use_total)
        table_row["subscription_cost"] = self.subscription_cost
        table_row["ill_cost"] = self.ill_cost
        table_row["cpu"] = display_cpu(self.cpu)
        table_row["cpu_rank"] = display_cpu(self.cpu_rank)
        table_row["cost"] = self.cost_actual

        # more that show up as columns in table
        table_row["instant_usage_percent"] = self.use_instant_percent
        table_row["free_instant_usage_percent"] = round(self.use_free_instant_percent)
        table_row["subscription_minus_ill_cost"] = round(self.cost_subscription_minus_ill)

        # just used for debugging, frontend calculates this itself
        table_row["use_instant_for_debugging"] = self.use_instant

        # keep this format
        table_row["use_groups_free_instant"] = {"oa": self.use_oa_plus_social_networks, "backfile": self.use_backfile, "social_networks": 0}
        table_row["use_groups_if_subscribed"] = {"subscription": self.use_subscription}
        table_row["use_groups_if_not_subscribed"] = {"ill": self.use_ill, "other_delayed": self.use_other_delayed}

        # fulfillment
        table_row["use_oa_percent"] = round(float(100)*self.use_actual["oa_plus_social_networks"]/self.use_total)
        table_row["use_backfile_percent"] = round(float(100)*self.use_actual["backfile"]/self.use_total)
        table_row["use_subscription_percent"] = round(float(100)*self.use_actual["subscription"]/self.use_total)
        table_row["use_ill_percent"] = round(float(100)*self.use_actual["ill"]/self.use_total)
        table_row["use_other_delayed_percent"] =  round(float(100)*self.use_actual["other_delayed"]/self.use_total)
        table_row["perpetual_access_years_text"] = self.display_perpetual_access_years
        table_row["baseline_access_text"] = self.baseline_access

        # oa
        table_row["use_social_networks_percent"] = round(float(100)*self.use_social_networks/self.use_total)
        table_row["use_green_percent"] = round(float(100)*self.use_oa_green/self.use_total)
        table_row["use_hybrid_percent"] = round(float(100)*self.use_oa_hybrid/self.use_total)
        table_row["use_bronze_percent"] = round(float(100)*self.use_oa_bronze/self.use_total)
        table_row["use_peer_reviewed_percent"] =  round(float(100)*self.use_oa_peer_reviewed/self.use_total)
        table_row["bronze_oa_embargo_months"] = self.bronze_oa_embargo_months
        table_row["is_hybrid_2019"] = self.is_hybrid_2019

        # impact
        table_row["downloads"] = round(self.downloads_total)
        table_row["citations"] = round(self.num_citations, 1)
        table_row["authorships"] = round(self.num_authorships, 1)

        # fuzzed
        table_row["cpu_fuzzed"] = display_cpu(self.cpu_fuzzed)
        table_row["subscription_cost_fuzzed"] = self.cost_subscription_fuzzed
        table_row["subscription_minus_ill_cost_fuzzed"] = self.cost_subscription_minus_ill_fuzzed
        table_row["usage_fuzzed"] = self.use_total_fuzzed
        table_row["downloads_fuzzed"] = self.downloads_fuzzed
        table_row["citations_fuzzed"] = self.num_citations_fuzzed
        table_row["authorships_fuzzed"] = self.num_authorships_fuzzed

        return table_row

    def to_values_journals_for_consortium(self):
        table_row = self.to_dict_journals()
        response = [table_row["package_id"], 'scenario_id', datetime.datetime.utcnow().isoformat(), 
            table_row["issn_l"], table_row["usage"], self.cpu, 'package_id', 'consortium_name', table_row["institution_name"].replace("'", "''"),
            self.institution_short_name, self.institution_id, table_row["subject"], None, table_row["is_society_journal"], 
            table_row["subscription_cost"], table_row["ill_cost"], table_row["use_instant_for_debugging"], self.use_social_networks, 
            self.use_oa_plus_social_networks, self.use_backfile, self.use_subscription, self.use_other_delayed, self.use_ill,
            self.display_perpetual_access_years, self.baseline_access, table_row['use_social_networks_percent'], table_row['use_green_percent'],
            table_row['use_hybrid_percent'], table_row['use_bronze_percent'], table_row['use_peer_reviewed_percent'],
            table_row['bronze_oa_embargo_months'], self.is_hybrid_2019, table_row['downloads'], table_row['citations'], table_row['authorships'],]
        return response


    def to_dict_details(self):
        response = OrderedDict()

        response["top"] = {
                "issn_l": self.issn_l,
                "title": self.title,
                "subject": self.subject,
                "subject_top_three": self.subject_top_three,
                "subjects_all": self.subjects_all,
                # "publisher": self.publisher,
                "is_society_journal": self.is_society_journal,
                "subscribed": self.subscribed,
                "num_papers": self.num_papers,
                "subscription_cost": format_currency(self.subscription_cost),
                "ill_cost": format_currency(self.ill_cost),
                "cost_actual": format_currency(self.cost_actual),
                "cost_subscription_minus_ill": format_currency(self.cost_subscription_minus_ill),
                "cpu": format_currency(self.cpu, True),
                "use_instant_percent": self.use_instant_percent,
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
        response["fulfillment"]["perpetual_access_years_text"] = self.display_perpetual_access_years
        response["fulfillment"]["perpetual_access_years"] = self.perpetual_access_years
        response["fulfillment"]["has_perpetual_access"] = self.has_perpetual_access

        oa_list = []
        oa_list += [OrderedDict([("oa_status", "ResearchGate, etc."),
                                # ("num_papers", round(self.num_oa_historical)),
                                ("usage", format_with_commas(self.use_social_networks)),
                                ("usage_percent", format_percent(round(100*float(self.use_social_networks)/self.use_total)))])]
        for oa_type in ["green", "hybrid", "bronze"]:
            oa_dict = OrderedDict()
            use = self.__getattribute__("use_oa_{}".format(oa_type))
            oa_dict["oa_status"] = oa_type.title()
            # oa_dict["num_papers"] = round(self.__getattribute__("num_{}_historical".format(oa_type)))
            oa_dict["usage"] = format_with_commas(use)
            oa_dict["usage_percent"] = format_percent(round(float(100)*use/self.use_total))
            oa_list += [oa_dict]
        oa_list += [OrderedDict([("oa_status", "*Total*"),
                                # ("num_papers", round(self.num_oa_historical)),
                                ("usage", format_with_commas(self.use_oa_plus_social_networks)),
                                ("usage_percent", format_percent(round(100*float(self.use_oa_plus_social_networks)/self.use_total)))])]
        response["oa"] = {
            "bronze_oa_embargo_months": self.bronze_oa_embargo_months,
            "headers": [
                {"text": "OA Type", "value": "oa_status"},
                # {"text": "Number of papers (annual)", "value": "num_papers"},
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
        for cost_type in ["cost_actual_by_year", "subscription_cost_by_year", "ill_cost_by_year", "cost_subscription_minus_ill_by_year"]:
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
            "cpu": format_currency(self.cpu, True),
            "headers": [
                {"text": "Cost Type", "value": "cost_type"},
                {"text": "Cost (projected annual)", "value": "cost_avg"},
                {"text": "Cost-Type per paid use", "value": "cost_per_use"},
                {"text": "Cost projected 2020", "value": "year_2020"},
                {"text": "2021", "value": "year_2021"},
                {"text": "2022", "value": "year_2022"},
                {"text": "2023", "value": "year_2023"},
                {"text": "2024", "value": "year_2024"},
            ],
            "data": cost_list
            }

        num_papers_list = []
        num_papers_dict = OrderedDict()
        for year in self.years:
            num_papers_dict["year_" + str(2015 + year)] = int(round(self.raw_num_papers_historical_by_year[year]))
        num_papers_list += [num_papers_dict]
        response["num_papers"] = {
            "headers": [
                {"text": "2015", "value": "year_2015"},
                {"text": "2016", "value": "year_2016"},
                {"text": "2017", "value": "year_2017"},
                {"text": "2018", "value": "year_2018"},
                {"text": "2019", "value": "year_2019"},
                {"text": "2020", "value": "year_2020"},
                {"text": "2021", "value": "year_2021"},
                {"text": "2022", "value": "year_2022"},
                {"text": "2023", "value": "year_2023"},
                {"text": "2024", "value": "year_2024"}
            ],
            "data": num_papers_list
            }

        num_papers_list = []
        num_papers_dict = OrderedDict()
        if not self.use_default_num_papers_curve:
            if self.curve_fit_for_num_papers and "x" in self.curve_fit_for_num_papers:
                for i, year in enumerate(self.curve_fit_for_num_papers["x"]):
                    # print self.curve_fit_for_num_papers["x"]
                    # print self.curve_fit_for_num_papers["y_fit"]
                    num_papers_dict["year_" + str(2015 + year)] = int(round(self.curve_fit_for_num_papers["y_fit"][i]))
        for year in self.years:
            num_papers_dict["year_" + str(2020 + year)] = int(round(self.num_papers_by_year[year]))
        num_papers_list += [num_papers_dict]
        response["num_papers_forecast"] = {"headers": response["num_papers"]["headers"], "data": num_papers_list}


        # removed temporarily until can be fixed to work with new ApcJournal init approach
        # from apc_journal import ApcJournal
        # my_apc_journal = ApcJournal(self.issn_l, self._scenario_data)
        # response["apc"] = {
        #     "apc_price": my_apc_journal.apc_price_display,
        #     "annual_projected_cost": my_apc_journal.cost_apc_historical,
        #     "annual_projected_fractional_authorship": my_apc_journal.fractional_authorships_total,
        #     "annual_projected_num_papers": my_apc_journal.num_apc_papers_historical,
        # }

        response["apc"] = {
            "apc_price": None,
            "annual_projected_cost": None,
            "annual_projected_fractional_authorship": None,
            "annual_projected_num_papers": None,
        }

        response_debug = OrderedDict()
        response_debug["num_papers_slope_percent"] = self.num_papers_slope_percent
        response_debug["num_papers_by_year"] = self.num_papers_by_year
        response_debug["oa_data"] = self.get_oa_data()
        response_debug["num_hybrid_historical_by_year"] = self.num_hybrid_historical_by_year
        response_debug["num_hybrid_by_year"] = self.num_hybrid_by_year
        response_debug["scenario_settings"] = self.settings.to_dict()
        response_debug["use_instant_percent"] = self.use_instant_percent
        response_debug["use_instant_percent_by_year"] = self.use_instant_percent_by_year
        response_debug["bronze_oa_embargo_months"] = self.bronze_oa_embargo_months
        response_debug["num_papers"] = self.num_papers
        response_debug["use_weight_multiplier_normalized"] = self.use_weight_multiplier_normalized
        response_debug["use_weight_multiplier"] = self.use_weight_multiplier
        response_debug["downloads_counter_multiplier_normalized"] = self.downloads_counter_multiplier_normalized
        response_debug["downloads_counter_multiplier"] = self.downloads_counter_multiplier
        response_debug["use_instant_by_year"] = self.use_instant_by_year
        response_debug["use_instant_percent_by_year"] = self.use_instant_percent_by_year
        response_debug["use_actual_by_year"] = self.use_actual_by_year
        response_debug["use_actual"] = self.use_actual
        # response_debug["use_oa_green"] = self.use_oa_green
        # response_debug["use_oa_hybrid"] = self.use_oa_hybrid
        # response_debug["use_oa_bronze"] = self.use_oa_bronze
        response_debug["perpetual_access_years"] = self.perpetual_access_years
        response_debug["display_perpetual_access_years"] = self.display_perpetual_access_years
        # response_debug["use_oa_peer_reviewed"] = self.use_oa_peer_reviewed
        response_debug["use_oa"] = self.use_oa
        response_debug["downloads_total_by_year"] = self.downloads_total_by_year
        response_debug["use_default_download_curve"] = self.use_default_download_curve
        response_debug["use_default_num_papers_curve"] = self.use_default_num_papers_curve
        response_debug["curve_fit_for_num_papers"] = self.curve_fit_for_num_papers
        response_debug["curve_fit_for_downloads"] = self.curve_fit_for_downloads
        response_debug["downloads_total_older_than_five_years"] = self.downloads_total_older_than_five_years
        response_debug["raw_downloads_by_age"] = self.raw_downloads_by_age
        response_debug["downloads_by_age"] = self.downloads_by_age
        response_debug["num_papers_by_year"] = self.num_papers_by_year
        response_debug["num_papers_growth_from_2018_by_year"] = self.num_papers_growth_from_2018_by_year
        response_debug["raw_num_papers_historical_by_year"] = self.raw_num_papers_historical_by_year
        response_debug["downloads_oa_by_year"] = self.downloads_oa_by_year
        response_debug["downloads_backfile_by_year"] = self.downloads_backfile_by_year
        response_debug["downloads_obs_pub_matrix"] = self.display_obs_pub_matrix(self.downloads_obs_pub)
        response_debug["oa_obs_pub_matrix"] = self.display_obs_pub_matrix(self.oa_obs_pub)
        response_debug["backfile_obs_pub_matrix"] = self.display_obs_pub_matrix(self.backfile_obs_pub)
        response_debug["use_oa_percent_by_year"] = self.use_oa_percent_by_year
        response_debug["cpu"] = self.cpu
        response_debug["cpu_rank"] = self.cpu_rank
        response_debug["old_school_cpu"] = self.old_school_cpu
        response_debug["old_school_cpu_rank"] = self.old_school_cpu_rank
        response_debug["downloads_oa_by_age"] = self.downloads_oa_by_age
        response_debug["num_oa_historical_by_year"] = self.num_oa_historical_by_year
        response_debug["raw_num_oa_historical_by_year"] = self.raw_num_oa_historical_by_year
        response_debug["num_bronze_by_year"] = self.num_bronze_by_year
        response_debug["num_hybrid_by_year"] = self.num_hybrid_by_year
        response_debug["num_green_by_year"] = self.num_green_by_year
        response_debug["downloads_oa_by_year"] = self.downloads_oa_by_year
        response_debug["downloads_oa_bronze_by_year"] = self.downloads_oa_bronze_by_year
        response_debug["downloads_oa_hybrid_by_year"] = self.downloads_oa_hybrid_by_year
        response_debug["downloads_oa_green_by_year"] = self.downloads_oa_green_by_year
        response_debug["downloads_oa_peer_reviewed_by_year"] = self.downloads_oa_peer_reviewed_by_year
        response_debug["downloads_oa_by_age"] = self.downloads_oa_by_age
        response_debug["downloads_oa_bronze_by_age"] = self.downloads_oa_bronze_by_age
        response_debug["downloads_oa_hybrid_by_age"] = self.downloads_oa_hybrid_by_age
        response_debug["downloads_oa_green_by_age"] = self.downloads_oa_green_by_age
        response_debug["downloads_oa_bronze_older"] = self.downloads_oa_bronze_older
        response_debug["downloads_oa_hybrid_older"] = self.downloads_oa_hybrid_older
        response_debug["downloads_oa_green_older"] = self.downloads_oa_green_older
        response["debug"] = response_debug

        return response


    def __repr__(self):
        return "<{} ({}) {}>".format(self.__class__.__name__, self.issn_l, self.title)



# observation_year 	total views 	total views percent of 2018 	total oa views 	total oa views percent of 2018
# 2018 	25,565,054.38 	1.00 	12,664,693.62 	1.00
# 2019 	28,162,423.76 	1.10 	14,731,000.96 	1.16
# 2020 	30,944,070.68 	1.21 	17,033,520.59 	1.34
# 2021 	34,222,756.60 	1.34 	19,830,049.25 	1.57
# 2022 	38,000,898.80 	1.49 	23,092,284.75 	1.82
# 2023 	42,304,671.82 	1.65 	26,895,794.03 	2.12



