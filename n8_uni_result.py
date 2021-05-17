# coding: utf-8

from collections import defaultdict
from collections import OrderedDict
from time import time

from saved_scenario import SavedScenario

class N8UniResult(object):

    def __init__(self, jusp_id):
        self.jusp_id = jusp_id
        self.scenario_id_ownpta = u"scenario-n8els_{}_ownpta".format(jusp_id)
        self.saved_scenario_ownpta = SavedScenario.query.get(self.scenario_id_ownpta)
        self.saved_scenario_ownpta.set_live_scenario()  # in case not done

        self.scenario_id_grouppta = u"scenario-n8els_{}_grouppta".format(jusp_id)
        self.saved_scenario_grouppta = SavedScenario.query.get(self.scenario_id_grouppta)
        self.saved_scenario_grouppta.set_live_scenario()  # in case not done


    def to_list(self):

        response = []
        response += [self.jusp_id]
        response += [self.num_own_subscriptions()]
        response += [self.num_group_subscriptions()]
        response += [self.num_own_backfile_journals()]
        response += [self.num_group_backfile_journals()]
        response += [self.percent_oa()]
        response += [self.percent_own_backfile()]
        response += [self.percent_own_subscriptions()]
        response += [self.percent_group_subscriptions()]
        response += [self.percent_group_backfile()]
        response += [self.usage()]
        response += [self.subscription_cost()]
        response += [self.big_deal_cost()]
        print response
        return response

    def num_own_subscriptions(self):
        # print self.saved_scenario_ownpta
        # print self.saved_scenario_ownpta.package_id
        # print self.saved_scenario_ownpta.live_scenario
        # print len(self.saved_scenario_ownpta.live_scenario.subscribed)
        return len(self.saved_scenario_ownpta.live_scenario.subscribed)

    def num_group_subscriptions(self):
        return len(self.saved_scenario_grouppta.live_scenario.subscribed)

    def num_own_backfile_journals(self):
        journal_has_backfile = [my_journal.issn_l for my_journal in self.saved_scenario_ownpta.journals if my_journal.perpetual_access_years]
        return len(journal_has_backfile)

    def num_group_backfile_journals(self):
        journal_has_backfile = [my_journal.issn_l for my_journal in self.saved_scenario_grouppta.journals if my_journal.perpetual_access_years]
        return len(journal_has_backfile)

    def percent_oa(self):
        return round(self.saved_scenario_ownpta.live_scenario.use_oa/self.saved_scenario_ownpta.live_scenario.use_total, 3)

    def percent_own_backfile(self):
        return round(self.saved_scenario_ownpta.live_scenario.use_backfile/self.saved_scenario_ownpta.live_scenario.use_total, 3)

    def percent_own_subscriptions(self):
        return round(self.saved_scenario_ownpta.live_scenario.use_subscription_percent/100.0, 3)

    def percent_group_subscriptions(self):
        return round(self.saved_scenario_grouppta.live_scenario.use_subscription_percent/100.0, 3)

    def percent_group_backfile(self):
        total_plus_group = round(self.saved_scenario_grouppta.live_scenario.use_backfile/self.saved_scenario_grouppta.live_scenario.use_total, 3)
        return round(total_plus_group - self.percent_own_backfile(), 3)

    def usage(self):
        return round(self.saved_scenario_ownpta.live_scenario.use_total, 0)

    def subscription_cost(self):
        running_cost = 0
        for my_journal in self.saved_scenario_ownpta.live_scenario.journals:
            if my_journal.subscribed:
                if my_journal.cost_actual < 100000:
                    running_cost += my_journal.cost_actual
                else:
                    # median price 2051.0 pounds, average price 2395 pounds
                    running_cost += 2395.0
        return round(running_cost, 0)
        # return self.saved_scenario_ownpta.live_scenario.cost_actual_subscription

    def big_deal_cost(self):
        return round(self.saved_scenario_ownpta.live_scenario.cost_bigdeal_projected, 0)

