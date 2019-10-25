from cached_property import cached_property
import numpy as np

class Journal(object):
    def __init__(self, journals_dict, settings):
        for (k, v) in journals_dict.iteritems():
            setattr(self, k, v)
        self.settings = settings
        self.subscribed = False

    def set_subscribe(self):
        self.subscribed = True

    @cached_property
    def usage_total(self):
        return sum(self.unweighted_usage["total"])

    @cached_property
    def notfreenotinstant_total(self):
        return sum(self.unweighted_usage["notfreenotinstant"])

    @cached_property
    def subscription_price_per_year(self):
        response = [((1+self.settings.alacart_cost_increase)**year) *
                                            self.dollars_2018_subscription
                                            for year in range(0,5)]
        return response

    @cached_property
    def subscription_price_average(self):
        return np.mean(self.subscription_price_per_year)

    @cached_property
    def subscription_cost_per_paid_use(self):
        if not self.notfreenotinstant_total:
            return None
        return self.subscription_price_average/self.notfreenotinstant_total

    def to_dict(self):
        return [self.issn_l, self.subscription_price_average, self.subscription_cost_per_paid_use, self.subscribed]

    def __repr__(self):
        return u"{} ({}) {}".format(self.__class__.__name__, self.issn_l, self.name)
