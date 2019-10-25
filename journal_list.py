from cached_property import cached_property
import numpy as np
from journal import Journal

def for_sorting(x):
    if x is None:
        return float('inf')
    return x

class JournalList(object):
    def __init__(self, journals_dicts, settings):
        self.journals = [Journal(my_dict, settings) for my_dict in journals_dicts]
        self.settings = settings

    @property
    def journals_by_value(self):
        return sorted(self.journals, key=lambda k: for_sorting(k.subscription_cost_per_paid_use), reverse=False)

    @property
    def subscribed(self):
        return [j for j in self.journals_by_value if j.subscribed]

    @property
    def cost(self):
        return sum([j.subscription_price_average for j in self.journals_by_value if j.subscribed])

    def __repr__(self):
        return u"{} (n={})".format(self.__class__.__name__, len(self.journals))
