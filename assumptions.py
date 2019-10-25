from cached_property import cached_property
import numpy as np

class Assumptions(object):
    def __init__(self):
        self.docdel_cost = 25
        self.ill_cost = 5
        self.ill_request_percent = 0.1
        self.bigdeal_cost_increase = 0.05
        self.alacart_cost_increase = 0.08
        self.bigdeal_cost = 2200000
        self.include_docdel = False
        self.weight_citation = 0
        self.weight_authorship = 0
        self.docdel_cost = 0

    def set_assumption(self, key, value):
        return self.__setattr__(key, value)

    def to_dict(self):
        return self.__dict__

    def __repr__(self):
        return u"{}".format(self.__class__.__name__)

