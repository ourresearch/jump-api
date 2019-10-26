# coding: utf-8

from cached_property import cached_property
import numpy as np

class Assumptions(object):
    def __init__(self, http_request_args=None):
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

        if http_request_args:
            for key in http_request_args:
                try:
                    self.set_assumption(key, float(http_request_args.get(key)))
                except ValueError:
                    self.set_assumption(key, http_request_args.get(key))
            self.package = http_request_args.get("package", None)  # so get demo if that's what was used


    def set_assumption(self, key, value):
        return self.__setattr__(key, value)

    def to_dict(self):
        return self.__dict__

    def __repr__(self):
        return u"{}".format(self.__class__.__name__)

