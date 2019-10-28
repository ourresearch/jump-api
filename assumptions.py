# coding: utf-8

from cached_property import cached_property
import numpy as np

class Assumptions(object):
    def __init__(self, http_request_args=None):
        self.cost_docdel = 25
        self.cost_ill = 5
        self.cost_bigdeal_increase = 0.05
        self.cost_alacart_increase = 0.08
        self.cost_bigdeal = 2200000
        self.cost_content_fee_percent = .057
        self.ill_request_percent_of_delayed = 0.1
        self.social_networks_percent = 0.1
        self.weight_citation = 10
        self.weight_authorship = 100
        self.include_docdel = False
        self.include_bronze = True
        self.include_submitted_version = True

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

