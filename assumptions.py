# coding: utf-8

from cached_property import cached_property
import numpy as np

from util import str2bool

class Assumptions(object):
    def __init__(self, http_request_args=None):
        self.cost_ill = 5
        self.cost_bigdeal_increase = 5 #percent
        self.cost_alacart_increase = 8 #percent
        self.cost_bigdeal = 2200000
        self.cost_content_fee_percent = 5.7
        self.ill_request_percent_of_delayed = 10
        self.weight_citation = 10
        self.weight_authorship = 100
        self.include_bronze = True
        self.include_submitted_version = True
        self.include_social_networks = True
        self.include_backfile = True

        if http_request_args:
            if "configs" in http_request_args:
                http_request_args = http_request_args["configs"]

            for input_key in http_request_args:
                value = http_request_args.get(input_key)
                self.set_assumption(input_key, value)
            self.package = http_request_args.get("package", None)  # so get demo if that's what was used

    def set_assumption(self, key, value):
        if key.startswith("include_"):
            if isinstance(value, int):
                self.__setattr__(key, value != 0)
            else:
                try:
                    self.__setattr__(key, str2bool(value))
                except:
                    self.__setattr__(key, value)
        else:
            try:
                self.__setattr__(key, float(value))
            except:
                self.__setattr__(key, value)


    def to_dict(self):
        return self.__dict__

    def __repr__(self):
        return u"{}".format(self.__class__.__name__)

