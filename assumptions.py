# coding: utf-8

from util import str2bool

DEFAULT_COST_BIGDEAL = 2100000

class Assumptions(object):
    def __init__(self, http_request_args=None, currency="USD"):
        if currency == "GBP":
            self.cost_ill = 12
        else:
            self.cost_ill = 17
        self.cost_bigdeal_increase = 5  # percent
        self.cost_alacart_increase = 8  # percent
        self.cost_bigdeal = DEFAULT_COST_BIGDEAL
        self.cost_content_fee_percent = 5.7
        self.ill_request_percent_of_delayed = 5
        self.weight_citation = 10
        self.weight_authorship = 100
        self.backfile_contribution = 100  # percent
        self.include_bronze = True
        self.include_submitted_version = True
        self.include_social_networks = True
        self.include_backfile = True
        self.description = u""
        self.notes = u""
        # self.consortium_name = None  # for consortium?
        # self.min_bundle_size = True

        if http_request_args:
            if "configs" in http_request_args:
                http_request_args = http_request_args["configs"]

            for input_key in http_request_args:
                if input_key not in ["jwt"]:
                    value = http_request_args.get(input_key)
                    self.set_assumption(input_key, value)

    def set_assumption(self, key, value):
        if value is None or value == '':
            return

        if key.startswith("include_"):
            if isinstance(value, int):
                self.__setattr__(key, value != 0)
            else:
                try:
                    self.__setattr__(key, str2bool(value))
                except AttributeError:
                    self.__setattr__(key, value)
        else:
            try:
                self.__setattr__(key, float(value))
            except (ValueError, TypeError):
                self.__setattr__(key, value)

    def to_dict(self):
        my_dict = self.__dict__
        if "email" in my_dict:
            del my_dict["email"]
        return my_dict

    def __repr__(self):
        return u"{}".format(self.__class__.__name__)

