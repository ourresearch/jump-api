# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
from collections import OrderedDict
import datetime
import shortuuid

from app import db
from saved_scenario import SavedScenario

class Package(db.Model):
    __tablename__ = 'jump_account_package'
    account_id = db.Column(db.Text, db.ForeignKey("jump_account.id"))
    package_id = db.Column(db.Text, primary_key=True)
    publisher = db.Column(db.Text)
    package_name = db.Column(db.Text)
    created = db.Column(db.DateTime)
    saved_scenarios = db.relationship('SavedScenario', lazy='subquery', backref=db.backref("package", lazy="subquery"))

    def __init__(self, **kwargs):
        self.created = datetime.datetime.utcnow().isoformat()
        super(Package, self).__init__(**kwargs)

    @property
    def unique_saved_scenarios(self):
        response = self.saved_scenarios
        if self.is_demo_account:
            unique_saved_scenarios = self.saved_scenarios
            unique_key = self.package_id.replace("demo", "").replace("-package-", "")
            for my_scenario in unique_saved_scenarios:
                my_scenario.package_id = self.package_id
                my_scenario.scenario_id = u"demo-scenario-{}".format(unique_key)
            response = unique_saved_scenarios
        return response

    @property
    def is_demo_account(self):
        return self.package_id.startswith("demo")

    @property
    def has_counter_data(self):
        return self.num_journals > 0

    @property
    def num_journals(self):
        return len(self.saved_scenarios[0].journals)

    @property
    def num_perpetual_access_journals(self):
        # self TODO
        return self.num_journals

    def to_dict_summary(self):
        return {
                "id": self.package_id,
                "name": self.package_name,
                "hasCounterData": self.has_counter_data,
                "numJournals": self.num_journals,
                "numPerpAccessJournals": self.num_perpetual_access_journals
        }

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.package_id, self.package_name)


