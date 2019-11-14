# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
from collections import OrderedDict
import datetime
import shortuuid

from app import db
from scenario import Scenario

class Package(db.Model):
    __tablename__ = 'jump_account_package'
    account_id = db.Column(db.Text, db.ForeignKey("jump_account.id"))
    package_id = db.Column(db.Text, primary_key=True)
    publisher = db.Column(db.Text)
    package_name = db.Column(db.Text)
    created = db.Column(db.DateTime)

    def __init__(self, **kwargs):
        self.id = shortuuid.uuid()[0:8]
        self.created = datetime.datetime.utcnow().isoformat()
        super(Package, self).__init__(**kwargs)

    @property
    def package_id_old(self):
        lookup = {
            "658349d9": "uva_elsevier",
            "15d18dca": "suny_elsevier",
            "00cbd1bb": "uva_elsevier", #demo
            "51e8103d":	"mit_elsevier"
        }
        return lookup.get(self.package_id, self.package_id)

    @property
    def is_demo_account(self):
        return self.account_id == "demo"

    @property
    def scenarios(self):
        # TODO
        my_scenario = Scenario("uva_elsevier")
        my_scenario.package_id = self.package_id
        return [my_scenario]

    def to_dict_summary(self):
        return {
                "id": self.package_id,
                "name": self.package_name,
                "hasCounterData": True,  #TODO
                "numJournals": 42, #TODO
                "numPerpAccessJournals": 42 #TODO
        }

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.package_id, self.package_name)


