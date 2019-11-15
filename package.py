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
    scenarios = db.relationship('SavedScenario', lazy='subquery', backref=db.backref("package", lazy="subquery"))

    def __init__(self, **kwargs):
        self.id = shortuuid.uuid()[0:8]
        self.created = datetime.datetime.utcnow().isoformat()
        super(Package, self).__init__(**kwargs)

    @property
    def package_id_old(self):
        lookup = {
            "658349d9": "uva_elsevier",
            "15d18dca": "suny_elsevier",
            "demo": "uva_elsevier", #demo
            "51e8103d":	"mit_elsevier"
        }
        return lookup.get(self.package_id, self.package_id)

    @property
    def is_demo_account(self):
        return self.package_id == "demo"

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


