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

class SavedScenario(db.Model):
    __tablename__ = 'jump_package_scenario'
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"))
    scenario_id = db.Column(db.Text, primary_key=True)
    scenario_name = db.Column(db.Text)
    created = db.Column(db.DateTime)

    def __init__(self, **kwargs):
        self.scenario_id = shortuuid.uuid()[0:8]
        self.created = datetime.datetime.utcnow().isoformat()
        super(SavedScenario, self).__init__(**kwargs)

    @property
    def package_id_old(self):
        lookup = {
            "658349d9": "uva_elsevier",
            "15d18dca": "suny_elsevier",
            "00cbd1bb": "uva_elsevier", #demo
            "51e8103d":	"mit_elsevier"
        }
        return lookup.get(self.package_id, self.package_id)

    def to_dict_definition(self):
        live_scenario = Scenario("uva_elsevier") #TODO

        response = {
            "id": self.scenario_id,
            "name": self.scenario_name,
            "pkgId": self.package_id,
            "summary": {
                "cost_percent": live_scenario.cost_spent_percent,
                "use_instant_percent": live_scenario.use_instant_percent,
                "num_journals_subscribed": len(live_scenario.subscribed),
            },
            "subrs": live_scenario.subscribed,
            "customSubrs": [],
            "configs": live_scenario.settings.to_dict()
        }
        return response

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.scenario_id, self.scenario_name)


