# coding: utf-8

from cached_property import cached_property
import simplejson as json
import datetime
import shortuuid

from app import db
from app import get_db_cursor
from scenario import Scenario

package_lookup = {
    "658349d9": "uva_elsevier",
    "15d18dca": "suny_elsevier",
    "demo": "uva_elsevier", #demo
    "51e8103d":	"mit_elsevier"
}

def get_latest_scenario(scenario_id):
    if scenario_id.startswith("demo"):
        tablename = "jump_scenario_details_demo"
    else:
        tablename = "jump_scenario_details_paid"
    rows = None
    with get_db_cursor() as cursor:
        command = u"""select scenario_json from {} where scenario_id='{}' order by updated desc limit 1;""".format(
            tablename, scenario_id
        )
        # print command
        cursor.execute(command)
        rows = cursor.fetchall()

    scenario_data = None

    if rows:
        scenario_data = json.loads(rows[0]["scenario_json"])

    if scenario_data:
        old_package_id = package_lookup.get(scenario_data["pkgId"], scenario_data["pkgId"])
    else:
        old_package_id = "uva_elsevier"
    my_scenario = Scenario(old_package_id, scenario_data)
    return my_scenario



class SavedScenario(db.Model):
    __tablename__ = 'jump_package_scenario'
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"))
    scenario_id = db.Column(db.Text, primary_key=True)
    scenario_name = db.Column(db.Text)
    created = db.Column(db.DateTime)

    def __init__(self, is_demo, scenario_id, scenario_input):
        self.created = datetime.datetime.utcnow().isoformat()
        self.scenario_input = scenario_input
        self.live_scenario = None

    def save_to_db(self, ip):
        if not self.scenario_id or self.scenario_id=="demo":
            if self.is_demo_account:
                self.scenario_id = "demo"+shortuuid.uuid()[0:20]
            else:
                self.scenario_id = shortuuid.uuid()[0:8]

        if self.is_demo_account:
            tablename = "jump_scenario_details_demo"
        else:
            tablename = "jump_scenario_details_paid"
        scenario_json = json.dumps(self.to_dict_definition())
        with get_db_cursor() as cursor:
            command = u"""INSERT INTO {} (scenario_id, updated, ip, scenario_json) values ('{}', sysdate, '{}', '{}');""".format(
                tablename, self.scenario_id, ip, scenario_json
            )
            # print command
            cursor.execute(command)

    @property
    def is_demo_account(self):
        return self.package_real.is_demo_account

    @property
    def package_real(self):
        from package import Package

        if self.package:
            return self.package
        return Package.query.get("demo")

    @property
    def journals(self):
        return self.set_live_scenario().journals

    def set_live_scenario(self):
        if not hasattr(self, "live_scenario") or not self.live_scenario:
            self.live_scenario = get_latest_scenario(self.scenario_id)
        return self.live_scenario

    @property
    def package_id_old(self):
        return package_lookup.get(self.package_id, self.package_id)

    def to_dict_definition(self):
        if not hasattr(self, "live_scenario") or not self.live_scenario:
            self.live_scenario = get_latest_scenario(self.scenario_id)

        response = {
            "id": self.scenario_id,
            "name": self.scenario_name,
            "pkgId": self.package_id,
            "summary": {
                "cost_percent": self.live_scenario.cost_spent_percent,
                "use_instant_percent": self.live_scenario.use_instant_percent,
                "num_journals_subscribed": len(self.live_scenario.subscribed),
            },
            "subrs": [j.issn_l for j in self.live_scenario.subscribed],
            "customSubrs": [],
            "configs": self.live_scenario.settings.to_dict(),
            "_debug": {
                "package_name": self.package_real.package_name
            }
        }
        return response

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.scenario_id, self.scenario_name)


