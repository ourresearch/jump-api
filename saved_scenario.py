# coding: utf-8

from cached_property import cached_property
import simplejson as json
import datetime
import shortuuid

from app import db
from app import get_db_cursor
from scenario import Scenario


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

    old_package_id = "658349d9"

    my_scenario = Scenario(old_package_id, scenario_data)
    return my_scenario



class SavedScenario(db.Model):
    __tablename__ = 'jump_package_scenario'
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"))
    scenario_id = db.Column(db.Text, primary_key=True)
    scenario_name = db.Column(db.Text)
    created = db.Column(db.DateTime)

    def __init__(self, is_demo_account, scenario_id, scenario_input):
        if is_demo_account:
            demo_saved_scenario = SavedScenario.query.get("demo")
            self.scenario_name = demo_saved_scenario.scenario_name
        self.created = datetime.datetime.utcnow().isoformat()
        self.scenario_input = scenario_input
        self.live_scenario = None

    def save_live_scenario_to_db(self, ip):
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

    def set_unique_id(self, unique_id):
        self.scenario_id = u"demo-scenario-{}".format(unique_id)

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
            "summary": self.live_scenario.to_dict_summary_dict(),
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


