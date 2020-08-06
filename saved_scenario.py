# coding: utf-8

from cached_property import cached_property
import simplejson as json
import datetime
import shortuuid
from flask_jwt_extended import jwt_required, jwt_optional, create_access_token, get_jwt_identity
from collections import OrderedDict
from time import time
from sqlalchemy import orm

from app import db
from app import get_db_cursor
from scenario import Scenario
from app import DEMO_PACKAGE_ID
from util import elapsed

def save_raw_scenario_to_db(scenario_id, raw_scenario_definition, ip):
    scenario_json = json.dumps(raw_scenario_definition)

    if scenario_id.startswith("demo"):
        tablename = "jump_scenario_details_demo"
    else:
        tablename = "jump_scenario_details_paid"
    scenario_json = scenario_json.replace("'", "''")
    with get_db_cursor() as cursor:
        command = u"""INSERT INTO {} (scenario_id, updated, ip, scenario_json) values ('{}', sysdate, '{}', '{}');""".format(
            tablename, scenario_id, ip, scenario_json
        )
        # print command
        cursor.execute(command)

def save_raw_member_institutions_included_to_db(scenario_id, member_institutions_list, ip):
    scenario_members = json.dumps(member_institutions_list)

    with get_db_cursor() as cursor:
        command = u"""INSERT INTO jump_consortium_member_institutions (scenario_id, updated, ip, scenario_members) values ('{}', sysdate, '{}', '{}');""".format(
            scenario_id, ip, scenario_members
        )
        # print command
        cursor.execute(command)


def get_latest_scenario_raw(scenario_id):
    scenario_data = None
    with get_db_cursor() as cursor:
        command = u"""select scenario_json from jump_scenario_details_paid where scenario_id='{}' order by updated desc limit 1;""".format(
            scenario_id
        )
        # print command
        cursor.execute(command)
        rows = cursor.fetchall()

    if rows:
        scenario_data = json.loads(rows[0]["scenario_json"])

    return scenario_data


def get_latest_scenario(scenario_id, my_jwt=None):
    my_saved_scenario = SavedScenario.query.get(scenario_id)
    if my_saved_scenario:
        package_id = my_saved_scenario.package_id
    else:
        package_id = DEMO_PACKAGE_ID

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

    my_scenario = Scenario(package_id, scenario_data, my_jwt=my_jwt)
    return my_scenario


class SavedScenario(db.Model):
    __tablename__ = 'jump_package_scenario'
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"))
    scenario_id = db.Column(db.Text, primary_key=True)
    scenario_name = db.Column(db.Text)
    created = db.Column(db.DateTime)
    is_base_scenario = db.Column(db.Boolean)

    def __init__(self, is_demo_account, scenario_id, scenario_input):
        # if is_demo_account:
        #     demo_saved_scenario = SavedScenario.query.get("demo")
        #     self.scenario_name = demo_saved_scenario.scenario_name
        #     self.package_id = DEMO_PACKAGE_ID
        self.created = datetime.datetime.utcnow().isoformat()
        self.scenario_id = scenario_id
        self.scenario_input = scenario_input
        self.live_scenario = None

    @orm.reconstructor
    def on_load(self):
        self.timing_messages = [];
        self.section_time = time()

    def log_timing(self, message):
        self.timing_messages.append("{: <30} {: >6}s".format(message, elapsed(self.section_time, 2)))
        self.section_time = time()

    def save_live_scenario_to_db(self, ip):
        if self.is_demo_account:
            tablename = "jump_scenario_details_demo"
        else:
            tablename = "jump_scenario_details_paid"
        scenario_json = json.dumps(self.to_dict_definition())
        print "got json"
        scenario_json = scenario_json.replace("'", "''")
        # print "\n\n scenario_json", scenario_json
        with get_db_cursor() as cursor:
            print "got cursor"
            command = u"""INSERT INTO {} (scenario_id, updated, ip, scenario_json) values ('{}', sysdate, '{}', '{}');""".format(
                tablename, self.scenario_id, ip, scenario_json
            )
            # print command
            cursor.execute(command)
        print "done with execute"


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

    @property
    def institution_name(self):
        return self.set_live_scenario().institution_name

    def set_live_scenario(self, my_jwt=None):
        if not hasattr(self, "live_scenario") or not self.live_scenario:
            self.live_scenario = get_latest_scenario(self.scenario_id, my_jwt)
        self.live_scenario.package_id = self.package_id
        self.live_scenario.jwt = my_jwt
        return self.live_scenario


    def to_dict_saved(self):
        self.set_live_scenario()  # in case not done

        response = {
            "subrs": [j.issn_l for j in self.live_scenario.subscribed_bulk],
            "customSubrs": [j.issn_l for j in self.live_scenario.subscribed_custom],
            "configs": self.live_scenario.settings.to_dict(),
            "name": self.scenario_name,
            "id": self.scenario_id
        }
        return response


    def to_dict_definition(self):
        self.set_live_scenario()  # in case not done

        response = {
            "id": self.scenario_id,
            "name": self.scenario_name,
            "pkgId": self.package_id,
            "summary": self.live_scenario.to_dict_summary_dict(),
            "subrs": [j.issn_l for j in self.live_scenario.subscribed_bulk],
            "customSubrs": [j.issn_l for j in self.live_scenario.subscribed_custom],
            "configs": self.live_scenario.settings.to_dict(),
            "_debug": {
                "package_name": self.package_real.package_name
            }
        }
        return response

    def to_dict_minimal(self):
        response = {
            "id": self.scenario_id,
            "name": self.scenario_name,
            "pkgId": self.package_id,
            "saved": self.to_dict_saved()
        }
        return response

    def to_dict_micro(self):
        response = {
            "id": self.scenario_id,
            "name": self.scenario_name,
        }
        return response

    def to_dict_meta(self):
        response = OrderedDict()
        response["scenario_id"] = self.scenario_id
        response["scenario_name"] = self.scenario_name

        if self.package.institution_id:
            response["publisher_id"] = self.package_id
            response["publisher_name"] = self.package.package_name
            response["institution_id"] = self.package.institution.id
            response["institution_name"] = self.package.institution.display_name

        response["scenario_created"] = self.created
        response["is_base_scenario"] = self.is_base_scenario
        return response

    def to_dict_journals(self):
        response = OrderedDict()
        response["meta"] = self.to_dict_meta()
        response["saved"] = self.to_dict_saved()
        response["journals"] = [j.to_dict_journals() for j in self.live_scenario.journals_sorted_ncppu]
        response["_debug"] = {"summary": self.live_scenario.to_dict_summary_dict()}
        self.log_timing("to dict")
        response["_timing"] = self.timing_messages
        return response

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.scenario_id, self.scenario_name)


def default_scenario(package_id, created=None):
    created = created or datetime.datetime.utcnow().isoformat()

    new_scenario = SavedScenario(False, u'scenario-{}'.format(shortuuid.uuid()[0:12]), None)
    new_scenario.package_id = package_id
    new_scenario.scenario_name = u'First Scenario'
    new_scenario.created = created
    new_scenario.is_base_scenario = True

    return new_scenario
