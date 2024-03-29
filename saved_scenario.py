# coding: utf-8

from cached_property import cached_property
import simplejson as json
import datetime
from collections import OrderedDict
from time import time
from sqlalchemy import orm
from psycopg2 import sql
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
register_adapter(dict, Json)

from app import db
from app import get_db_cursor
from scenario import Scenario, openalex_export_concepts
from app import DEMO_PACKAGE_ID
from util import elapsed

def save_raw_scenario_to_db(scenario_id, raw_scenario_definition, ip):
    print("in save_raw_scenario_to_db")
    if scenario_id.startswith("demo"):
        tablename = "jump_scenario_details_demo"
    else:
        tablename = "jump_scenario_details_paid"
    cols = ['scenario_id', 'updated', 'ip', 'scenario_json']
    values = (scenario_id, datetime.datetime.utcnow(), ip, Json(raw_scenario_definition), )
    with get_db_cursor() as cursor:
        qry = sql.SQL("INSERT INTO {} ({}) values ({})").format( 
            sql.Identifier(tablename),
            sql.SQL(', ').join(map(sql.Identifier, cols)),
            sql.SQL(', ').join(sql.Placeholder() * len(cols)))
        print(cursor.mogrify(qry, values))
        cursor.execute(qry, values)

def save_raw_member_institutions_included_to_db(scenario_id, member_institutions_list, ip):
    with get_db_cursor() as cursor:
        cols = ['scenario_id', 'updated', 'ip', 'scenario_members']
        values = (scenario_id, datetime.datetime.utcnow(), ip, Json(member_institutions_list), )
        qry = sql.SQL("INSERT INTO jump_consortium_member_institutions ({}) values ({})").format(
            sql.SQL(', ').join(map(sql.Identifier, cols)),
            sql.SQL(', ').join(sql.Placeholder() * len(cols)))
        cursor.execute(qry, values)

def get_feedback_member_institution_scenario_id(consortium_scenario_id, member_package_id):
    member_institution_scenario_id = "scenario-feedback{}".format(member_package_id)
    member_institution_scenario_id = member_institution_scenario_id.replace("package-", "")
    member_institution_scenario_id = member_institution_scenario_id.replace("publisher-", "")
    return member_institution_scenario_id



def save_feedback_on_member_institutions_included_to_db(consortium_scenario_id, member_institutions_list, ip):
    from saved_scenario import save_raw_scenario_to_db

    scenario_members = json.dumps(member_institutions_list)
    (updated, scenario_raw) = get_latest_scenario_raw(consortium_scenario_id)
    scenario_json = json.dumps(scenario_raw)

    command = ""
    for member_package_id in member_institutions_list:
        member_institution_scenario_id = get_feedback_member_institution_scenario_id(consortium_scenario_id, member_package_id)

        save_raw_scenario_to_db(member_institution_scenario_id, scenario_raw, ip)

        command += """
            UPDATE jump_scenario_details_paid set added_via_pushpull=True WHERE 
            scenario_id=%(member_institution_scenario_id)s;

            DELETE FROM jump_package_scenario WHERE 
            scenario_id=%(member_institution_scenario_id)s;
            
            INSERT INTO jump_package_scenario 
            (package_id, scenario_id, scenario_name, created, is_base_scenario)
            values 
            (%(member_package_id)s, %(member_institution_scenario_id)s, '', sysdate, False);

            DELETE FROM jump_consortium_feedback_requests WHERE 
            consortium_scenario_id=%(consortium_scenario_id)s and member_scenario_id=%(member_institution_scenario_id)s;
            
            INSERT INTO jump_consortium_feedback_requests 
            (consortium_scenario_id, scenario_json, member_package_id, member_scenario_id, sent_date, return_date, ip) 
            values 
            (%(consortium_scenario_id)s, %(scenario_json)s, %(member_package_id)s, %(member_institution_scenario_id)s, sysdate, null, %(ip)s);            
            """

    with get_db_cursor() as cursor:
        cursor.execute(command, {'consortium_scenario_id':consortium_scenario_id, 'scenario_json':scenario_json, 
            'member_package_id':member_package_id, 'member_institution_scenario_id':member_institution_scenario_id, 'ip':ip})

def get_latest_scenario_raw(scenario_id, exclude_added_via_pushpull=False):
    updated = None
    scenario_data = None
    with get_db_cursor() as cursor:
        if exclude_added_via_pushpull:
            # is not True includes false and null, importantly
            command = "select updated,scenario_json from jump_scenario_details_paid where scenario_id=%s and added_via_pushpull is not True order by updated desc limit 1;"
        else:
            command = "select updated,scenario_json from jump_scenario_details_paid where scenario_id=%s order by updated desc limit 1;"
        cursor.execute(command, (scenario_id,))
        rows = cursor.fetchall()

    if rows:
        updated = rows[0]["updated"]
        scenario_data = json.loads(rows[0]["scenario_json"])
        if not "member_added_subrs" in scenario_data:
            scenario_data["member_added_subrs"] = []

    return (updated, scenario_data)


def get_latest_scenario(scenario_id, pkg_id=None, my_jwt=None):
    my_saved_scenario = SavedScenario.query.get(scenario_id)
    if my_saved_scenario:
        package_id = my_saved_scenario.package_id
    elif pkg_id:
        package_id = pkg_id
    else:
        package_id = DEMO_PACKAGE_ID

    if scenario_id.startswith("demo"):
        tablename = "jump_scenario_details_demo"
    else:
        tablename = "jump_scenario_details_paid"
    rows = None
    with get_db_cursor() as cursor:
        qry = sql.SQL("select scenario_json from {} where scenario_id=%s order by updated desc limit 1").format( 
            sql.Identifier(tablename))
        cursor.execute(qry, (scenario_id,))
        rows = cursor.fetchall()

    scenario_data = None

    if rows:
        scenario_data = json.loads(rows[0]["scenario_json"])
        if not "member_added_subrs" in scenario_data:
            scenario_data["member_added_subrs"] = []

    my_scenario = Scenario(package_id, scenario_data, my_jwt=my_jwt)
    return my_scenario


class SavedScenario(db.Model):
    __tablename__ = 'jump_package_scenario'
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"))
    scenario_id = db.Column(db.Text, primary_key=True)
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

    @property
    def scenario_name(self):
        (updated, response) = get_latest_scenario_raw(self.scenario_id)
        if not response:
            return "First Scenario"
        return response["name"]

    def log_timing(self, message):
        self.timing_messages.append("{: <30} {: >6}s".format(message, elapsed(self.section_time, 2)))
        self.section_time = time()

    def save_live_scenario_to_db(self, ip):
        if self.is_demo_account:
            tablename = "jump_scenario_details_demo"
        else:
            tablename = "jump_scenario_details_paid"
        with get_db_cursor() as cursor:
            cols = ['scenario_id', 'updated', 'ip', 'scenario_json']
            values = (self.scenario_id, datetime.datetime.utcnow(), ip, Json(self.to_dict_definition()), )
            qry = sql.SQL("INSERT INTO {} ({}) values ({})").format(
                sql.Identifier(tablename),
                sql.SQL(', ').join(map(sql.Identifier, cols)),
                sql.SQL(', ').join(sql.Placeholder() * len(cols)))
            cursor.execute(qry, values)

    def set_unique_id(self, unique_id):
        self.scenario_id = "demo-scenario-{}".format(unique_id)

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

    @property
    def description(self):
        return self.set_live_scenario().settings.description

    @property
    def notes(self):
        return self.set_live_scenario().settings.notes

    @cached_property
    def is_locked_pending_update(self):
        # Always False for individual scenarios, overridden by Consortium object
        return False

    @cached_property
    def update_notification_email(self):
        # Always None for individual scenarios, overridden by Consortium object
        return None

    @cached_property
    def update_percent_complete(self):
        # Always None for individual scenarios, overridden by Consortium object
        return None

    @cached_property
    def row_for_feedback(self):
        if not self.is_feedback_scenario:
            return []

        command = "select * from jump_consortium_feedback_requests where member_scenario_id=%s"
        with get_db_cursor() as cursor:
            cursor.execute(command, (self.scenario_id,))
            rows_for_feedback = cursor.fetchall()
            return rows_for_feedback[0]

    @cached_property
    def feedback_sent_date(self):
        if not self.row_for_feedback:
            return None
        return self.row_for_feedback["sent_date"]

    @cached_property
    def feedback_return_date(self):
        if not self.row_for_feedback:
            return None
        return self.row_for_feedback["return_date"]

    @cached_property
    def feedback_last_updated(self):
        if not self.row_for_feedback:
            return None
        (updated, response) = get_latest_scenario_raw(self.scenario_id, exclude_added_via_pushpull=True)
        return updated

    def set_live_scenario(self, my_jwt=None):
        if not hasattr(self, "live_scenario") or not self.live_scenario:
            self.live_scenario = get_latest_scenario(self.scenario_id, self.package_id, my_jwt)
        self.live_scenario.package_id = self.package_id
        self.live_scenario.jwt = my_jwt
        return self.live_scenario


    def to_dict_saved_from_db(self):
        (updated, response) = get_latest_scenario_raw(self.scenario_id)
        if not response:
            self.set_live_scenario()  # in case not done
            response = {
                "subrs": [],
                "member_added_subrs": [],
                "customSubrs": [],
                "configs": self.live_scenario.settings.to_dict(),
                "name": self.scenario_name,
                "id": self.scenario_id
            }
        if not "member_added_subrs" in response:
            response["member_added_subrs"] = []
        return response


    def to_dict_saved_freshly_computed(self):
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
            "description": self.description,
            "notes": self.notes,

            # these are used by consortium
            "is_locked_pending_update": self.is_locked_pending_update,
            "update_notification_email": self.update_notification_email,
            "update_percent_complete": self.update_percent_complete,
        }
        return response


    def to_dict_meta(self):
        response = OrderedDict()
        response["scenario_id"] = self.scenario_id
        response["scenario_name"] = self.scenario_name
        response["scenario_description"] = self.description
        response["scenario_notes"] = self.notes

        if self.package.institution_id:
            response["publisher_id"] = self.package_id
            response["publisher_name"] = self.package.package_name
            response["institution_id"] = self.package.institution.id
            response["institution_name"] = self.package.institution.display_name
            response["cost_bigdeal"] = self.package.big_deal_cost
            response["cost_bigdeal_increase"] = self.package.big_deal_cost_increase

        response["scenario_created"] = self.created
        response["is_base_scenario"] = self.is_base_scenario
        response["is_consortial_proposal"] = self.is_feedback_scenario

        return response

    @cached_property
    def is_feedback_scenario(self):
        return self.scenario_id.startswith("scenario-feedback")

    def to_dict_feedback(self):
        response = {
			"sent_date": self.feedback_sent_date,
			"changed_date": self.feedback_last_updated,
			"return_date": self.feedback_return_date
        }
        return response


    def to_dict_journals(self, gather_export_concepts = False):
        self.set_live_scenario()  # in case not done

        if gather_export_concepts:
            self.live_scenario.data['concepts'] = openalex_export_concepts(self.live_scenario.data['concepts'], self.live_scenario.my_package.unique_issns)
            [setattr(j, 'subject_top_three', self.live_scenario.data['concepts'].get(j.issn_l, {}).get("top_three", "")) for j in self.live_scenario.journals]
            [setattr(j, 'subjects_all', self.live_scenario.data['concepts'].get(j.issn_l, {}).get("all", "")) for j in self.live_scenario.journals]

        response = OrderedDict()
        response["meta"] = self.to_dict_meta()

        response["_debug"] = {"summary": self.live_scenario.to_dict_summary_dict()}

        response["saved"] = self.to_dict_saved_from_db()

        response["consortial_proposal_dates"] = self.to_dict_feedback()

        response["journals"] = [j.to_dict_journals() for j in self.live_scenario.journals_sorted_cpu]

        # these are used by consortium
        response["is_locked_pending_update"] = self.is_locked_pending_update
        response["update_notification_email"] = self.update_notification_email
        response["update_percent_complete"] = self.update_percent_complete

        response["warnings"] = self.package_real.warnings

        try:
            self.log_timing("to dict")
            response["_timing"] = self.timing_messages
        except AttributeError:
            response["_timing"] = ["timing unknown. self.timing_messages didn't load"]

        return response

    def __repr__(self):
        return "<{} ({}) {}>".format(self.__class__.__name__, self.scenario_id, self.scenario_name)



