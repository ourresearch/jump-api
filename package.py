# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
from collections import OrderedDict
import datetime
import shortuuid
import json
from time import time

from app import db
from app import get_db_cursor
from app import DEMO_PACKAGE_ID
from app import s3_client

# from app import my_memcached # disable memcached
from assumptions import Assumptions
from counter import CounterInput
from journal_price import JournalPriceInput
from perpetual_access import PerpetualAccessInput
from saved_scenario import SavedScenario # used in relationship
from institution import Institution  # used in relationship
from scenario import get_hybrid_2019
from scenario import get_ricks_journal
from scenario import get_prices_from_cache
from scenario import get_core_list_from_db
from scenario import get_perpetual_access_from_cache
from util import get_sql_answers
from util import get_sql_rows
from util import get_sql_dict_rows, safe_commit


def get_ids():
    rows = get_sql_dict_rows("""select * from jump_account_package_scenario_view order by username""")
    return rows

class Package(db.Model):
    __tablename__ = "jump_account_package"
    institution_id = db.Column(db.Text, db.ForeignKey("jump_institution.id"))
    package_id = db.Column(db.Text, primary_key=True)
    publisher = db.Column(db.Text)
    package_name = db.Column(db.Text)
    consortium_package_id = db.Column(db.Text)
    created = db.Column(db.DateTime)
    is_demo = db.Column(db.Boolean)
    big_deal_cost = db.Column(db.Numeric)
    is_deleted = db.Column(db.Boolean)
    currency = db.Column(db.Text)
    is_dismissed_warning_missing_perpetual_access = db.Column(db.Boolean)
    is_dismissed_warning_missing_prices = db.Column(db.Boolean)

    saved_scenarios = db.relationship("SavedScenario", lazy="subquery", backref=db.backref("package", lazy="subquery"))
    institution = db.relationship("Institution", lazy="subquery", uselist=False)

    def __init__(self, **kwargs):
        self.created = datetime.datetime.utcnow().isoformat()
        self.is_deleted = False
        self.currency = "USD"
        super(Package, self).__init__(**kwargs)

    @property
    def unique_saved_scenarios(self):
        response = self.saved_scenarios

    @property
    def scenario_ids(self):
        return [s.scenario_id for s in self.saved_scenarios]

    @property
    def is_demo_account(self):
        return self.package_id.startswith("demo")

    @property
    def has_counter_data(self):
        return True

    @property
    def has_custom_perpetual_access(self):
        # perpetual_access_rows = get_perpetual_access_from_cache([self.package_id])
        perpetual_access_rows = get_perpetual_access_from_cache(self.package_id)
        if perpetual_access_rows:
            return True
        from app import suny_consortium_package_ids
        if self.package_id in suny_consortium_package_ids or self.consortium_package_id in suny_consortium_package_ids:
            return True
        return False

    @property
    def has_custom_prices(self):
        package_ids = [x for x in [self.package_id, self.consortium_package_id] if x]
        if package_ids:
            prices_rows = get_prices_from_cache(package_ids, self.publisher)
            package_ids_with_prices = prices_rows.keys()
            if self.package_id in package_ids_with_prices or self.consortium_package_id in package_ids_with_prices:
                return True
        from app import suny_consortium_package_ids
        if self.package_id in suny_consortium_package_ids:
            return True
        return False

    @property
    def has_core_journal_list(self):
        rows = get_core_list_from_db(self.package_id)
        if rows:
            return True
        return False

    def filter_by_core_list(self, my_list):
        if not self.has_core_journal_list:
            return my_list
        core_rows = get_core_list_from_db(self.package_id)
        core_issnls = core_rows.keys()
        return [row for row in my_list if row["issn_l"] in core_issnls]

    @cached_property
    def get_core_journal_rows(self):
        q = """
            select 
            core.issn_l, 
            title as title
            from jump_core_journals core
            left outer join ricks_journal on core.issn_l = ricks_journal.issn_l
            where package_id='{package_id}' 
            order by title desc
            """.format(package_id=self.package_id_for_db)
        rows = get_sql_dict_rows(q)
        return rows

    @cached_property
    def get_counter_rows(self):
        return self.filter_by_core_list(self.get_unfiltered_counter_rows)

    @cached_property
    def get_unfiltered_counter_rows(self):
        q = """
           select
           rj.issn_l,
           listagg(rj.issn, ',') as issns,
           listagg(title, ',') as title, 
           sum(total::int) as num_2018_downloads
           from jump_counter counter
           left outer join ricks_journal_flat rj on counter.issn_l = rj.issn
           where package_id='{package_id}'
           group by rj.issn_l           
           order by num_2018_downloads desc
           """.format(package_id=self.package_id_for_db)
        return get_sql_dict_rows(q)

    def get_base(self, and_where=""):
        q = """
            select 
            rj.issn_l, 
            listagg(rj.issn, ',') as issns,
            listagg(title, ',') as title, 
            sum(total::int) as num_2018_downloads, 
            count(*) as num_journals_with_issn_l
            from jump_counter counter
            left outer join ricks_journal_flat rj on counter.issn_l = rj.issn
            where package_id='{package_id}' 
            {and_where}
            group by rj.issn_l
            order by num_2018_downloads desc
            """.format(package_id=self.package_id_for_db, and_where=and_where)
        rows = get_sql_dict_rows(q)
        return rows

    @cached_property
    def get_published_in_2019(self):
        rows = self.get_base(and_where=""" and rj.issn_l in
	            (select rj.issn_l from unpaywall u 
	            join ricks_journal_flat rj on u.journal_issn_l = rj.issn
	            where year=2019 
	            group by rj.issn_l) """)
        return self.filter_by_core_list(rows)

    @cached_property
    def get_published_toll_access_in_2019(self):
        rows = self.get_base(and_where=""" and rj.issn_l in
	            (select rj.issn_l from unpaywall u 
	            join ricks_journal_flat rj on u.journal_issn_l = rj.issn
	            where year=2019 and journal_is_oa='false' 
	            group by rj.issn_l) """)
        return self.filter_by_core_list(rows)

    @cached_property
    def publisher_where(self):
        if self.publisher == "Elsevier":
            return "(rj.publisher ilike '%elsevier%')"
        elif self.publisher == "Wiley":
            return "(rj.publisher ilike '%wiley%')"
        elif self.publisher == "SpringerNature":
            return "((rj.publisher ilike '%springer%') or (rj.publisher ilike '%nature%'))"
        elif self.publisher == "Sage":
            return "(rj.publisher ilike '%sage%')"
        elif self.publisher == "TaylorFrancis":
            return "(rj.publisher ilike '%informa uk%')"
        else:
            return "false"

    @property
    def publisher_name_snippets(self):
        if self.publisher == "Elsevier":
            return ["elsevier"]
        elif self.publisher == "Wiley":
            return ["wiley"]
        elif self.publisher == "SpringerNature":
            return ["springer", "nature"]
        elif self.publisher == "Sage":
            return ["sage"]
        elif self.publisher == "TaylorFrancis":
            return ["taylor", "informa"]
        else:
            return []

    @cached_property
    def get_published_toll_access_in_2019_with_publisher(self):
        rows = self.get_base(and_where=""" and rj.issn_l in
	            (select distinct rj.issn_l 
	            from unpaywall u 
	            join ricks_journal_flat rj on u.journal_issn_l=rj.issn
	            where year=2019 and journal_is_oa='false'
	            and {publisher_where}
	            ) """.format(publisher_where=self.publisher_where))
        return self.filter_by_core_list(rows)

    @cached_property
    def get_published_toll_access_in_2019_with_publisher_have_price(self):
        rows = self.get_base(and_where=""" and rj.issn_l in
	            (select distinct rj.issn_l 
	            from unpaywall u 
	            join ricks_journal_flat rj on u.journal_issn_l=rj.issn
	            where year=2019 and journal_is_oa='false'
	            and {publisher_where}
	            )
	            and rj.issn_l in 
                (select distinct issn_l from jump_journal_prices 
                    where price > 0 and package_id in('658349d9', '{package_id}') 
                ) """.format(package_id=self.package_id, publisher_where=self.publisher_where))
        return self.filter_by_core_list(rows)

    @cached_property
    def get_in_scenario(self):
        my_saved_scenario = None
        if self.unique_saved_scenarios:
            first_scenario = self.unique_saved_scenarios[0]
            my_saved_scenario = SavedScenario.query.get(first_scenario.scenario_id)
        if not my_saved_scenario:
            my_saved_scenario = SavedScenario.query.get("demo")
        my_saved_scenario.set_live_scenario(None)
        response = my_saved_scenario.to_dict_journals()
        rows = response["journals"]
        return self.filter_by_core_list(rows)

    @cached_property
    def get_counter_unique_rows(self):
        rows = self.get_base()
        return self.filter_by_core_list(rows)

    @cached_property
    def get_diff_not_in_counter(self):
        if not self.has_core_journal_list:
            return []
        response_dict = {}
        remove = [row["issn_l"] for row in self.get_counter_rows]
        for row in self.get_core_journal_rows:
            if row["issn_l"] not in remove:
                response_dict[row["issn_l"]] = row
        response = sorted(response_dict.values(), key=lambda x: x["issn_l"], reverse=True)
        return response

    @cached_property
    def get_diff_non_unique(self):
        response = []
        for row in self.get_counter_unique_rows:
            if not row["issn_l"]:
                response += [row]
            if row["num_journals_with_issn_l"] > 1:
                response += [row]
        response = sorted(response, key=lambda x: x["num_2018_downloads"], reverse=True)
        return response

    @cached_property
    def get_diff_not_published_in_2019(self):
        response_dict = {}
        remove = [row["issn_l"] for row in self.get_published_in_2019]
        for row in self.get_counter_unique_rows:
            if row["issn_l"] not in remove:
                response_dict[row["issn_l"]] = row
        response = sorted(response_dict.values(), key=lambda x: x["num_2018_downloads"], reverse=True)
        return response

    @cached_property
    def get_diff_open_access_journals(self):
        response_dict = {}
        remove = [row["issn_l"] for row in self.get_published_toll_access_in_2019]
        for row in self.get_published_in_2019:
            if row["issn_l"] not in remove:
                response_dict[row["issn_l"]] = row
        response = sorted(response_dict.values(), key=lambda x: x["num_2018_downloads"], reverse=True)
        return response

    @cached_property
    def get_diff_changed_publisher(self):
        response_dict = {}
        remove = [row["issn_l"] for row in self.get_published_toll_access_in_2019_with_publisher]
        for row in self.get_published_toll_access_in_2019:
            if row["issn_l"] not in remove:
                response_dict[row["issn_l"]] = row
        response = sorted(response_dict.values(), key=lambda x: x["num_2018_downloads"], reverse=True)
        return response

    @cached_property
    def get_diff_no_price(self):
        response_dict = {}
        remove = [row["issn_l"] for row in self.get_published_toll_access_in_2019_with_publisher_have_price]
        for row in self.get_published_toll_access_in_2019_with_publisher:
            if row["issn_l"] not in remove:
                response_dict[row["issn_l"]] = row
        response = sorted(response_dict.values(), key=lambda x: x["num_2018_downloads"], reverse=True)
        return response

    @cached_property
    def get_diff_missing_from_scenario(self):
        response_dict = {}
        remove = [row["issn_l"] for row in self.get_in_scenario]
        for row in self.get_published_toll_access_in_2019_with_publisher_have_price:
            if row["issn_l"] not in remove:
                response_dict[row["issn_l"]] = row
        response = sorted(response_dict.values(), key=lambda x: x["num_2018_downloads"], reverse=True)
        return response

    @cached_property
    def get_diff_extra_in_scenario(self):
        response_dict = {}
        remove = [row["issn_l"] for row in self.get_published_toll_access_in_2019_with_publisher_have_price]
        for row in self.get_in_scenario:
            if row["issn_l"] not in remove:
                response_dict[row["issn_l"]] = row
        response = response_dict.values()
        # response = sorted(response_dict.values(), key=lambda x: x["num_2018_downloads"], reverse=True)
        return response

    @cached_property
    def package_id_for_db(self):
        package_id = self.package_id
        if not package_id or package_id.startswith("demo") or package_id==DEMO_PACKAGE_ID:
            package_id = DEMO_PACKAGE_ID
        return package_id

    @cached_property
    def is_owned_by_consortium(self):
        if self.consortia_scenario_ids_who_own_this_package:
            return True
        return False

    @cached_property
    def consortia_scenario_ids_who_own_this_package(self):
        q = u"""
        select consortium_package_id, scenario_id as consortium_scenario_id
            from jump_consortium_members cm
            join jump_package_scenario ps on cm.consortium_package_id=ps.package_id
            where member_package_id='{}'
        """.format(self.package_id)
        with get_db_cursor() as cursor:
            cursor.execute(q)
            rows = cursor.fetchall()
        return [row["consortium_scenario_id"] for row in rows]

    def get_journal_attributes(self):
        counter_rows = dict((x["issn_l"], x) for x in self.get_unfiltered_counter_rows)
        counter_defaults = defaultdict(lambda: defaultdict(lambda: None), counter_rows)

        # pa_rows = get_perpetual_access_from_cache([self.package_id])
        pa_rows = get_perpetual_access_from_cache(self.package_id)
        pa_defaults = defaultdict(lambda: defaultdict(lambda: None), pa_rows)

        price_packages = [self.package_id]
        if self.currency == "USD":
            price_packages += [DEMO_PACKAGE_ID]
        all_prices = get_prices_from_cache(price_packages, self.publisher)
        package_prices = all_prices[self.package_id]
        public_prices = all_prices[DEMO_PACKAGE_ID]
        package_price_defaults = defaultdict(lambda: None, package_prices)
        public_price_defaults = defaultdict(lambda: None, public_prices)

        open_access = set([x["issn_l"] for x in self.get_diff_open_access_journals])
        not_published_2019 = set([x["issn_l"] for x in self.get_diff_not_published_in_2019])
        changed_publisher = set([x["issn_l"] for x in self.get_diff_changed_publisher])

        distinct_issnls = set([x for x in
                               counter_rows.keys() +
                               pa_rows.keys() +
                               package_prices.keys() +
                               list(open_access) +
                               list(not_published_2019) +
                               list(changed_publisher)
                               if x
                               ])

        journal_rows = get_ricks_journal()

        for issn_l, journal in journal_rows.items():
            try:
                journal["issns"] = json.loads(journal["issns"])
            except (TypeError, ValueError):
                journal["issns"] = None

        return [{
            "issn_l": issn_l,
            "name": journal_rows.get(issn_l, {}).get("title", None),
            "upload_data": {
                "counter_downloads": counter_defaults[issn_l]["num_2018_downloads"],
                "perpetual_access_dates": [pa_defaults[issn_l]["start_date"], pa_defaults[issn_l]["end_date"]],
                "price": package_price_defaults[issn_l],
            },
            "has_upload_data": {
                "counter": issn_l in counter_rows,
                "perpetual_access": issn_l in pa_rows,
                "price": issn_l in package_prices,
            },
            "attributes": {
                "is_oa": issn_l in open_access,
                "not_published_2019": issn_l in not_published_2019,
                "changed_publisher": issn_l in changed_publisher,
                "is_hybrid_2019": issn_l in get_hybrid_2019(),
                "has_public_price": issn_l in public_prices,
                "public_price": public_price_defaults[issn_l],
            },
            "data_sources": [
                {
                    "id": "counter",
                    "source": "custom" if issn_l in counter_rows else None,
                    "value": counter_defaults[issn_l]["num_2018_downloads"],
                },
                {
                    "id": "perpetual_access",
                    "source": (
                        "custom" if issn_l in pa_rows
                        else None if pa_rows
                        else "default"
                    ),
                    "value": (
                        [pa_rows[issn_l]["start_date"], pa_rows[issn_l]["end_date"]] if issn_l in pa_rows
                        else [None, None] if pa_rows
                        else [datetime.datetime(2010, 1, 1), None]
                    ),
                },
                {
                    "id": "price",
                    "source": (
                        "custom" if package_prices.get(issn_l, None) is not None
                        else "default" if public_prices.get(issn_l, None) is not None
                        else None
                    ),
                    "value": package_price_defaults[issn_l] or public_price_defaults[issn_l],
                },
            ],
            "issns": journal_rows.get(issn_l, {}).get("issns", [])
        } for issn_l in distinct_issnls]

    @cached_property
    def journals_missing_prices(self):
        counter_rows = dict((x["issn_l"], x) for x in self.get_unfiltered_counter_rows)

        price_packages = [self.package_id]
        if self.currency == "USD":
            price_packages += [DEMO_PACKAGE_ID]
        has_a_prices = get_prices_from_cache(price_packages, self.publisher)

        open_access = [x["issn_l"] for x in self.get_diff_open_access_journals]
        not_published_2019 = [x["issn_l"] for x in self.get_diff_not_published_in_2019]
        changed_publisher = [x["issn_l"] for x in self.get_diff_changed_publisher]

        journals_missing_prices = []
        for issn_l in counter_rows:
            if issn_l in has_a_prices:
                continue
            if issn_l in open_access:
                continue
            if issn_l in not_published_2019:
                continue
            if issn_l in changed_publisher:
                continue
            if issn_l in changed_publisher:
                continue
            my_dict = {
                "issn_l": issn_l,
                "name": get_ricks_journal()[issn_l]["title"],
                "issns": get_ricks_journal()[issn_l]["issns"],
                # "counter_sum": counter_rows[issn_l]
                "counter_total": 42
            }
            journals_missing_prices.append(my_dict)

        return journals_missing_prices


    @cached_property
    def warnings(self):
        from scenario import get_package_specific_scenario_data_from_db

        response = []
        if not self.has_custom_perpetual_access:
            response += [{
                "id": "missing_perpetual_access",
                "is_dismissed": (True == self.is_dismissed_warning_missing_perpetual_access),
                "journals": None
            }]

        if self.journals_missing_prices:
            response += [{
                "id": "missing_prices",
                "is_dismissed": (True == self.is_dismissed_warning_missing_prices),
                "journals": self.journals_missing_prices
            }]

        return response

    def to_dict_summary(self):

        return {
                "id": self.package_id,
                "name": self.package_name,
                "currency": self.currency,
                "hasCounterData": self.has_counter_data,
                "hasCustomPrices": self.has_custom_prices,
                "hasCoreJournalList": self.has_core_journal_list,
                "hasCustomPerpetualAccess": self.has_custom_perpetual_access,
        }

    def to_package_dict(self):
        # journal_detail = dict(self.get_package_counter_breakdown())  # not used anymore
        # journal_detail["publisher_id"] = journal_detail.pop("package_id") # not used anymore

        # counter stats

        counter_errors = CounterInput().load_errors(self.package_id)

        if counter_errors:
            num_counter_error_rows = len(counter_errors["rows"])
        else:
            num_counter_error_rows = 0

        num_counter_rows = CounterInput.query.filter(CounterInput.package_id == self.package_id,
                                                    CounterInput.report_version != "5").count()
        num_counter_rows += num_counter_error_rows

        # perpetual access stats

        pa_errors = PerpetualAccessInput().load_errors(self.package_id)

        if pa_errors:
            num_pa_error_rows = len(pa_errors["rows"])
        else:
            num_pa_error_rows = 0

        num_pa_rows = PerpetualAccessInput.query.filter(PerpetualAccessInput.package_id == self.package_id).count()
        num_pa_rows += num_pa_error_rows

        # price stats

        price_errors = JournalPriceInput().load_errors(self.package_id)

        if price_errors:
            num_price_error_rows = len(price_errors["rows"])
        else:
            num_price_error_rows = 0

        num_price_rows = JournalPriceInput.query.filter(JournalPriceInput.package_id == self.package_id).count()
        num_price_rows += num_price_error_rows

        with get_db_cursor() as cursor:
            num_core_rows = cursor.execute(
                "select count(*) from jump_core_journals_input where package_id = '{}'".format(self.package_id)
            ).scalar()

            if self.institution.is_consortium:
                counter_uploaded = True
            else:
                counter_uploaded = num_counter_rows > 0


            data_files_list = [
                    {
                        "name": "counter",
                        "uploaded": counter_uploaded,
                        "rows_count": num_counter_rows,
                        "created_date": None,
                        "error_rows": counter_errors,
                    },
                    {
                        "name": "perpetual-access",
                        "uploaded": False if self.is_demo else num_pa_rows > 0,
                        "rows_count": num_pa_rows,
                        "created_date": None,
                        "error_rows": pa_errors,
                    },
                    {
                        "name": "price",
                        "uploaded": False if self.is_demo else num_price_rows > 0,
                        "rows_count": num_price_rows,
                        "created_date": None,
                        "error_rows": price_errors,
                    },
                    {
                        "name": "core-journals",
                        "uploaded": False if self.is_demo else num_core_rows > 0,
                        "rows_count": num_core_rows,
                        "created_date": None,
                        "error_rows": None,
                    }]
            for filename in ["counter-trj2", "counter-trj3", "counter-trj4"]:
                data_files_list += [{
                    "name": filename,
                    "uploaded": False,
                    "created_date": None,
                    "rows_count": None,
                    "error_rows": None}]

            command = u"""select * from jump_raw_file_upload_object where package_id = '{}'""".format(self.package_id)
            with get_db_cursor() as cursor:
                cursor.execute(command)
                raw_file_upload_rows = cursor.fetchall()

            for data_file in data_files_list:
                data_file["error"] = None
                data_file["error_details"] = None
                data_file["is_uploaded"] = False
                data_file["is_parsed"] = False
                data_file["is_live"] = False

            for raw_file_upload_row in raw_file_upload_rows:
                for my_dict in data_files_list:
                    if (my_dict["name"] == raw_file_upload_row["file"]):
                        my_dict["is_uploaded"] = True
                        my_dict["is_parsed"] = True
                        my_dict["created_date"] = raw_file_upload_row["created"]
                        if raw_file_upload_row["num_rows"]:
                            my_dict["rows_count"] = raw_file_upload_row["num_rows"]
                        if raw_file_upload_row["error"]:
                            my_dict["is_live"] = False
                            my_dict["error"] = raw_file_upload_row["error"]
                            error_details_dict = {
                                "no_useable_rows": "No usable rows found.",
                                "error_reading_file": "Error reading this file. Try opening this file, save in .xlsx format, and upload that.",
                                "runtime_error": "Error processing file. Please email this file to team@ourresearch.org so the Unsub team can look into the problem."
                            }
                            my_dict["error_details"] = error_details_dict.get(my_dict["error"], "There was an error")
                        else:
                            my_dict["is_live"] = True


            preprocess_file_list = s3_client.list_objects(Bucket="unsub-file-uploads-preprocess")
            for preprocess_file in preprocess_file_list.get("Contents", []):
                filename = preprocess_file["Key"]
                filename_base = filename.split(".")[0]
                try:
                    package_id, filetype = filename_base.split("_")
                except ValueError:
                    # not a valid file, skip it
                    continue
                size = preprocess_file["Size"]
                age_seconds = (datetime.datetime.utcnow() - preprocess_file["LastModified"].replace(tzinfo=None)).total_seconds()
                for my_dict in data_files_list:
                    if my_dict["name"] == filetype:
                        my_dict["is_uploaded"] = True
                        my_dict["is_parsed"] = False
                        my_dict["is_live"] = False

            response = {
                "id": self.package_id,
                "name": self.package_name,
                "currency": self.currency,
                "publisher": self.publisher,
                "is_demo": self.is_demo,
                "journal_detail": None,  #not used anymore
                "scenarios": [s.to_dict_minimal() for s in self.saved_scenarios],
                "data_files": data_files_list,
                "journals": self.get_journal_attributes(),
                "is_owned_by_consortium": self.is_owned_by_consortium,
                "is_deleted": self.is_deleted is not None and self.is_deleted,
                "warnings": self.warnings
            }
            return response

    def to_dict_minimal(self):
        response = {
            "id": self.package_id,
            "name": self.package_name,
            "currency": self.currency,
            "publisher": self.publisher,
            "is_owned_by_consortium": self.is_owned_by_consortium,
            "is_deleted": self.is_deleted is not None and self.is_deleted,
        }
        return response

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.package_id, self.package_name)


def clone_demo_package(institution):
    demo_package = Package.query.filter(Package.package_id == DEMO_PACKAGE_ID).first()
    now = datetime.datetime.utcnow().isoformat(),

    # jump_account_package
    new_package = Package(
        package_id="package-{}".format(shortuuid.uuid()[0:12]),
        publisher=demo_package.publisher,
        package_name=demo_package.publisher,
        created=now,
        institution_id=institution.id,
        is_demo=True
    )

    db.session.add(new_package)

    # jump_package_scenario
    demo_scenarios = SavedScenario.query.filter(SavedScenario.package_id == DEMO_PACKAGE_ID).all()
    for scenario in demo_scenarios:
        new_scenario = SavedScenario(False, "scenario-{}".format(shortuuid.uuid()[0:12]), None)
        new_scenario.package_id = new_package.package_id
        new_scenario.scenario_name = scenario.scenario_name
        new_scenario.created = now
        new_scenario.is_base_scenario = scenario.is_base_scenario

        db.session.add(new_scenario)
    safe_commit(db)

    with get_db_cursor() as cursor:

        # jump_counter
        cursor.execute(
            """
                insert into jump_counter (issn_l, package_id, journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type) (
                    select issn_l, '{}', journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type 
                    from jump_counter
                    where package_id = '{}'
                )
            """.format(new_package.package_id, DEMO_PACKAGE_ID)
        )

        # 'jump_counter_input',
        cursor.execute(
            """
                insert into jump_counter_input (issn, journal_name, total, package_id, report_year, report_name, report_version, metric_type, yop, access_type) (
                    select issn, journal_name, total, '{}', report_year, report_name, report_version, metric_type, yop, access_type
                    from jump_counter_input
                    where package_id = '{}'
                )
            """.format(new_package.package_id, DEMO_PACKAGE_ID)
        )

        # jump_core_journals
        cursor.execute(
            """
                insert into jump_core_journals (package_id, issn_l, baseline_access) (
                    select '{}', issn_l, baseline_access from jump_core_journals where package_id = '{}'
                )
            """.format(new_package.package_id, DEMO_PACKAGE_ID)
        )

        # 'jump_perpetual_access'
        cursor.execute(
            """
                insert into jump_perpetual_access (package_id, issn_l, start_date, end_date) (
                    select '{}', issn_l, start_date, end_date 
                    from jump_perpetual_access
                    where package_id = '{}'
                )
            """.format(new_package.package_id, DEMO_PACKAGE_ID)
        )

        # 'jump_perpetual_access_input'
        cursor.execute(
            """
                insert into jump_perpetual_access_input (package_id, issn, start_date, end_date) (
                    select '{}', issn, start_date, end_date 
                    from jump_perpetual_access_input
                    where package_id = '{}'
                )
            """.format(new_package.package_id, DEMO_PACKAGE_ID)
        )

        # 'jump_apc_authorships'
        cursor.execute(
            """
                insert into jump_apc_authorships (
                    package_id, doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc
                ) (
                    select '{}', doi, publisher, num_authors_total, num_authors_from_uni, journal_name, issn_l, year, oa_status, apc 
                    from jump_apc_authorships
                    where package_id = '{}'
                )
            """.format(new_package.package_id, DEMO_PACKAGE_ID)
        )

    return new_package


