# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import OrderedDict
import datetime
import shortuuid
from time import time
import os
import numpy as np
import pandas as pd

from app import db
from app import get_db_cursor
from app import DEMO_PACKAGE_ID
from app import s3_client

# from app import my_memcached # disable memcached
from apc_journal import ApcJournal
from saved_scenario import SavedScenario # used in relationship
from institution import Institution  # used in relationship
from scenario import get_core_list_from_db, get_apc_data_from_db
from util import get_sql_dict_rows
from util import safe_commit
from util import for_sorting
from util import elapsed
from openalex import JournalMetadata, MissingJournalMetadata, all_journal_metadata_flat


class Package(db.Model):
    __tablename__ = "jump_account_package"
    institution_id = db.Column(db.Text, db.ForeignKey("jump_institution.id"))
    package_id = db.Column(db.Text, primary_key=True)
    publisher = db.Column(db.Text)
    package_name = db.Column(db.Text)
    package_description = db.Column(db.Text)
    consortium_package_id = db.Column(db.Text)
    created = db.Column(db.DateTime)
    is_demo = db.Column(db.Boolean)
    big_deal_cost = db.Column(db.Numeric(asdecimal=False))
    big_deal_cost_increase = db.Column(db.Float)
    is_deleted = db.Column(db.Boolean)
    currency = db.Column(db.Text)

    saved_scenarios = db.relationship("SavedScenario", lazy="subquery", backref=db.backref("package", lazy="subquery"))
    institution = db.relationship("Institution", lazy="subquery", uselist=False, backref=db.backref("packages", lazy="subquery"))

    def __init__(self, **kwargs):
        self.created = datetime.datetime.utcnow().isoformat()
        self.is_deleted = False
        super(Package, self).__init__(**kwargs)

    @cached_property
    def is_consortial_package(self):
        is_cons_pkg = False
        if self.consortial_package_ids:
            is_cons_pkg = True
        return is_cons_pkg

    @cached_property
    def consortial_package_ids(self):
        with get_db_cursor() as cursor:
            qry = "select member_package_id from jump_consortium_members where consortium_package_id = %s"
            cursor.execute(qry, (self.package_id,))
            rows = cursor.fetchall()
        return [w[0] for w in rows]

    @cached_property
    def unique_issns(self):
        if self.is_consortial_package:
            package_ids = tuple(self.consortial_package_ids)
            with get_db_cursor() as cursor:
                qry = "select distinct(issn_l) from jump_counter where package_id in %s"
                cursor.execute(qry, (package_ids,))
                rows = cursor.fetchall()
        else:
            with get_db_cursor() as cursor:
                qry = "select distinct(issn_l) from jump_counter where package_id = %s"
                cursor.execute(qry, (self.package_id,))
                rows = cursor.fetchall()
        return [w[0] for w in rows]

    @cached_property
    def journal_metadata(self):
        meta_list = JournalMetadata.query.filter(
            JournalMetadata.issn_l.in_(self.unique_issns),
            JournalMetadata.is_current_subscription_journal).all()
        [db.session.expunge(my_meta) for my_meta in meta_list]
        return dict(list(zip([j.issn_l for j in meta_list], meta_list)))

    @cached_property
    def journal_metadata_flat(self):
        jmf = {}
        for issn_l, x in self.journal_metadata.items():
            for issn in x.issns:
                jmf[issn] = x
        return jmf

    def get_journal_metadata(self, issn):
        journal_meta = self.journal_metadata_flat.get(issn, None)
        if not journal_meta:
            journal_meta = MissingJournalMetadata(issn_l=issn)
        return journal_meta

    @property
    def unique_saved_scenarios(self):
        return self.saved_scenarios

    @property
    def scenario_ids(self):
        return [s.scenario_id for s in self.saved_scenarios]

    @property
    def is_demo_account(self):
        return self.package_id.startswith("demo")

    @cached_property
    def has_complete_counter_data(self):
        if self.institution.is_consortium:
            return True

        if self.data_files_dict["counter"]["is_live"]:
            return True

        if self.data_files_dict["counter-trj2"]["is_live"] and \
                self.data_files_dict["counter-trj3"]["is_live"] and \
                self.data_files_dict["counter-trj4"]["is_live"]:
            return True

        return False

    @cached_property
    def filter_data_set(self):
        if self.data_files_dict["filter"]["is_live"]:
            return True

        return False

    @property
    def has_custom_perpetual_access(self):
        my_data_file_dict = self.data_files_dict["perpetual-access"]
        if my_data_file_dict["is_live"]:
            return True
        return False

    @property
    def has_custom_prices(self):
        my_data_file_dict = self.data_files_dict["price"]
        if my_data_file_dict["is_live"]:
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
        core_issnls = list(core_rows.keys())
        return [row for row in my_list if row["issn_l"] in core_issnls]

    @cached_property
    def get_core_journal_rows(self):
        q = """
            select 
            core.issn_l, 
            title as title
            from jump_core_journals core
            left outer join openalex_computed on core.issn_l = openalex_computed.issn_l
            where package_id=%(package_id_for_db)s
            order by title desc
            """
        rows = get_sql_dict_rows(q, {'package_id_for_db': self.package_id_for_db})
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
           left outer join openalex_computed_flat rj on counter.issn_l = rj.issn
           where package_id=%(package_id_for_db)s
           group by rj.issn_l           
           order by num_2018_downloads desc
           """
        return get_sql_dict_rows(q, {'package_id_for_db': self.package_id_for_db})

    def get_base(self, and_where="", extrakv={}):
        q = """
            select 
            rj.issn_l, 
            listagg(rj.issn, ',') as issns,
            listagg(title, ',') as title, 
            sum(total::int) as num_2018_downloads, 
            count(*) as num_journals_with_issn_l
            from jump_counter counter
            left outer join openalex_computed_flat rj on counter.issn_l = rj.issn
            where package_id=%(package_id_for_db)s 
            {}
            group by rj.issn_l
            order by num_2018_downloads desc
            """.format(and_where)
        rows = get_sql_dict_rows(q, {'package_id_for_db': self.package_id_for_db} | extrakv)
        return rows

    @cached_property
    def get_published_in_2019(self):
        rows = self.get_base(and_where=""" and rj.issn_l in
	            (select rj.issn_l from unpaywall u 
	            join openalex_computed_flat rj on u.journal_issn_l = rj.issn
	            where year=2019 
	            group by rj.issn_l) """)
        return self.filter_by_core_list(rows)

    @cached_property
    def get_published_toll_access_in_2019(self):
        rows = self.get_base(and_where=""" and rj.issn_l in
	            (select rj.issn_l from unpaywall u 
	            join openalex_computed_flat rj on u.journal_issn_l = rj.issn
	            where year=2019 and journal_is_oa='false' 
	            group by rj.issn_l) """)
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
        response = sorted(list(response_dict.values()), key=lambda x: x["issn_l"], reverse=True)
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
        response = sorted(list(response_dict.values()), key=lambda x: x["num_2018_downloads"], reverse=True)
        return response

    @cached_property
    def get_diff_open_access_journals(self):
        response_dict = {}
        remove = [row["issn_l"] for row in self.get_published_toll_access_in_2019]
        for row in self.get_published_in_2019:
            if row["issn_l"] not in remove:
                response_dict[row["issn_l"]] = row
        response = sorted(list(response_dict.values()), key=lambda x: x["num_2018_downloads"], reverse=True)
        return response

    @cached_property
    def package_id_for_db(self):
        package_id = self.package_id
        if not package_id or package_id.startswith("demo") or package_id==DEMO_PACKAGE_ID:
            package_id = DEMO_PACKAGE_ID
        return package_id

    @property
    def feedback_scenario_dicts(self):
        feedback_scenarios = [s for s in self.saved_scenarios if s.is_feedback_scenario]
        feedback_scenario_dicts = [s.to_dict_minimal() for s in feedback_scenarios]
        return feedback_scenario_dicts

    @cached_property
    def feedback_set_id(self):
        return self.package_id.replace("package-", "feedback-")

    @cached_property
    def feedback_set_name(self):
        return "Feedback on {} scenarios".format(self.publisher)

    @cached_property
    def feedback_rows(self):
        if not self.is_feeder_package:
            return []
        command = 'select * from jump_consortium_feedback_requests where member_package_id=%s'
        with get_db_cursor() as cursor:
            cursor.execute(command, (self.package_id,))
            rows_for_feedback = cursor.fetchall()
        return rows_for_feedback

    @cached_property
    def is_feedback_package(self):
        if not self.is_feeder_package:
            return False
        return (len(self.feedback_rows) > 0)

    @cached_property
    def is_feeder_package(self):
        return self.is_owned_by_consortium

    @cached_property
    def is_owned_by_consortium(self):
        if self.consortia_scenario_ids_who_own_this_package:
            return True
        return False

    @cached_property
    def consortia_scenario_ids_who_own_this_package(self):
        q = """
        select consortium_package_id, scenario_id as consortium_scenario_id
            from jump_consortium_members cm
            join jump_package_scenario ps on cm.consortium_package_id=ps.package_id
            where member_package_id=%s
        """
        with get_db_cursor() as cursor:
            cursor.execute(q, (self.package_id,))
            rows = cursor.fetchall()
        return [row["consortium_scenario_id"] for row in rows]



    @cached_property
    def counter_totals_from_db(self):
        from scenario import get_counter_totals_from_db
        return get_counter_totals_from_db(self.package_id)

    @cached_property
    def counter_journals_by_report_name(self):
        from scenario import get_counter_journals_by_report_name_from_db
        return get_counter_journals_by_report_name_from_db(self.package_id)

    @cached_property
    def journals_missing_prices(self):
        counter_rows = self.counter_totals_from_db
        prices_uploaded_raw = get_custom_prices(self.package_id)
        journals_missing_prices = []

        for my_journal_metadata in list(self.journal_metadata.values()):
            if my_journal_metadata.is_current_subscription_journal:
                issn_l = my_journal_metadata.issn_l
                if not issn_l in counter_rows:
                    pass
                elif counter_rows[issn_l] == 0:
                    pass
                elif prices_uploaded_raw.get(issn_l, None) != None:
                    pass
                else:
                    my_dict = OrderedDict([
                        ("issn_l_prefixed", my_journal_metadata.display_issn_l),
                        ("issn_l", my_journal_metadata.issn_l),
                        ("name", my_journal_metadata.title),
                        ("issns", my_journal_metadata.display_issns),
                        ("publisher", my_journal_metadata.publisher),
                        ("currency", self.currency),
                        ("counter_total", counter_rows[issn_l]),
                    ])
                    journals_missing_prices.append(my_dict)

        journals_missing_prices = sorted(journals_missing_prices, key=lambda x: 0 if x["counter_total"]==None else x["counter_total"], reverse=True)

        return journals_missing_prices

    @cached_property
    def journals_filtering(self):
        filtering = []
        with get_db_cursor() as cursor:
            qry = "select distinct(issn_l) from jump_journal_filter where package_id = %s"
            cursor.execute(qry, (self.package_id,))
            rows = cursor.fetchall()
        if rows:
            filtering = [w[0] for w in rows]
        return filtering

    @cached_property
    def returned_big_deal_cost(self):
        if self.institution.is_consortium:
            return 42
        return self.big_deal_cost

    @cached_property
    def returned_big_deal_cost_increase(self):
        if self.institution.is_consortium:
            return 42
        return self.big_deal_cost_increase

    @cached_property
    def warnings(self):
        from scenario import get_package_specific_scenario_data_from_db

        if self.institution.is_consortium:
            return []

        if any([w in self.package_id for w in ['jiscels', 'jiscsage', 'jiscwiley', 'jiscspringer', 'jisctf']]):
            # don't show warnings for those packages
            # maybe best thing is don't show warnings for any feedback packages?
            # Update on 2022-03-07: Scott added ignore warnings for sage, wiley, springer, and tf pkgs
            #   This removes warnings for not just feeder pkgs but standalone packages for each institution
            return []

        response = []

        if not self.has_custom_perpetual_access:
            response += [OrderedDict([
                ("id", "missing_perpetual_access"),
                ("journals", None)
            ])]

        if (not self.has_complete_counter_data) or (len(self.journals_missing_prices) > 0):
            response += [OrderedDict([
                ("id", "missing_prices"),
                ("journals", self.journals_missing_prices)
            ])]

        if self.filter_data_set:
            response += [OrderedDict([
                ("id", "filtering_titles"),
                ("journals", self.journals_filtering)
            ])]

        return response

    def get_fresh_apc_journal_list(self, issn_ls, apc_df_dict):
        apc_journals = []
        if not hasattr(self, "apc_data"):
            self.apc_data = get_apc_data_from_db(self.package_id)

        for issn_l in issn_ls:
            meta = all_journal_metadata_flat.get(issn_l, None)
            if meta:
                if meta.get_apc_price(self.currency):
                    apc_journal = ApcJournal(issn_l, self.apc_data, apc_df_dict, self.currency, self)
                    apc_journals.append(apc_journal)
        return apc_journals

    @cached_property
    def apc_journals(self):
        if not hasattr(self, "apc_data"):
            self.apc_data = get_apc_data_from_db(self.package_id)

        if not self.apc_data:
            return []

        df = pd.DataFrame(self.apc_data)
        df["year"] = df["year"].astype(int)
        df["authorship_fraction"] = df.num_authors_from_uni/df.num_authors_total
        df_by_issn_l_and_year = df.groupby(["issn_l", "year"]).authorship_fraction.agg([np.size, np.sum]).reset_index().rename(columns={'size': 'num_papers', "sum": "authorship_fraction"})
        my_dict = {"df": df, "df_by_issn_l_and_year": df_by_issn_l_and_year}
        return self.get_fresh_apc_journal_list(my_dict["df"].issn_l.unique(), my_dict)

    @cached_property
    def apc_journals_sorted_spend(self):
        self.apc_journals.sort(key=lambda k: for_sorting(k.cost_apc_historical), reverse=True)
        return self.apc_journals

    @cached_property
    def num_apc_papers_historical(self):
        return round(np.sum([j.num_apc_papers_historical for j in self.apc_journals]))

    @cached_property
    def years(self):
        return list(range(0, 5))

    @cached_property
    def cost_apc_historical_by_year(self):
        return [round(np.sum([j.cost_apc_historical_by_year[year] for j in self.apc_journals])) for year in self.years]

    @cached_property
    def cost_apc_historical(self):
        return round(np.mean(self.cost_apc_historical_by_year))

    @cached_property
    def cost_apc_historical_hybrid_by_year(self):
        return [round(np.sum([j.cost_apc_historical_by_year[year] for j in self.apc_journals if j.oa_status=="hybrid"]), 4) for year in self.years]

    @cached_property
    def cost_apc_historical_hybrid(self):
        return round(np.mean(self.cost_apc_historical_hybrid_by_year))

    @cached_property
    def cost_apc_historical_gold_by_year(self):
        return [round(np.sum([j.cost_apc_historical_by_year[year] for j in self.apc_journals if j.oa_status=="gold"]), 4) for year in self.years]

    @cached_property
    def cost_apc_historical_gold(self):
        return round(np.mean(self.cost_apc_historical_gold_by_year))

    @cached_property
    def fractional_authorships_total_by_year(self):
        return [round(np.sum([j.fractional_authorships_total_by_year[year] for j in self.apc_journals]), 4) for year in self.years]

    @cached_property
    def fractional_authorships_total(self):
        return round(np.mean(self.fractional_authorships_total_by_year), 2)

    @cached_property
    def apc_journals_sorted_fractional_authorship(self):
        self.apc_journals.sort(key=lambda k: for_sorting(k.fractional_authorships_total), reverse=True)
        return self.apc_journals

    @cached_property
    def apc_price(self):
        if self.apc_journals:
            return np.max([j.apc_price for j in self.apc_journals])
        else:
            return 0

    def update_apc_authorships(self):
        delete_q = 'delete from jump_apc_authorships where package_id = %s'
        insert_q = """
                insert into jump_apc_authorships (
                    select * from jump_apc_authorships_view
                    where package_id = %s and issn_l in 
                    (select issn_l from openalex_computed))
            """
        with get_db_cursor() as cursor:
            cursor.execute(delete_q, (self.package_id,))
            cursor.execute(insert_q, (self.package_id,))

    def to_dict_apc(self):
        response = {
            "headers": [
                    {"text": "OA type", "value": "oa_status", "percent": None, "raw": None, "display": "text"},
                    {"text": "APC price", "value": "apc_price", "percent": None, "raw": self.apc_price, "display": "currency_int"},
                    {"text": "Number APC papers", "value": "num_apc_papers", "percent": None, "raw": self.num_apc_papers_historical, "display": "float1"},
                    {"text": "Total fractional authorship", "value": "fractional_authorship", "percent": None, "raw": self.fractional_authorships_total, "display": "float1"},
                    {"text": "APC Dollars Spent", "value": "cost_apc", "percent": None, "raw": self.cost_apc_historical, "display": "currency_int"},
            ]
        }
        response["journals"] = [j.to_dict() for j in self.apc_journals_sorted_spend]
        return response

    def to_dict_summary(self):

        return {
                "id": self.package_id,
                "name": self.package_name,
                "description": self.package_description,
                "currency": self.currency,
                "hasCounterData": self.has_complete_counter_data,
                "hasCustomPrices": self.has_custom_prices,
                "hasCoreJournalList": self.has_core_journal_list,
                "hasCustomPerpetualAccess": self.has_custom_perpetual_access,
        }


    @cached_property
    def data_files_dict(self):
        command = "select * from jump_raw_file_upload_object where package_id=%s"
        with get_db_cursor() as cursor:
            cursor.execute(command, (self.package_id,))
            raw_file_upload_rows = cursor.fetchall()

        data_files_dict = {}
        data_file_types = ["counter", "counter-trj2", "counter-trj3", "counter-trj4", "price", "perpetual-access", "filter"]
        for data_file_type in data_file_types:
            my_dict = OrderedDict()
            my_dict["name"] = data_file_type
            my_dict["is_live"] = False
            my_dict["is_parsed"] = False
            my_dict["is_uploaded"] = False
            my_dict["rows_count"] = None
            my_dict["error"] = None
            my_dict["error_details"] = None
            my_dict["created_date"] = None
            data_files_dict[data_file_type] = my_dict

        # data_files_dict["price-public"]["is_uploaded"] = False
        # data_files_dict["price-public"]["is_parsed"] = False
        # data_files_dict["price-public"]["is_live"] = False
        # data_files_dict["price-public"]["rows_count"] = len(self.public_price_rows())

        # go through all the upload rows
        for raw_file_upload_row in raw_file_upload_rows:
            my_dict = data_files_dict[raw_file_upload_row["file"]]
            if (my_dict["name"] == raw_file_upload_row["file"]):
                if raw_file_upload_row["to_delete_date"] != None:
                    # handle the ones that have been marked for delete but not deleted yet
                    my_dict["rows_count"] = 0
                else:
                    my_dict["is_uploaded"] = True
                    my_dict["is_parsed"] = True
                    my_dict["is_live"] = True
                    my_dict["created_date"] = raw_file_upload_row["created"]
                    if raw_file_upload_row["num_rows"]:
                        my_dict["rows_count"] = raw_file_upload_row["num_rows"]
                    if raw_file_upload_row["error"]:
                        my_dict["error"] = raw_file_upload_row["error"]
                        my_dict["error_details"] = raw_file_upload_row["error_details"]
                        my_dict["is_live"] = False

        # handle the ones that have been uploaded but not processed yet
        upload_preprocess_bucket = "unsub-file-uploads-preprocess-testing" if os.getenv("TESTING_DB") else "unsub-file-uploads-preprocess"
        preprocess_file_list = s3_client.list_objects(Bucket=upload_preprocess_bucket)
        for preprocess_file in preprocess_file_list.get("Contents", []):
            filename = preprocess_file["Key"]
            filename_base = filename.split(".")[0]
            try:
                preprocess_package_id, preprocess_filetype = filename_base.split("_")
            except ValueError:
                # not a valid file, skip it
                continue
            # size = preprocess_file["Size"]
            # age_seconds = (datetime.datetime.utcnow() - preprocess_file["LastModified"].replace(tzinfo=None)).total_seconds()
            if preprocess_package_id == self.package_id:
                my_dict = data_files_dict[preprocess_filetype]
                my_dict["is_uploaded"] = True
                my_dict["is_parsed"] = False
                my_dict["is_live"] = False

        return data_files_dict

    def to_package_dict(self):
        data_files_list = sorted(list(self.data_files_dict.values()), key=lambda x: 0 if x["rows_count"]==None else x["rows_count"], reverse=True)
        response = OrderedDict([
            ("id", self.package_id),
            ("created", self.created),
            ("name", self.package_name),
            ("publisher", self.publisher),
            ("description", self.package_description),
            ("currency", self.currency),
            ("cost_bigdeal", self.returned_big_deal_cost),
            ("cost_bigdeal_increase", self.returned_big_deal_cost_increase),
            ("is_consortium", self.institution.is_consortium),
            ("is_deleted", self.is_deleted is not None and self.is_deleted),
            ("is_demo", self.is_demo),
            ("has_complete_counter_data", self.has_complete_counter_data),
            ("filter_data_set", self.filter_data_set),
            ("has_custom_prices", self.has_custom_prices),
            ("data_files", data_files_list),
            # @todo for testing, show all scenarios even with owned by consortium
            # ("is_owned_by_consortium", self.is_owned_by_consortium),
            ("is_owned_by_consortium", False),
            ("is_consortial_proposal_set", self.is_feedback_package),
            # ("scenarios", [s.to_dict_minimal() for s in self.saved_scenarios if not s.is_feedback_scenario]),
            ("scenarios", [s.to_dict_minimal() for s in self.saved_scenarios]),
            ("warnings", self.warnings),
        ])
        return response

    def to_package_dict_feedback_set(self):
        response = self.to_package_dict()
        response["id"] = self.feedback_set_id
        response["name"] = self.feedback_set_name
        response["scenarios"] = self.feedback_scenario_dicts
        return response


    def to_dict_minimal(self):
        return self.to_dict_minimal_base()

    def to_dict_minimal_base(self):
        response = OrderedDict([
            ("id", self.package_id),
            ("name", self.package_name),
            ("currency", self.currency),
            ("publisher", self.publisher),
            ("description", self.package_description),
            ("is_deleted", self.is_deleted is not None and self.is_deleted),
            ("is_consortium", self.institution.is_consortium),
            ("is_owned_by_consortium", self.is_owned_by_consortium),
            ("is_feeder_package", self.is_feeder_package),
            ("is_consortial_proposal_set", False),
        ])
        return response

    def to_dict_minimal_feedback_set(self):
        response = self.to_dict_minimal_base()
        response["id"] = self.feedback_set_id
        response["name"] = self.feedback_set_name
        response["is_feeder_package"] = False
        response["is_owned_by_consortium"] = False
        response["is_consortial_proposal_set"] = True
        return response


    def __repr__(self):
        return "<{} ({}) {}>".format(self.__class__.__name__, self.package_id, self.package_name)


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
        new_scenario.created = now
        new_scenario.is_base_scenario = scenario.is_base_scenario

        db.session.add(new_scenario)
    safe_commit(db)

    with get_db_cursor() as cursor:

        # jump_counter
        cursor.execute(
            """
                insert into jump_counter (issn_l, package_id, journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type, created) (
                    select issn_l, '{}', journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type, created 
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
                insert into jump_perpetual_access (package_id, issn_l, start_date, end_date, created) (
                    select '{}', issn_l, start_date, end_date, created 
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



def check_if_to_delete(package_id, file):
    command = "select * from jump_raw_file_upload_object where package_id=%s and to_delete_date is not null"
    with get_db_cursor() as cursor:
        cursor.execute(command, (package_id,))
        rows_to_delete = cursor.fetchall()
    for row in rows_to_delete:
        if (row["package_id"] == package_id) and (row["file"] == file):
            return True
    return False


def get_custom_prices(package_id):
    package_dict = {}

    if check_if_to_delete(package_id, "price"):
        return package_dict

    command = "select issn_l, price from jump_journal_prices where (package_id=%s)"
    with get_db_cursor() as cursor:
        cursor.execute(command, (package_id,))
        rows = cursor.fetchall()

    for row in rows:
        package_dict[row["issn_l"]] = row["price"]

    return package_dict
