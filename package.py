# coding: utf-8

from cached_property import cached_property
import numpy as np
from collections import defaultdict
import weakref
from collections import OrderedDict
import datetime
import shortuuid

from app import db
from app import get_db_cursor
from app import DEMO_PACKAGE_ID
from saved_scenario import SavedScenario
from util import get_sql_answer
from util import get_sql_rows
from util import get_sql_dict_rows

def get_ids():
    rows = get_sql_dict_rows("""select * from jump_account_package_scenario_view order by username""")
    for row in rows:
        row["created"] = row["created"].isoformat()[0:10]
    return rows



class Package(db.Model):
    __tablename__ = 'jump_account_package'
    account_id = db.Column(db.Text, db.ForeignKey("jump_account.id"))
    package_id = db.Column(db.Text, primary_key=True)
    publisher = db.Column(db.Text)
    package_name = db.Column(db.Text)
    consortium_package_id = db.Column(db.Text)
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
        # TODO
        return True

    @property
    def num_journals(self):
        # TODO
        return 1850
        # return len(self.saved_scenarios[0].journals)

    @property
    def num_perpetual_access_journals(self):
        # self TODO
        return self.num_journals

    @cached_property
    def get_counter_rows(self):
        q = """
            select 
            counter.issn_l, 
            counter.issn as issns,
            title, 
            total::int as num_2018_downloads
            from jump_counter counter
            left outer join ricks_journal on counter.issn_l = ricks_journal.issn_l
            where package_id='{package_id}' 
            order by num_2018_downloads desc
            """.format(package_id=self.package_id)
        rows = get_sql_dict_rows(q)
        return rows

    def get_base(self, and_where=""):
        q = """
            select 
            counter.issn_l, 
            listagg(counter.issn, ',') as issns,
            listagg(title, ',') as title, 
            sum(total::int) as num_2018_downloads, 
            count(*) as num_journals_with_issn_l
            from jump_counter counter
            left outer join ricks_journal on counter.issn_l = ricks_journal.issn_l
            where package_id='{package_id}' 
            {and_where}
            group by counter.issn_l
            order by num_2018_downloads desc
            """.format(package_id=self.package_id, and_where=and_where)
        rows = get_sql_dict_rows(q)
        return rows


    @cached_property
    def get_published_in_2019(self):
        rows = self.get_base(and_where=""" and counter.issn_l in
	            (select journal_issn_l from unpaywall u where year=2019 group by journal_issn_l) """)
        return rows

    @cached_property
    def get_published_toll_access_in_2019(self):
        rows = self.get_base(and_where=""" and counter.issn_l in
	            (select journal_issn_l from unpaywall u where year=2019 and journal_is_oa='false' group by journal_issn_l) """)
        return rows

    @cached_property
    def get_published_toll_access_in_2019_with_elsevier(self):
        rows = self.get_base(and_where=""" and counter.issn_l in
	            (select journal_issn_l from unpaywall u where year=2019 and journal_is_oa='false' and publisher ilike '%elsevier%' group by journal_issn_l) """)
        return rows

    @cached_property
    def get_published_toll_access_in_2019_with_elsevier_have_price(self):
        rows = self.get_base(and_where=""" and counter.issn_l in
	            (select journal_issn_l from unpaywall u where year=2019 and journal_is_oa='false' and publisher ilike '%elsevier%' group by journal_issn_l) 
	            and counter.issn_l in 
            	(select issn_l from jump_journal_prices where usa_usd > 0 and package_id='93YfzkaA' group by issn_l) """)
        return rows


    @cached_property
    def get_counter_unique_rows(self):
        rows = self.get_base()
        return rows

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
        remove = [row["issn_l"] for row in self.get_published_toll_access_in_2019_with_elsevier]
        for row in self.get_published_toll_access_in_2019:
            if row["issn_l"] not in remove:
                response_dict[row["issn_l"]] = row
        response = sorted(response_dict.values(), key=lambda x: x["num_2018_downloads"], reverse=True)
        return response

    @cached_property
    def get_diff_no_price(self):
        response_dict = {}
        remove = [row["issn_l"] for row in self.get_published_toll_access_in_2019_with_elsevier_have_price]
        for row in self.get_published_toll_access_in_2019_with_elsevier:
            if row["issn_l"] not in remove:
                response_dict[row["issn_l"]] = row
        response = sorted(response_dict.values(), key=lambda x: x["num_2018_downloads"], reverse=True)
        return response

    @cached_property
    def package_id_for_db(self):
        package_id = self.package_id
        if not package_id or package_id.startswith("demo") or package_id==DEMO_PACKAGE_ID:
            package_id = DEMO_PACKAGE_ID
        return package_id

    def get_package_counter_breakdown(self):
        package_id = self.package_id_for_db

        response = OrderedDict()
        response["diff_counts"] = OrderedDict()
        response["papers"] = OrderedDict()
        response["package_id"] = package_id


        answer = get_sql_answer(db, "select display_name from jump_account_package_scenario_view where package_id='{}'".format(package_id))
        response["display_name"] = answer

        response["counter_rows"] = len(self.get_counter_rows)
        response["counter_unique_rows"] = len(self.get_counter_unique_rows)
        response["diff_counts"]["diff_non_unique"] = len(self.get_diff_non_unique)
        response["papers"]["diff_non_unique"] = self.get_diff_non_unique

        response["counter_published_in_2019"] = len(self.get_published_in_2019)
        response["diff_counts"]["diff_not_published_in_2019"] = len(self.get_diff_not_published_in_2019)
        response["papers"]["diff_not_published_in_2019"] = self.get_diff_not_published_in_2019

        response["counter_toll_access_published_in_2019"] = len(self.get_published_toll_access_in_2019)
        response["diff_counts"]["diff_open_access_journals"] =  len(self.get_diff_open_access_journals)
        response["papers"]["diff_open_access_journals"] =  self.get_diff_open_access_journals

        response["counter_toll_access_published_in_2019_with_elsevier"] = len(self.get_published_toll_access_in_2019_with_elsevier)
        response["diff_counts"]["diff_changed_publisher"] =  len(self.get_diff_changed_publisher)
        response["papers"]["diff_changed_publisher"] =  self.get_diff_changed_publisher

        response["published_toll_access_in_2019_with_elsevier_have_price"] = len(self.get_published_toll_access_in_2019_with_elsevier_have_price)
        response["diff_counts"]["diff_no_price"] =  len(self.get_diff_no_price)
        response["papers"]["diff_no_price"] =  self.get_diff_no_price

        response["papers"]["good_to_use"] =  self.get_published_toll_access_in_2019_with_elsevier_have_price

        return response


    def get_unexpectedly_no_price(self):
        package_id = self.package_id

        command = """select counter.issn_l, title, total::int as num_2018_downloads 
        from jump_counter counter
        left outer join ricks_journal on counter.issn_l = ricks_journal.issn_l
        where 
            package_id='{package_id}' 
            and counter.issn_l in ( 
                select distinct journal_issn_l from unpaywall u 
                where journal_issn_l in (	
                select jump_counter.issn_l from jump_counter
                 where package_id='{package_id}'	
                )
                and journal_is_oa='false'
                and year=2019
                and publisher ilike '%%elsevier%%'
                and journal_issn_l not in (
                    select jump_counter.issn_l from jump_counter
                    join jump_journal_prices on jump_journal_prices.issn_l = jump_counter.issn_l
                    where jump_counter.package_id='{package_id}' and jump_journal_prices.package_id='93YfzkaA')
                )
        order by num_2018_downloads desc
        """.format(package_id=package_id)
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()
        return rows

    def get_unexpectedly_no_price_and_greater_than_200_downloads(self):
        package_id = self.package_id
        rows = self.get_unexpectedly_no_price()
        answer_filtered = [row for row in rows if row["num_2018_downloads"] > 200]
        return answer_filtered


    def get_gold_oa(self):
        package_id = self.package_id

        command = """select counter.issn_l, title, total::int as num_2018_downloads 
        from jump_counter counter
        left outer join ricks_journal on counter.issn_l = ricks_journal.issn_l
        where 
            package_id='{package_id}' 
            and counter.issn_l not in ( 
                select distinct journal_issn_l from unpaywall u 
                where journal_issn_l in (	
                select jump_counter.issn_l from jump_counter
                 where package_id='{package_id}'	
                )
                and journal_is_oa='false'
                )
        order by num_2018_downloads desc
        """.format(package_id=package_id)
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()
        return rows


    def get_toll_access_no_2019_papers(self):
        package_id = self.package_id

        command = """select counter.issn_l, title, total::int as num_2018_downloads 
        from jump_counter counter
        left outer join ricks_journal on counter.issn_l = ricks_journal.issn_l
        where 
            package_id='{package_id}' 
            and counter.issn_l not in ( 
                select distinct journal_issn_l from unpaywall u 
                where journal_issn_l in (	
                select jump_counter.issn_l from jump_counter
                 where package_id='{package_id}'	
                )
                and journal_is_oa='false'
                )
        order by num_2018_downloads desc
        """.format(package_id=package_id)
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()
        return rows


    def to_dict_summary(self):

        return {
                "id": self.package_id,
                "name": self.package_name,
                "hasCounterData": self.has_counter_data,
                "numJournals": self.num_journals,
                "numPerpAccessJournals": self.num_perpetual_access_journals,
        }

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.package_id, self.package_name)


