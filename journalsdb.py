# coding: utf-8

import datetime
import argparse
import simplejson as json
from cached_property import cached_property
from time import time

from app import db
from app import get_db_cursor
from util import elapsed
from util import chunks
from util import sql_bool
from util import sql_escape_string

def recompute_journal_metadata():
    journals_raw = JournalsDBRaw.query.all()
    print len(journals_raw)

    new_computed_journals = []

    with get_db_cursor() as cursor:
       cursor.execute("truncate journalsdb_computed;")

    for journal_raw in journals_raw:
        new_journal_metadata = JournalMetadata(journal_raw)
        new_computed_journals.append(new_journal_metadata)
        print "X",

    print "starting commits"
    start_time = time()
    insert_values_list = [j.get_insert_values() for j in new_computed_journals]
    command_start = u"""INSERT INTO journalsdb_computed ({}) VALUES """.format(
        ",".join(JournalMetadata.get_insert_column_names()))

    with get_db_cursor() as cursor:
        i = 0
        for short_values_list in chunks(insert_values_list, 1000):
            values_list_string = u",".join(short_values_list)
            q = u"{} {};".format(command_start, values_list_string)
            cursor.execute(q)
            i += 1
            print i
    print u"done committing journals, took {} seconds total".format(elapsed(start_time))
    print u"now refreshing flat view"

    with get_db_cursor() as cursor:
       cursor.execute("refresh materialized view journalsdb_computed_flat;")

    print u"done writing to db, took {} seconds total".format(elapsed(start_time))


class JournalsDBRaw(db.Model):
    __tablename__ = "journalsdb_raw"
    issn_l = db.Column(db.Text, primary_key=True)
    issns = db.Column(db.Text)
    title = db.Column(db.Text)
    publisher = db.Column(db.Text)
    dois_by_issued_year = db.Column(db.Text)
    subscription_pricing = db.Column(db.Text)
    apc_pricing = db.Column(db.Text)
    open_access = db.Column(db.Text)

    def __repr__(self):
        return u"<{} ({}) '{}' {}>".format(self.__class__.__name__, self.issn_l, self.title, self.publisher)


class JournalMetadata(db.Model):
    __tablename__ = "journalsdb_computed"
    created = db.Column(db.DateTime)
    issn_l = db.Column(db.Text, primary_key=True)
    issns_string = db.Column(db.Text)
    title = db.Column(db.Text)
    publisher = db.Column(db.Text)
    is_current_subscription_journal = db.Column(db.Boolean)
    is_gold_journal_in_most_recent_year = db.Column(db.Boolean)
    is_currently_publishing = db.Column(db.Boolean)
    subscription_price_usd = db.Column(db.Numeric)
    subscription_price_gbp = db.Column(db.Numeric)
    apc_price_usd = db.Column(db.Numeric)
    apc_price_gbp = db.Column(db.Numeric)
    num_dois_in_2020 = db.Column(db.Numeric)

    def __init__(self, journal_raw):
        self.created = datetime.datetime.utcnow().isoformat()
        for attr in ("issn_l", "title", "publisher"):
            setattr(self, attr, getattr(journal_raw, attr))
        self.issns_string = journal_raw.issns
        for attr in ("is_current_subscription_journal", "is_gold_journal_in_most_recent_year", "is_currently_publishing", "num_dois_in_2020", ):
            setter = getattr(self, "set_{}".format(attr))
            setter(journal_raw)
        self.set_subscription_prices(journal_raw)
        self.set_apc_prices(journal_raw)
        super(JournalMetadata, self).__init__()

    @cached_property
    def issns(self):
        return json.loads(self.issns_string)

    @cached_property
    def publisher_code(self):
        if self.publisher == "Elsevier":
            return "Elsevier"
        elif self.publisher == "Springer Nature":
            return "SpringerNature"
        elif self.publisher == "Wiley":
            return "Wiley"
        elif self.publisher == "SAGE":
            return "Sage"
        elif self.publisher == "Taylor & Francis":
            return "TaylorFrancis"
        return self.publisher

    def get_insert_values(self):
        response = u"""(
                    '{created}', 
                    '{issn_l}', 
                    '{issns_string}', 
                    '{title}', 
                    '{publisher}', 
                    {is_current_subscription_journal}, 
                    {is_gold_journal_in_most_recent_year},
                    {is_currently_publishing},
                    {subscription_price_usd},
                    {subscription_price_gbp},
                    {apc_price_usd},
                    {apc_price_gbp},
                    {num_dois_in_2020}  
                )
                """.format(created=self.created,
                           issn_l=self.issn_l,
                           issns_string=self.issns_string,
                           title=sql_escape_string(self.title),
                           publisher=sql_escape_string(self.publisher),
                          is_current_subscription_journal=sql_bool(self.is_current_subscription_journal),
                          is_gold_journal_in_most_recent_year=sql_bool(self.is_gold_journal_in_most_recent_year),
                          is_currently_publishing=sql_bool(self.is_currently_publishing),
                          subscription_price_usd=self.subscription_price_usd,
                          subscription_price_gbp=self.subscription_price_gbp,
                          apc_price_usd=self.apc_price_usd,
                          apc_price_gbp=self.apc_price_gbp,
                          num_dois_in_2020=self.num_dois_in_2020)
        response = response.replace("None", "null")
        return response

    @classmethod
    def get_insert_column_names(cls):
        return ["created",
                "issn_l",
                "issns_string",
                "title",
                "publisher",
                "is_current_subscription_journal",
                "is_gold_journal_in_most_recent_year",
                "is_currently_publishing",
                "subscription_price_usd",
                "subscription_price_gbp",
                "apc_price_usd",
                "apc_price_gbp",
                "num_dois_in_2020"
                ]

    def set_is_current_subscription_journal(self, journal_raw):
        self.set_is_currently_publishing(journal_raw)
        self.set_is_gold_journal_in_most_recent_year(journal_raw)
        self.set_subscription_prices(journal_raw)

        self.is_current_subscription_journal = True
        if not (self.subscription_price_usd or self.subscription_price_gbp):
            if not self.is_currently_publishing:
                self.is_current_subscription_journal = False
            if self.is_gold_journal_in_most_recent_year:
                self.is_current_subscription_journal = False

    def set_is_gold_journal_in_most_recent_year(self, journal_raw):
        self.is_gold_journal_in_most_recent_year = False
        if journal_raw.open_access:
            self.is_gold_journal_in_most_recent_year = (json.loads(journal_raw.open_access)["gold_rate"] == 1)

    def set_is_currently_publishing(self, journal_raw):
        self.is_currently_publishing = False
        if journal_raw.dois_by_issued_year:
            dois_tuple = json.loads(journal_raw.dois_by_issued_year)
            for (year, num) in dois_tuple:
                if year == 2021 and num > 0:
                    self.is_currently_publishing = True

    def set_num_dois_in_2020(self, journal_raw):
        self.num_dois_in_2020 = 0
        if journal_raw.dois_by_issued_year:
            dois_tuple = json.loads(journal_raw.dois_by_issued_year)
            for (year, num) in dois_tuple:
                if year == 2020:
                    self.num_dois_in_2020 = num

    def set_subscription_prices(self, journal_raw):
        if journal_raw.subscription_pricing:
            subscription_dict = json.loads(journal_raw.subscription_pricing)
            for price_dict in subscription_dict["prices"]:
                if price_dict["currency"] == "USD":
                    self.subscription_price_usd = price_dict["price"]
                if price_dict["currency"] == "GBP":
                    self.subscription_price_gbp = price_dict["price"]

    def set_apc_prices(self, journal_raw):
        if journal_raw.apc_pricing:
            apc_dict = json.loads(journal_raw.apc_pricing)
            for price_dict in apc_dict["apc_prices"]:
                if price_dict["currency"] == "USD":
                    self.apc_price_usd = price_dict["price"]
                if price_dict["currency"] == "GBP":
                    self.apc_price_gbp = price_dict["price"]

    def get_subscription_price(self, currency="USD", use_high_price_if_unknown=False):
        if currency == "USD":
            response = self.subscription_price_usd
        elif currency == "GBP":
            response = self.subscription_price_gbp

        if not response:
            if use_high_price_if_unknown:
                response = 9999999999
        return response

    def __repr__(self):
        return u"<{} ({}) '{}' {}>".format(self.__class__.__name__, self.issn_l, self.title, self.publisher)


# python journalsdb.py --recompute
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--recompute", help="Update journal metadata", action="store_true", default=False)
    parsed_args = parser.parse_args()

    if parsed_args.recompute:
        recompute_journal_metadata()