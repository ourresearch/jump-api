# coding: utf-8

import datetime
import argparse
import simplejson as json
from kids.cache import cache
from cached_property import cached_property
from time import time
from psycopg2 import sql
from psycopg2.extras import execute_values
from enum import Enum

from app import db
from app import get_db_cursor
from util import elapsed
from util import chunks
from util import sql_bool
from util import sql_escape_string
from journalsdb_pricing import jdb_pricing
from openalex_date_last_doi import OpenalexDateLastDOI


class JiscDefaultPrices(Enum):
	TaylorFrancis = 954.10
	Sage = 659.99
	Wiley = 1350.47
	SpringerNature = 1476.53
	Elsevier = 3775

class OpenalexDBRaw(db.Model):
	__tablename__ = "openalex_journals"
	issn_l = db.Column(db.Text, primary_key=True)
	issn = db.Column(db.Text)
	display_name = db.Column(db.Text)
	is_oa = db.Column(db.Boolean)
	is_in_doaj = db.Column(db.Boolean)
	publisher = db.Column(db.Text)
	counts_by_year = db.Column(db.Text)
	x_concepts = db.Column(db.Text)
	updated_date = db.Column(db.DateTime)
	id = db.Column(db.Text)

	def __repr__(self):
		return "<{} ({}) '{}' {}>".format(self.__class__.__name__, self.issn_l, self.display_name, self.publisher)

class JournalMetadata(db.Model):
	__tablename__ = "openalex_computed"
	created = db.Column(db.DateTime)
	issn_l = db.Column(db.Text, primary_key=True)
	issns_string = db.Column(db.Text)
	title = db.Column(db.Text)
	publisher = db.Column(db.Text)
	is_current_subscription_journal = db.Column(db.Boolean)
	is_gold_journal_in_most_recent_year = db.Column(db.Boolean)
	is_currently_publishing = db.Column(db.Boolean)
	subscription_price_usd = db.Column(db.Numeric(asdecimal=False))
	subscription_price_gbp = db.Column(db.Numeric(asdecimal=False))
	apc_price_usd = db.Column(db.Numeric(asdecimal=False))
	apc_price_gbp = db.Column(db.Numeric(asdecimal=False))

	def __init__(self, journal_raw):
		self.now = datetime.datetime.utcnow()
		self.created = self.now.isoformat()
		for attr in ("issn_l", "publisher"):
			setattr(self, attr, getattr(journal_raw, attr))
		self.issns_string = journal_raw.issn
		for attr in ("title","is_current_subscription_journal",):
			setter = getattr(self, "set_{}".format(attr))
			setter(journal_raw)
		self.set_subscription_prices()
		self.set_apc_prices()
		super(JournalMetadata, self).__init__()

	@cached_property
	def issns(self):
		return json.loads(self.issns_string)

	@cached_property
	def display_issns(self):
		return ",".join(self.issns)

	@cached_property
	def display_issn_l(self):
		return "issn:{}".format(self.issn_l)

	@cached_property
	def is_hybrid(self):
		return not self.is_gold_journal_in_most_recent_year

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

	def get_insert_list(self):
		return (
			self.created,
			self.issn_l,
			self.issns_string,
			sql_escape_string(self.title),
			sql_escape_string(self.publisher),
			sql_bool(self.is_current_subscription_journal),
			sql_bool(self.is_gold_journal_in_most_recent_year),
			sql_bool(self.is_currently_publishing),
			self.subscription_price_usd,
			self.subscription_price_gbp,
			self.apc_price_usd,
			self.apc_price_gbp,)

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
				"apc_price_gbp",]

	def set_title(self, journal_raw):
		self.title = journal_raw.display_name

	def set_is_current_subscription_journal(self, journal_raw):
		self.set_is_currently_publishing(journal_raw)
		self.set_is_gold_journal_in_most_recent_year(journal_raw)
		self.set_subscription_prices()

		self.is_current_subscription_journal = True
		if not self.is_currently_publishing:
			self.is_current_subscription_journal = False
		if self.is_gold_journal_in_most_recent_year:
			self.is_current_subscription_journal = False

	def set_is_gold_journal_in_most_recent_year(self, journal_raw):
		self.is_gold_journal_in_most_recent_year = False
		if journal_raw.is_oa is not None or journal_raw.is_in_doaj is not None:
			self.is_gold_journal_in_most_recent_year = any([journal_raw.is_oa, journal_raw.is_in_doaj])

	def set_is_currently_publishing(self, journal_raw):
		self.is_currently_publishing = False
		match = last_dois_dict.get(self.issn_l)
		if match:
			if match.date_last_doi:
				date_last_doi_as_date = datetime.datetime.strptime(match.date_last_doi, "%Y-%m-%d")
				if (self.now - date_last_doi_as_date).days < 365:
					self.is_currently_publishing = True
		else:
			if journal_raw.counts_by_year:
				dois = json.loads(journal_raw.counts_by_year)
				for row in dois:
					if row['year'] == this_year_ish() and row['works_count'] > 0:
						self.is_currently_publishing = True
		# TODO: hack for Scientific American, take out when fixed in metadata
		if self.issn_l == '0036-8733':
			self.is_currently_publishing = True

	def set_subscription_prices(self):
		jdb = jdb_pricing().get(self.issn_l)
		if jdb:
			self.subscription_price_usd = jdb.subscription_price_usd
			self.subscription_price_gbp = jdb.subscription_price_gbp
	
	def set_apc_prices(self):
		jdb = jdb_pricing().get(self.issn_l)
		if jdb:
			self.apc_price_usd = jdb.apc_price_usd
			self.apc_price_gbp = jdb.apc_price_gbp

	def get_subscription_price(self, currency="USD", use_high_price_if_unknown=False):
		response = None
		if currency == "USD":
			if self.subscription_price_usd:
				response = float(self.subscription_price_usd)
		elif currency == "GBP":
			if self.subscription_price_gbp:
				response = float(self.subscription_price_gbp)

		if not response:
			if use_high_price_if_unknown and currency == "GBP":
				JISC_DEFAULT_PRICE_IN_GBP = JiscDefaultPrices[self.publisher_code].value
				response = JISC_DEFAULT_PRICE_IN_GBP
		return response

	def get_apc_price(self, currency="USD"):
		response = None
		if currency == "USD":
			if self.apc_price_usd:
				response = float(self.apc_price_usd)
		elif currency == "GBP":
			if self.apc_price_gbp:
				response = float(self.apc_price_gbp)
		return response

	def __repr__(self):
		return "<{} ({}) '{}' {}>".format(self.__class__.__name__, self.issn_l, self.title, self.publisher)

class JournalConcepts(object):
	def __init__(self, journal_raw):
		self.created = datetime.datetime.utcnow().isoformat()
		self.issn_l = journal_raw.issn_l
		self.x_concepts = json.loads(journal_raw.x_concepts)
		self.data = None
		self.set_data()
		super(JournalConcepts, self).__init__()

	@classmethod
	def get_insert_column_names(cls):
		return ["created",
				"issn_l",
				"concept",
				"level",
				"score",
				"openalex_url",
				"openalex_id",
				"wikidata_url",
				"wikidata_id"]

	def keys_map(self):
		return {v: i for i, v in enumerate(self.get_insert_column_names())}

	def set_data(self):
		if self.x_concepts:
			level_zero_one_concepts = list(filter(lambda x: x['level'] in (0,1), self.x_concepts))
			for concept in level_zero_one_concepts:
				concept['concept'] = concept.pop('display_name')
				concept['openalex_url'] = concept.pop('id')
				concept['openalex_id'] = concept['openalex_url'].split('/')[-1]
				concept['issn_l'] = self.issn_l
				concept['created'] = self.created
				concept['wikidata_url'] = concept.pop('wikidata')
				concept['wikidata_id'] = concept['wikidata_url'].split('/')[-1] if concept['wikidata_url'] else None
			res = [sorted(w.items(), key=lambda pair: self.keys_map()[pair[0]]) for w in level_zero_one_concepts]
			self.data = [tuple([z[1] for z in w]) for w in res]

	def __repr__(self):
		return "<{} ({})>".format(self.__class__.__name__, self.issn_l)

def this_year_ish():
	from dateutil.relativedelta import relativedelta
	now = datetime.datetime.now()
	current_year = int(now.strftime('%Y'))
	first_of_year = datetime.datetime(current_year, 1, 1) 
	diff = now - first_of_year
	# if it's less than 6 months after the new year, use the previous year
	if diff.days < 180:
		year = current_year - 1
	else:
		year = current_year
	return year

def recompute_journal_metadata():
	journals_raw = OpenalexDBRaw.query.all()
	print(f"retrieved {len(journals_raw)} records from openalex_journals")
	last_dois = OpenalexDateLastDOI.query.all()
	print(f"retrieved {len(last_dois)} records from openalex_date_last_doi")
	
	global last_dois_dict
	last_dois_dict = {}
	for x in last_dois:
		last_dois_dict[x.issn_l] = x

	print("making backups and getting tables ready to run")
	with get_db_cursor() as cursor:
		cursor.execute("drop table openalex_computed_bak_yesterday;")
		cursor.execute("create table openalex_computed_bak_yesterday as (select * from openalex_computed);")
		cursor.execute("drop table openalex_concepts_bak_yesterday;")
		cursor.execute("create table openalex_concepts_bak_yesterday as (select * from openalex_concepts);")

	# do it as its own to force commit
	with get_db_cursor() as cursor:
		cursor.execute("delete from openalex_computed")
		cursor.execute("delete from openalex_concepts")
	print("tables ready for insertion")

	new_computed_journals = []
	for journal_raw in journals_raw:
		new_journal_metadata = JournalMetadata(journal_raw)
		if new_journal_metadata.issns:
			new_computed_journals.append(new_journal_metadata)

	print("now commiting to openalex_computed")
	start_time = time()
	insert_values = [j.get_insert_list() for j in new_computed_journals]
	cols = JournalMetadata.get_insert_column_names()

	with get_db_cursor() as cursor:
		qry = sql.SQL("INSERT INTO openalex_computed ({}) VALUES %s").format(
			sql.SQL(', ').join(map(sql.Identifier, cols)))
		execute_values(cursor, qry, insert_values, page_size=1000)

	print("now refreshing openalex_computed_flat view")
	with get_db_cursor() as cursor:
		cursor.execute("refresh materialized view openalex_computed_flat;")
		cursor.execute("analyze openalex_computed;")

	new_computed_concepts = []
	for journal_raw in journals_raw:
		new_journal_concept = JournalConcepts(journal_raw)
		new_computed_concepts.append(new_journal_concept)

	concept_insert_values_tmp = [j.data for j in new_computed_concepts]
	concept_insert_values = []
	for index, x in enumerate(concept_insert_values_tmp):
		if x:
			concept_insert_values.extend(x)
	
	concept_cols = JournalConcepts.get_insert_column_names()

	print("now commiting to openalex_concepts")
	with get_db_cursor() as cursor:
		qry = sql.SQL("INSERT INTO openalex_concepts ({}) VALUES %s").format(
			sql.SQL(', ').join(map(sql.Identifier, concept_cols)))
		execute_values(cursor, qry, concept_insert_values, page_size=1000)

	# print("adding sort key (issn_l) to openalex_concepts")
	# with get_db_cursor() as cursor:
	#     cursor.execute("alter table openalex_concepts alter sortkey(issn_l)")

	print("done writing to db, took {} seconds total".format(elapsed(start_time)))

# load issns from openalex_computed_flat
with get_db_cursor() as cursor:
	cursor.execute("select issn from openalex_computed_flat")
	rows = cursor.fetchall()

oa_issns = [w[0] for w in rows]

class MissingJournalMetadata(object):
	def __init__(self, issn_l):
		self.issn_l = issn_l
		# print("in MissingJournalMetadata missing journal {} from openalex: https://api.openalex.org/venues/issn:{}".format(issn_l, issn_l))
		super(MissingJournalMetadata, self).__init__()

	@cached_property
	def display_issn_l(self):
		return "issn:{}".format(self.issn_l)

	@cached_property
	def issns(self):
		return [self.issn_l]

	@cached_property
	def is_hybrid(self):
		return None

	@cached_property
	def display_issns(self):
		return ",".join(self.issns)

	@cached_property
	def title(self):
		return "Unrecognized Title"

	@cached_property
	def publisher(self):
		return "Unrecognized Publisher"

	def get_apc_price(self, currency):
		return None

	@cached_property
	def get_subscription_price(self, currency, use_high_price_if_unknown=False):
		return None

# python openalex.py --recompute
# heroku run --size=performance-l python openalex.py --recompute -r heroku
# heroku local:run python openalex.py --recompute
if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument("--recompute", help="Update journal metadata", action="store_true", default=False)
	parsed_args = parser.parse_args()

	if parsed_args.recompute:
		recompute_journal_metadata()
