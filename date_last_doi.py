import argparse
import datetime
import requests
import csv
from requests.exceptions import RequestException

from app import db
from app import get_db_cursor
from util import safe_commit
from psycopg2 import sql
from psycopg2.extras import execute_values
from openalex import JournalMetadata
# from date_last_doi import DateLastDOI, OpenalexDateLastDOI

# res = DateLastDOI()
# class Empty(object):
# 	pass
# self = Empty()
# self.__class__ = DateLastDOI
# self.load_openalex

FILE_PATH = "date_last_doi_{}.txt".format(datetime.datetime.now().strftime("%Y_%m_%d_%H"))

class OpenalexDateLastDOI(db.Model):
	__tablename__ = "openalex_date_last_doi"
	created = db.Column(db.DateTime)
	issn_l = db.Column(db.Text, primary_key=True)
	date_last_doi = db.Column(db.Text)

	def __init__(self, journal):
		self.created = datetime.datetime.utcnow().isoformat()
		for attr in ("issn_l", "date_last_doi"):
			setattr(self, attr, getattr(journal, attr))
		super(OpenalexDateLastDOI, self).__init__()

	def get_values(self):
		return (
			self.created,
			self.issn_l,
			self.date_last_doi,)

	@classmethod
	def get_insert_column_names(cls):
		return ["created",
				"issn_l",
				"date_last_doi",]

	def __repr__(self):
		return "<{} ({}) '{}'>".format(self.__class__.__name__, self.issn_l, self.date_last_doi)

class LastDate:
	def __init__(self, issn_l, date_last_doi):
		self.issn_l = issn_l
		self.date_last_doi = date_last_doi

	def get_obj(self):
		return OpenalexDateLastDOI(self)

	def __repr__(self):
		return "<{} ({}) '{}'>".format(self.__class__.__name__, self.issn_l, self.date_last_doi)

def write_to_database(data):
	cols = OpenalexDateLastDOI.get_insert_column_names()
	input_values = [LastDate(w[0], w[1]).get_obj().get_values() for w in data]

	with get_db_cursor() as cursor:
		qry = sql.SQL("INSERT INTO openalex_date_last_doi ({}) VALUES %s").format(
			sql.SQL(', ').join(map(sql.Identifier, cols)))
		execute_values(cursor, qry, input_values, page_size = 1000)

class DateLastDOI:
	def __init__(self, only_missing=False):
		self.file_path = FILE_PATH
		self.api_url = "https://api.crossref.org/journals/{}/works?sort=published&select=published&rows=1&mailto=scott@ourresearch.org"
		self.cols = OpenalexDateLastDOI.get_insert_column_names()
		self.load_openalex()
		self.load_datelastdois()
		self.all_date_last_dois(only_missing)

	def load_openalex(self):
		self.openalex_data = JournalMetadata.query.all()
		print(f"{len(self.openalex_data)} openalex_compute records found")

	def load_datelastdois(self):
		self.openalex_date_last_doi = OpenalexDateLastDOI.query.all()
		self.openalex_date_last_doi_issnls = [w.issn_l for w in self.openalex_date_last_doi]
		print(f"{len(self.openalex_date_last_doi)} openalex_date_last_doi records found")

	def all_date_last_dois(self, only_missing=False):
		if only_missing:
			self.openalex_data = list(filter(lambda x: x.issn_l not in self.openalex_date_last_doi_issnls, self.openalex_data))
		for x in self.openalex_data:
			# print(x.issn_l)
			# present = False
			# if only_missing:
			# 	present = self.in_table_already(x.issn_l)
			# if not present:
			self.date_last_doi(x)
			if getattr(x, 'date_last_doi', None):
				self.write_to_file(x)
				# self.write_to_database(x)
			# else:
			# 	self.keep_db_alive()
			# else:
			# 	print(f'{x.issn_l} already in database (& only_missing: {only_missing})')

	def in_table_already(self, issn_l):
		return issn_l in self.openalex_date_last_doi_issnls

	def keep_db_alive(self):
		with get_db_cursor() as cursor:
			cursor.execute("select count(*) from openalex_date_last_doi")

	def write_to_file(self, journal):
		with open(self.file_path, 'a') as f:
			writer = csv.writer(f)
			writer.writerow([journal.issn_l, journal.date_last_doi])

	def write_to_database(self, journal):
		out = OpenalexDateLastDOI(journal)
		values = out.get_values()

		with get_db_cursor() as cursor:
			cursor.execute(f"SELECT TOP 1 1 FROM openalex_date_last_doi WHERE issn_l = '{out.issn_l}'")
			exists = cursor.fetchone()
			
			if exists:
				cursor.execute(f"delete from openalex_date_last_doi where issn_l = '{out.issn_l}'")
			
			qry = sql.SQL("INSERT INTO openalex_date_last_doi ({}) VALUES ({})").format(
				sql.SQL(', ').join(map(sql.Identifier, self.cols)),
				sql.SQL(', ').join(sql.Placeholder() * len(self.cols)))
			cursor.execute(qry, values)

	def date_last_doi(self, journal):
		try:
			r = requests.get(self.api_url.format(journal.issn_l))
			if r.status_code == 404:
				for issn in journal.issns:
					if journal.issn_l != issn:
						r = requests.get(self.api_url.format(issn))
						if r.status_code == 200:
							break
		except RequestException:
			# go to next record
			print(f"Crossref request failed for {journal.issn_l} HTTP: ({r.status_code})")
			return None

		if (
			r.status_code == 200
			and r.json().get("message")
			and r.json()["message"].get("items")
		):
			if not r.json()["message"]["items"][0].get("published"):
				print("issue with issn {} (no 'published' field found).".format(journal.issn_l))
			else:
				try:
					# full date
					published = r.json()["message"]["items"][0]["published"]
					year = published["date-parts"][0][0]
					month = published["date-parts"][0][1]
					day = published["date-parts"][0][2]
					self.set_last_doi_date(journal, year, month, day)
				except (KeyError, IndexError):
					try:
						# year and month only
						published = r.json()["message"]["items"][0]["published"]
						year = published["date-parts"][0][0]
						month = published["date-parts"][0][1]
						self.set_last_doi_date(journal, year, month, 1)
					except (KeyError, IndexError):
						try:
							# year only
							published = r.json()["message"]["items"][0]["published"]
							year = published["date-parts"][0][0]
							self.set_last_doi_date(journal, year, 1, 1)
						except (KeyError, IndexError):
							print(
								"issue with issn {} (index out of range).".format(
									journal.issn_l
								)
							)
						
	@staticmethod
	def set_last_doi_date(journal, year, month, day):
		recent_article_date = "{} {} {}".format(year, month, day)
		status_as_of = datetime.datetime.strptime(recent_article_date, "%Y %m %d")
		# handle manual input of a recent date
		# if not journal.date_last_doi or (
		#     journal.date_last_doi and status_as_of > journal.date_last_doi
		# ):
		journal.date_last_doi = status_as_of.strftime('%Y-%m-%d')
		print(
			"setting issn {} with date last doi of {}".format(
				journal.issn_l, status_as_of
			)
		)

# python date_last_doi.py --update
# heroku run --size=performance-l python date_last_doi.py --update -r heroku
if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--update", help="Update date last DOI table", action="store_true", default=False)
	parser.add_argument("--only_missing", help="Only run for ISSNs w/o data?", action="store_true", default=False)
	parser.add_argument("--write_to_db", help="Write data in txt file to database?", action="store_true", default=False)
	parsed_args = parser.parse_args()

	if parsed_args.update:
		DateLastDOI(parsed_args.only_missing)

	if parsed_args.write_to_db:
		with open(FILE_PATH) as f:
			lns = f.read()
		tmp = [w.split(',') for w in lns.split()]
		write_to_database(tmp)
