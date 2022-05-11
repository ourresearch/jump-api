import argparse
from datetime import datetime
import requests
import csv
from requests.exceptions import RequestException
import sqlite3

from app import db
from app import get_db_cursor
from util import safe_commit
from psycopg2 import sql
from psycopg2.extras import execute_values
from openalex import JournalMetadata
from openalex_date_last_doi import OpenalexDateLastDOI
# from date_last_doi import DateLastDOI, Cache

# res = DateLastDOI()
# class Empty(object):
# 	pass
# self = Empty()
# self.__class__ = DateLastDOI
# self.load_openalex

FILE_PATH = "date_last_doi_{}.txt".format(datetime.now().strftime("%Y_%m_%d_%H"))

class Cache(object):
	"""Cache for Crossref 404s"""
	def __init__(self):
		self.now = datetime.now().strftime("%Y-%m-%d")
		self.con = sqlite3.connect('crossref.db')
		self.con.execute('CREATE TABLE if NOT EXISTS crossref404 (date text, issn_l text)')
		self.con.execute('CREATE TABLE if NOT EXISTS crossref_no_published (date text, issn_l text)')
		super(Cache, self).__init__()

	def sqlite_query(self, qry):
		with self.con as cursor:
			res = cursor.execute(qry)
			rows = res.fetchall()
		return rows

	def sqlite_insert(self, table, issn_l):
		with self.con as cursor:
			res = cursor.execute(f"INSERT INTO {table} VALUES (?, ?)", (self.now, issn_l))
		self.con.commit()

	def sqlite_delete(self, table, issn_l):
		with self.con as cursor:
			res = cursor.execute(f"DELETE FROM {table} WHERE issn_l = '{issn_l}'")
		self.con.commit()

	def exists_404(self, issn_l):
		return len(self.sqlite_query(f"SELECT * FROM crossref404 WHERE issn_l = '{issn_l}'")) > 0
	def exists_no_published(self, issn_l):
		return len(self.sqlite_query(f"SELECT * FROM crossref_no_published WHERE issn_l = '{issn_l}'")) > 0

	def log_404(self, issn_l):
		return self.sqlite_insert('crossref404', issn_l)
	def log_no_published(self, issn_l):
		return self.sqlite_insert('crossref_no_published', issn_l)

	def delete_404(self, issn_l):
		return self.sqlite_delete('crossref404', issn_l)
	def delete_no_published(self, issn_l):
		return self.sqlite_delete('crossref_no_published', issn_l)

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
	def __init__(self, only_missing=False, skip404=False, skipnopub=False):
		self.file_path = FILE_PATH
		self.api_url = "https://api.crossref.org/journals/{}/works?sort=published&select=published&rows=1&mailto=scott@ourresearch.org"
		self.cache = Cache()
		self.cols = OpenalexDateLastDOI.get_insert_column_names()
		self.load_openalex()
		self.load_datelastdois()
		self.all_date_last_dois(only_missing, skip404, skipnopub)

	def load_openalex(self):
		self.openalex_data = JournalMetadata.query.all()
		print(f"{len(self.openalex_data)} openalex_compute records found")

	def load_datelastdois(self):
		self.openalex_date_last_doi = OpenalexDateLastDOI.query.all()
		self.openalex_date_last_doi_issnls = [w.issn_l for w in self.openalex_date_last_doi]
		print(f"{len(self.openalex_date_last_doi)} openalex_date_last_doi records found")

	def all_date_last_dois(self, only_missing=False, skip404=False, skipnopub=False):
		if only_missing:
			self.openalex_data = list(filter(lambda x: x.issn_l not in self.openalex_date_last_doi_issnls, self.openalex_data))
		for x in self.openalex_data:
			# print(x.issn_l)
			# present = False
			# if only_missing:
			# 	present = self.in_table_already(x.issn_l)
			# if not present:
			if skip404:
				if self.cache.exists_404(x.issn_l):
					continue

			if skipnopub:
				if self.cache.exists_no_published(x.issn_l):
					continue

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
				print(f"Crossref 404 for {journal.issn_l}, logging it")
				self.cache.log_404(journal.issn_l)
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
				print("issue with issn {} (no 'published' field found; logging).".format(journal.issn_l))
				self.cache.log_no_published(journal.issn_l)
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
		status_as_of = datetime.strptime(recent_article_date, "%Y %m %d")
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
# heroku local:run python date_last_doi.py --update
# heroku local:run python date_last_doi.py --update --only_missing
# heroku local:run python date_last_doi.py --update --only_missing --skip404 --skipnopub
# heroku local:run python date_last_doi.py --write_to_db=filepath
if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--update", help="Update date last DOI table", action="store_true", default=False)
	parser.add_argument("--only_missing", help="Only run for ISSNs w/o data?", action="store_true", default=False)
	parser.add_argument("--write_to_db", help="Write data in txt file to database?", type = str)
	parser.add_argument("--skip404", help="Skip Crossref 404s?", action="store_true", default=False)
	parser.add_argument("--skipnopub", help="Skip Crossref responses that have no 'published' field?", action="store_true", default=False)
	parsed_args = parser.parse_args()

	if parsed_args.update:
		DateLastDOI(parsed_args.only_missing, parsed_args.skip404, parsed_args.skipnopub)

	if parsed_args.write_to_db:
		print(f"reading from: {parsed_args.write_to_db}")
		with open(parsed_args.write_to_db) as f:
			lns = f.read()
		tmp = [w.split(',') for w in lns.split()]
		write_to_database(tmp)