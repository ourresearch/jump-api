import argparse
import re
from datetime import datetime
from dateutil.parser import parse
import requests
import csv
from requests.exceptions import RequestException

from app import db
from app import get_db_cursor
from openalex import OpenalexDBRaw
from openalex_date_last_doi import OpenalexDateLastDOIFromOA

# class Empty(object):
# 	pass
# self = Empty()
# self.__class__ = DateLastDOI

class DateLastDoiOA:
	def __init__(self, since_update_date=None):
		self.api_url = "https://api.openalex.org/works?filter=host_venue.id:{}&per_page=1&sort=publication_date:desc&mailto=scott@ourresearch.org"
		self.load_openalex()
		self.load_datelastdois()
		self.all_date_last_dois(since_update_date)

	def load_openalex(self):
		self.openalex_data = OpenalexDBRaw.query.all()
		for x in self.openalex_data:
			x.id_oa = re.search('V.+', x.id)[0]
		print(f"{len(self.openalex_data)} openalex_journals records found")

	def load_datelastdois(self):
		self.openalex_date_last_doi = OpenalexDateLastDOIFromOA.query.all()
		self.openalex_date_last_doi_issnls = [w.issn_l for w in self.openalex_date_last_doi]
		print(f"{len(self.openalex_date_last_doi)} openalex_date_last_doi_from_oa records found")

	def all_date_last_dois(self, since_update_date=None):
		if since_update_date:
			since_update_date_dt = parse(since_update_date)
			not_update = list(filter(lambda x: x.updated > since_update_date_dt, self.openalex_date_last_doi))
			not_update_issns = [w.issn_l for w in not_update]
			self.openalex_data = list(filter(lambda x: x.issn_l not in not_update_issns, self.openalex_data))
			print(f"Since update date: {since_update_date} - limiting to {len(self.openalex_data)} records")
		
		for x in self.openalex_data:
			self.date_last_doi(x)
			if getattr(x, 'date_last_doi', None):
				try:
					self.write_to_database(x)
				except Exception as e:
					pass

	def write_to_database(self, journal):
		res = OpenalexDateLastDOIFromOA.query.get(journal.issn_l)
		if not res:
			res = OpenalexDateLastDOIFromOA(journal)
			res.openalex_id = journal.id_oa
			db.session.add(res)
		else:
			res.date_last_doi = journal.date_last_doi
			res.openalex_id = journal.id_oa
			res.updated = datetime.utcnow().isoformat()

		db.session.commit()

	def date_last_doi(self, journal):
		try:
			r = requests.get(self.api_url.format(journal.id_oa))
			if r.status_code == 404:
				print(f"OpenAlex 404 for {journal.id_oa}, logging it")
				self.cache.log_404(journal.id_oa)
		except RequestException:
			print(f"OpenAlex request failed for {journal.id_oa} HTTP: ({r.status_code})")
			return None

		if (
			r.status_code == 200
			and r.json().get("results")
			and r.json()["results"][0]
		):
			if not r.json()["results"][0].get("publication_date"):
				print("issue with id {} (no 'publication_date' field found; logging).".format(journal.id_oa))
				self.cache.log_no_published(journal.id_oa)
			else:
				try:
					published = r.json()["results"][0]["publication_date"]
					self.set_last_doi_date(journal, published)
				except:
					print(
						"issue with OpenAlex ID {}".format(
							journal.issn_l
						)
					)
						
	@staticmethod
	def set_last_doi_date(journal, published):
		status_as_of = datetime.strptime(published, "%Y-%m-%d")
		journal.date_last_doi = status_as_of.strftime('%Y-%m-%d')
		print(
			"setting OpenAlex ID {} with date last doi of {}".format(
				journal.id_oa, status_as_of
			)
		)

# heroku local:run python date_last_doi_from_oa.py --update --since_update_date="2022-05-13 15:26:35.051186"
if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--update", help="Update date last DOI table", action="store_true", default=False)
	parser.add_argument("--since_update_date", help="Only work on ISSNs not updated since the date", default=None)
	parsed_args = parser.parse_args()

	if parsed_args.update:
		DateLastDoiOA(parsed_args.since_update_date)
