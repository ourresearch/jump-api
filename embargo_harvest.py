import time
import argparse
import re
from datetime import datetime
from dateutil.parser import parse
import requests

from psycopg2 import sql
from psycopg2.extras import execute_values
from openalex import OpenalexDBRaw

# class Empty(object):
#   pass
# self = Empty()
# self.__class__ = Embargo

class Embargo:
    def __init__(self):
        self.api_url = "https://api.openaccessbutton.org/permissions/{}"
        self.table = "jump_embargo"
        self.load_openalex()
        self.harvest_embargos()

    def load_openalex(self):
        self.openalex_data = OpenalexDBRaw.query.all()
        for x in self.openalex_data:
            x.embargo_months = None
        print(f"{len(self.openalex_data)} openalex_journals records found")

    def harvest_embargos(self):
        from app import get_db_cursor
        with get_db_cursor() as cursor:
            print(f"deleting all rows in {self.table}")
            cursor.execute(f"truncate table {self.table}")
        
        for x in self.openalex_data:
            self.fetch_embargo(x)
            if x.embargo_months:
                self.write_to_db(x)

    def write_to_db(self, w):
        print(f"(oa.works) {w.issn_l} writing to db")
        cols = ['updated','issn_l','embargo_months','embargo_months_updated']
        input_values = (datetime.utcnow().isoformat(), w.issn_l, w.embargo_months, w.embargo_months_updated,)
        from app import get_db_cursor
        with get_db_cursor() as cursor:
            qry = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(self.table),
                sql.SQL(', ').join(map(sql.Identifier, cols)),
                sql.SQL(', ').join(sql.Placeholder() * len(cols)))
            cursor.execute(qry, input_values)

    def fetch_embargo(self, journal):
        try:
            r = requests.get(self.api_url.format(journal.issn_l))
            if r.status_code == 404:
                print(f"(oa.works) 404 for {journal.issn_l}")
        except RequestException:
            print(f"(oa.works) request failed for {journal.issn_l} HTTP: ({r.status_code})")

        if r.status_code == 200:
            if r.json().get("best_permission"):
                try:
                    months = r.json()["best_permission"].get("embargo_months")
                    updated = r.json()["best_permission"].get("meta").get("updated")
                    self.set_embargo(journal, months, updated)
                except (KeyError, IndexError):
                    print(f"(oa.works) issue with issn {journal.issn_l} (index out of range)")
            else:
                print(f"(oa.works) {journal.issn_l} not found")

    @staticmethod
    def set_embargo(journal, months, updated):
        journal.embargo_months = months
        try:
            journal.embargo_months_updated = parse(updated, dayfirst=True)
        except:
            pass

# heroku local:run python embargo_harvest.py --update
# heroku run --size=performance-l python embargo_harvest.py --update -r heroku
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--update",
        help="Update embargo data from oa.works",
        action="store_true",
        default=False,
    )
    parsed_args = parser.parse_args()

    if parsed_args.update:
        Embargo()
