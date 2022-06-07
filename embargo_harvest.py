import os
import click
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
    def __init__(self, use_cache = True, truncate = False, since_update_date = None):
        self.record_file = "oa_works_cache.csv"
        self.api_url = "https://api.openaccessbutton.org/permissions/{}"
        self.table = "jump_embargo"
        self.load_openalex()
        self.harvest_embargos(use_cache, truncate, since_update_date)

    def load_openalex(self):
        self.openalex_data = OpenalexDBRaw.query.all()
        for x in self.openalex_data:
            x.embargo_months = None
        print(f"{len(self.openalex_data)} openalex_journals records found")

    def harvest_embargos(self, use_cache, truncate, since_update_date):
        if use_cache:
            with open(self.record_file, 'r') as f:
                cached_issns = f.read().splitlines()

            len_original = len(self.openalex_data)
            self.openalex_data = list(filter(lambda x: x.issn_l not in cached_issns, self.openalex_data))
            print(f"Found {len(cached_issns)} in {self.record_file} cache file - limiting to {len(self.openalex_data)} ISSNs (of {len_original})")

        if since_update_date:
            from app import get_db_cursor
            with get_db_cursor() as cursor:
                cursor.execute(f"select distinct(issn_l) from {self.table} where updated <= '{since_update_date}'")
                rows = cursor.fetchall()

            len_original = len(self.openalex_data)
            not_update_issns = [w[0] for w in rows]
            self.openalex_data = list(filter(lambda x: x.issn_l not in not_update_issns, self.openalex_data))
            print(f"Since update date: {since_update_date} - limiting to {len(self.openalex_data)} ISSNs (of {len_original})")
        
        if truncate:
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

    def record(self, issn_l):
        with open(self.record_file, 'a') as f:
            f.write("\n" + issn_l)

    def fetch_embargo(self, journal):
        try:
            headers = {'X-apikey': os.getenv('OA_WORKS_KEY')}
            r = requests.get(self.api_url.format(journal.issn_l), )
            if r.status_code == 404:
                print(f"(oa.works) 404 for {journal.issn_l}")
        except requests.RequestException:
            print(f"(oa.works) request failed for {journal.issn_l} HTTP: ({r.status_code})")

        self.record(journal.issn_l)

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

@click.command()
@click.option('--update', help='Update embargo data from oa.works', is_flag=True)
@click.option('--use_cache', help='Use cache file for ISSNs already queried (b/c many ISSNs have no embargo data, so wont be captured w/ since_update_date)?', is_flag=True)
@click.option('--truncate', help='Drop all rows in jump_embargo table before running?', is_flag=True)
@click.option('--since_update_date', help='A publisher', required=False, default=None)
# heroku local:run python embargo_harvest.py --update --use_cache 
# heroku local:run python embargo_harvest.py --update --use_cache --since_update_date="2022-05-13 15:26:35.051186"
# heroku run:detached --size=performance-l python embargo_harvest.py --update -r heroku
def embargo_harvest(update, use_cache, truncate, since_update_date):
    if since_update_date:
        truncate = False

    click.echo("Arguments:")
    click.echo(f"  use_cache: {use_cache}")
    click.echo(f"  truncate: {truncate}")
    click.echo(f"  since_update_date: {since_update_date}")

    if update:
        Embargo(use_cache, truncate, since_update_date)

    click.echo("Done!")

if __name__ == '__main__':
    embargo_harvest()
