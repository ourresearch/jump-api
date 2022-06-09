import time
import argparse
import re
from datetime import datetime
from dateutil.parser import parse
import httpx
import asyncio

from app import db
from psycopg2 import sql
from psycopg2.extras import execute_values
from openalex import OpenalexDBRaw

def make_chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

# class Empty(object):
#   pass
# self = Empty()
# self.__class__ = MakeNumPapers

class NumPapers(db.Model):
    __tablename__ = "jump_num_papers_oa"
    updated = db.Column(db.DateTime)
    venue_id = db.Column(db.Text)
    issn_l = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Integer)
    num_papers = db.Column(db.Integer)

    def __repr__(self):
        return "<{} {} ({})>".format(self.__class__.__name__, self.issn_l, self.venue_id)

class MakeNumPapers:
    def __init__(self):
        self.api_url = "https://api.openalex.org/venues/issn:{}?mailto=scott@ourresearch.org"
        self.table = NumPapers.__tablename__
        self.load_openalex()
        self.gather_papers()

    def load_openalex(self):
        self.openalex_data = OpenalexDBRaw.query.all()
        for x in self.openalex_data:
            x.venue_id = None
            x.data = None
        print(f"{len(self.openalex_data)} openalex_journals records found")

    def gather_papers(self):
        self.openalex_data_chunks = list(make_chunks(self.openalex_data, 40))

        async def get_data(client, journal):
            try:
                r = await client.get(self.api_url.format(journal.issn_l), timeout = 10)
                if r.status_code == 404:
                    pass
            except httpx.RequestError:
                return None

            try:
                res = r.json()
                self.set_data(journal, re.search("V.+", res['id'])[0], res['counts_by_year'])
            except:
                print(f"http request error for: {journal.issn_l}")
                pass

        async def fetch_chunks(lst):
            async with httpx.AsyncClient() as client:
                tasks = []
                for s in lst:
                    tasks.append(asyncio.ensure_future(get_data(client, s)))

                async_results = await asyncio.gather(*tasks)
                return async_results

        from app import get_db_cursor

        with get_db_cursor() as cursor:
            print(f"deleting all rows in {self.table}")
            cursor.execute(f"truncate table {self.table}")

        print(f"querying OpenAlex API and writing each chunk to {self.table}")
        for i, item in enumerate(self.openalex_data_chunks):
            asyncio.run(fetch_chunks(item))
            self.write_to_db(item)
            time.sleep(1)

    def write_to_db(self, data):
        cols = NumPapers.__table__.columns.keys()
        all_rows = []
        for d in data:
            try:
                updated = datetime.utcnow().isoformat()
                rows = [(updated, d.venue_id, d.issn_l, w['year'], w['works_count']) for w in d.data]
                all_rows.append(rows)
            except:
                pass
        inputs = [w for sublist in all_rows for w in sublist]

        from app import get_db_cursor

        with get_db_cursor() as cursor:
            qry = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
                table = sql.Identifier(self.table),
                cols = sql.SQL(", ").join(map(sql.Identifier, cols)))
            execute_values(cursor, qry, inputs, page_size=1000)

    @staticmethod
    def set_data(journal, venue_id, data):
        journal.venue_id = venue_id
        journal.data = data


# heroku local:run python jump_num_papers.py --update
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--update", help="Update jump_num_papers_oa table", action="store_true", default=False,
    )
    parsed_args = parser.parse_args()

    if parsed_args.update:
        MakeNumPapers()
