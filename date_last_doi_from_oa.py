import time
import argparse
import re
from datetime import datetime
from dateutil.parser import parse
import httpx
import asyncio
import csv

from psycopg2 import sql
from psycopg2.extras import execute_values
from openalex import OpenalexDBRaw
from openalex_date_last_doi import OpenalexDateLastDOIFromOA

def make_chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

# class Empty(object):
#   pass
# self = Empty()
# self.__class__ = DateLastDoiOA

class DateLastDoiOA:
    def __init__(self):
        self.api_url = "https://api.openalex.org/works?filter=host_venue.id:{}&per_page=1&sort=publication_date:desc&mailto=scott@ourresearch.org"
        self.table = "openalex_date_last_doi_from_oa"
        self.load_openalex()
        self.all_date_last_dois()

    def load_openalex(self):
        self.openalex_data = OpenalexDBRaw.query.all()
        for x in self.openalex_data:
            x.id_oa = re.search("V.+", x.id)[0]
            x.date_last_doi = None
        print(f"{len(self.openalex_data)} openalex_journals records found")

    def all_date_last_dois(self):
        self.openalex_data_chunks = list(make_chunks(self.openalex_data, 40))

        async def get_data(client, journal):
            try:
                r = await client.get(self.api_url.format(journal.id_oa), timeout = 10)
                if r.status_code == 404:
                    pass
            except httpx.RequestError:
                return None

            if (
                r.status_code == 200
                and r.json().get("results")
                and r.json()["results"][0]
            ):
                if not r.json()["results"][0].get("publication_date"):
                    pass
                else:
                    try:
                        published = r.json()["results"][0]["publication_date"]
                        self.set_last_doi_date(journal, published)
                    except:
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
        cols = OpenalexDateLastDOIFromOA.get_insert_column_names()
        input_values = [(datetime.utcnow().isoformat(), w.issn_l, w.id_oa, w.date_last_doi,) for w in data]

        from app import get_db_cursor

        with get_db_cursor() as cursor:
            qry = sql.SQL(
                "INSERT INTO openalex_date_last_doi_from_oa ({}) VALUES %s"
            ).format(sql.SQL(", ").join(map(sql.Identifier, cols)))
            execute_values(cursor, qry, input_values, page_size=40)

    @staticmethod
    def set_last_doi_date(journal, published):
        status_as_of = datetime.strptime(published, "%Y-%m-%d")
        journal.date_last_doi = status_as_of.strftime("%Y-%m-%d")


# heroku local:run python date_last_doi_from_oa.py --update
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--update",
        help="Update date last DOI table",
        action="store_true",
        default=False,
    )
    parsed_args = parser.parse_args()

    if parsed_args.update:
        DateLastDoiOA()
