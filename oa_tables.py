import time
import argparse
import re
from collections import defaultdict
from datetime import datetime
from dateutil.parser import parse
import httpx
import asyncio

from app import db
from psycopg2 import sql
from psycopg2.extras import execute_values
from openalex import OpenalexDBRaw

def make_params(venue, oa_status, submitted):
    parts = [
        f"host_venue.id:{venue}", 
        f"oa_status:{oa_status}",]
    if submitted == "false":
        parts.append("has_oa_submitted_version:false")
    return ",".join(parts)

tables = {
    "jump_oa_with_submitted_with_bronze":
        """
        insert into jump_oa_with_submitted_with_bronze (updated, venue_id, issn_l, fresh_oa_status, year_int, count) (
            select sysdate,venue_id,issn_l,fresh_oa_status,year_int, sum(count) as sum_count from jump_oa_all_vars
            where with_submitted
            group by 2,3,4,5
        )
        """,
    "jump_oa_with_submitted_no_bronze":
        """
        insert into jump_oa_with_submitted_no_bronze (select * from jump_oa_with_submitted_with_bronze where fresh_oa_status != 'bronze')
        """,
    "jump_oa_no_submitted_with_bronze":
        """
        insert into jump_oa_no_submitted_with_bronze (updated, venue_id, issn_l, fresh_oa_status, year_int, count) (
            select sysdate,venue_id,issn_l,fresh_oa_status,year_int, sum(count) as sum_count from jump_oa_all_vars
            where not has_oa_submitted
            group by 2,3,4,5
        )
        """,
    "jump_oa_no_submitted_no_bronze":
        """
        insert into jump_oa_no_submitted_no_bronze (select * from jump_oa_no_submitted_with_bronze where fresh_oa_status != 'bronze')
        """,
}

def truncate_table(table):
    with get_db_cursor() as cursor:
        print(f"deleting all rows in table: {table}")
        cursor.execute(f"truncate table {table}")

def update_table(table, qry):
    with get_db_cursor() as cursor:
        print(f"updating table: {table}")
        cursor.execute(qry)

# class Empty(object):
#   pass
# self = Empty()
# self.__class__ = OpenAccessTables
# since_update_date=None

class OpenAccessTables:
    def __init__(self, since_update_date=None, truncate=False):
        self.truncate = truncate
        self.api_url = "https://api.openalex.org/works?group_by=publication_year&mailto=scott@ourresearch.org&filter="
        self.years = list(range(2010, datetime.now().year + 1))
        self.oa_statuses = ['gold','green','bronze','hybrid',]
        self.oa_submitted = ["none","false",]
        self.table = "jump_oa_all_vars_new"
        self.load_openalex()
        self.make_tables(since_update_date)

    def load_openalex(self):
        self.openalex_data = OpenalexDBRaw.query.all()
        for x in self.openalex_data:
            x.venue_id = re.search("V.+", x.id)[0]
            x.data = {}
            for oa_status in self.oa_statuses:
                x.data[oa_status] = dict((el, None) for el in self.oa_submitted)
        print(f"{len(self.openalex_data)} openalex_journals records found")

    def make_tables(self, since_update_date=None):
        if since_update_date:
            len_original = len(self.openalex_data)

            from app import get_db_cursor
            with get_db_cursor() as cursor:
                cursor.execute(f"select distinct(issn_l) from {self.table} where updated <= '{since_update_date}'")
                rows = cursor.fetchall()

            not_update_issns = [w[0] for w in rows]
            self.openalex_data = list(filter(lambda x: x.issn_l not in not_update_issns, self.openalex_data))
            print(f"Since update date: {since_update_date} - limiting to {len(self.openalex_data)} ISSNs (of {len_original})")

        async def get_data(client, journal, oa_status, submitted):
            try:
                url = self.api_url + make_params(journal.venue_id, oa_status, submitted)
                # r = httpx.get(url)
                r = await client.get(url, timeout = 10)
                if r.status_code == 404:
                    pass
            except httpx.RequestError:
                return None

            try:
                res = r.json()
                [w.pop('key_display_name') for w in res['group_by']]
                res['group_by'] = list(filter(lambda x: int(x['key']) in self.years, res['group_by']))
                self.set_data(journal, oa_status, submitted, res['group_by'])
            except:
                print(f"http request error for: {journal.issn_l}")
                pass

        async def fetch_many(j):
            async with httpx.AsyncClient() as client:
                tasks = []
                for oa_status in self.oa_statuses:
                    for submitted in self.oa_submitted:
                        tasks.append(asyncio.ensure_future(get_data(client, j, oa_status, submitted)))

                async_results = await asyncio.gather(*tasks)
                return async_results

        if self.truncate:
            from app import get_db_cursor
            with get_db_cursor() as cursor:
                print(f"deleting all rows in {self.table}")
                cursor.execute(f"truncate table {self.table}")

        print(f"querying OpenAlex API and writing data to {self.table}")
        for j in self.openalex_data:
            asyncio.run(fetch_many(j))
            self.write_to_db(j)

    def write_to_db(self, dat):
        cols = ['updated','venue_id','issn_l','fresh_oa_status','year_int','count','with_submitted',]
        all_rows = []
        updated = datetime.utcnow().isoformat()
        try:
            for oa_status in dat.data.keys():
                for key, value in dat.data[oa_status].items():
                    if value:
                        for years in value:
                            all_rows.append((updated, dat.venue_id, dat.issn_l, oa_status, 
                                int(years['key']), years['count'], False if key == "false" else True))
        except:
            pass

        from app import get_db_cursor

        with get_db_cursor() as cursor:
            qry = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
                table = sql.Identifier(self.table),
                cols = sql.SQL(", ").join(map(sql.Identifier, cols)))
            execute_values(cursor, qry, all_rows, page_size=1000)

    @staticmethod
    def set_data(journal, oa_status, submitted, data):
        journal.data[oa_status][submitted] = data


# heroku local:run python oa_tables.py --update
# heroku local:run python oa_tables.py --update --truncate
# heroku local:run python oa_tables.py --update --truncate --update_tables
# just update tables: heroku local:run python oa_tables.py --update_tables
# heroku local:run python oa_tables.py --update --since_update_date="2022-05-23 23:49:29.839859"
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--update", help="Update jump_oa_all_vars table from OpenAlex", action="store_true", default=False)
    parser.add_argument("--since_update_date", help="Only work on ISSNs not updated since the date", default=None)
    parser.add_argument("--truncate", help="Drop all rows in jump_oa_all_vars table before running?", action="store_true", default=False)
    parser.add_argument("--update_tables", help="Update jump_oa_* tables after jump_oa_all_vars updated?", action="store_true", default=False)
    parsed_args = parser.parse_args()

    if parsed_args.update:
        OpenAccessTables(parsed_args.since_update_date, parsed_args.truncate)

    if parsed_args.update_tables:
        from app import get_db_cursor
        for table in tables.keys():
            truncate_table(table)

        from app import get_db_cursor
        for table, qry in tables.items():
            update_table(table, qry)
