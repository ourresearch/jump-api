import os
import time
import argparse
import re
import csv
import tempfile
import shortuuid
from collections import defaultdict
from datetime import datetime
from dateutil.parser import parse
import httpx
import asyncio
from statistics import mean
from collections import defaultdict
from psycopg2 import sql
from psycopg2.extras import execute_values
from sqlalchemy.sql import text

from app import db
from app import get_db_cursor
from app import s3_client
from util import safe_commit
from openalex import OpenalexDBRaw

def distinct_issnls(table):
    with get_db_cursor() as cursor:
        qry = sql.SQL("select distinct(issn_l) from {}").format( 
            sql.Identifier(table))
        print(cursor.mogrify(qry))
        cursor.execute(qry)
        rows = cursor.fetchall()
    return [w[0] for w in rows]

def query_one(issn_l):
    with get_db_cursor() as cursor:
        cols = ['issn_l','fresh_oa_status','year_int','count']
        qry = sql.SQL("select {} from {} where issn_l = %s").format( 
            sql.SQL(', ').join(map(sql.Identifier, cols)), sql.Identifier(table))
        # print(cursor.mogrify(qry, (issn_l,)))
        cursor.execute(qry, (issn_l,))
        rows = cursor.fetchall()
    return rows

def fetch_data_and_split(table):
    with get_db_cursor() as cursor:
        qry = sql.SQL("select * from {}").format(sql.Identifier(table))
        cursor.execute(qry)
        rows = cursor.fetchall()
    
    import pandas as pd
    df = pd.DataFrame([dict(w) for w in rows])
    df.issn_l = [w.strip() for w in df.issn_l] # some have weird leading chars
    grouped = df.groupby('issn_l')
    issn_dict = defaultdict(list)
    for name, group in grouped:
        issn_dict[name] = group.to_dict(orient='records')

    return issn_dict
    # issns = list(set([w['issn_l'] for w in rows]))
    # issn_dict = defaultdict(list)
    # for issn in issns:
    #     issn_dict[issn] = list(filter(lambda x: x['issn_l'] == issn, rows))
    #     # [x['issn_l'] == issn for x in issns[0:3]]
    # issn_dict = defaultdict(list, { k:[] for k in issns })
    # for issn in issns[0:100]:
    #     issn_dict[issn] = [w for w in rows if w['issn_l'] == issn]
    # return issn_dict

def delete_rows(table, x):
    keys_to_extract = ["issn_l", "fresh_oa_status", "year_int"]
    delete_data = [{key: w[key] for key in keys_to_extract} for w in x]
    delete_tuples = [tuple(w.values()) for w in delete_data]
    with get_db_cursor() as cursor:
        query_str = "delete from {} where (issn_l, fresh_oa_status, year_int) IN (%s)"
        qry = sql.SQL(query_str).format(sql.Identifier(table))
        execute_values(cursor, qry, delete_tuples, page_size = 10000)
    
    print(f"deleted {len(delete_tuples)} rows in {table}")

def insert_rows(table, x):
    keys_to_extract = ["issn_l", "fresh_oa_status", "year_int"]
    for dct in x:
        dct['updated'] = datetime.utcnow()
    insert_tuples = [tuple(w.values()) for w in x]
    with get_db_cursor() as cursor:
        qry = sql.SQL("insert into {} values %s").format(sql.Identifier(table))
        execute_values(cursor, qry, insert_tuples, page_size = 10000)
    
    print(f"deleted {len(delete_tuples)} rows in {table}")

def year_count(rws, year):
    return list(filter(lambda x: x['year_int'] == year, rws))[0]['count']

def mean_of_two_years(year_1, year_2):
    val_mean = mean([year_1, year_2])
    return round(val_mean)

def correct_2020(lst):
    """
    returns only the data thats changed. if none changed, an empty 
    """
    colors = list(set([w['fresh_oa_status'] for w in lst]))
    # color = colors[1]
    fixed_data = []
    for color in colors:
        subset_2020 = []
        subset = list(filter(lambda x: x['fresh_oa_status'] == color, lst))
        years = set([w['year_int'] for w in subset if w['year_int'] in range(2019, 2022)])
        if 2020 not in years:
            fixed_data.append(subset_2020)
            continue
        
        subset_2020 = list(filter(lambda x: x['year_int'] == 2020, subset))[0]
        changed = False
        
        if set([2019, 2020, 2021]) == years:
            subset_2020['count'] = mean_of_two_years(year_count(subset, 2019), year_count(subset, 2021))
            changed = True
        elif set([2019, 2020]) == years:
            subset_2020['count'] = year_count(subset, 2019)
            changed = True
        elif set([2020, 2021]) == years:
            subset_2020['count'] = year_count(subset, 2021)
            changed = True
        elif set([2020]) == years:
            subset_2020['count'] = None
            changed = True

        if changed:
            fixed_data.append(subset_2020)

    fixed_dicts = [dict(w) for w in fixed_data if w]
    return fixed_dicts

def make_params(venue, oa_status, submitted):
    parts = [
        f"host_venue.id:{venue}", 
        f"oa_status:{oa_status}",]
    if submitted == "false":
        parts.append("has_oa_submitted_version:false")
    return ",".join(parts)

tables_with_bronze = {
    "jump_oa_with_submitted_with_bronze":
        """
        insert into jump_oa_with_submitted_with_bronze (updated, venue_id, issn_l, fresh_oa_status, year_int, count) (
            select sysdate,venue_id,issn_l,fresh_oa_status,year_int,count from jump_oa_all_vars_new
            where with_submitted
        )
        """,
    "jump_oa_no_submitted_with_bronze":
        """
        insert into jump_oa_no_submitted_with_bronze (updated, venue_id, issn_l, fresh_oa_status, year_int, count) (
            select sysdate,venue_id,issn_l,fresh_oa_status,year_int,count from jump_oa_all_vars_new
            where not with_submitted
        )
        """,
}

tables_no_bronze = {
    "jump_oa_with_submitted_no_bronze":
        """
        insert into jump_oa_with_submitted_no_bronze (select * from jump_oa_with_submitted_with_bronze where fresh_oa_status != 'bronze')
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
        for table in tables_with_bronze.keys():
            truncate_table(table)
        for table in tables_no_bronze.keys():
            truncate_table(table)

        # from app import get_db_cursor
        # for table, qry in tables.items():
        #     update_table(table, qry)
        
        # make tables_with_bronze tables
        print("making with_bronze tables")
        from app import get_db_cursor
        for table, qry in tables_with_bronze.items():
            update_table(table, qry)

        # correct year 2020 data in tables_with_bronze tables
        print("correcting 2020 year data in with_bronze tables")
        for table, qry in tables_with_bronze.items():
            print(f"table: {table}")
            data = fetch_data_and_split(table)
            to_update = []
            for issn, lst in data.items():
                out = correct_2020(lst)
                if out:
                    to_update.extend(out)

            # update all 'updated' values
            for dct in to_update:
                dct['updated'] = datetime.utcnow()

            # delete all 2020 rows
            with get_db_cursor() as cursor:
                qry = sql.SQL("delete from {} where year_int = 2020").format(sql.Identifier(table))
                cursor.execute(qry)

            # redshift copy
            fields = list(to_update[0].keys())
            csv_filename = tempfile.mkstemp()[1]
            num_rows = 0
            with open(csv_filename, "w", encoding="utf-8") as file:
                writer = csv.DictWriter(file, delimiter=",", fieldnames=fields)
                for row in to_update:
                    num_rows += 1
                    writer.writerow(row)

            bucket_name = "jump-redshift-staging"
            object_name = "{}_{}_{}".format(table, "inserts", shortuuid.uuid())
            s3_client.upload_file(csv_filename, bucket_name, object_name)
            s3_object = "s3://{}/{}".format(bucket_name, object_name)

            copy_cmd = text("""
                copy {table} ({fields}) from '{s3_object}'
                credentials :creds format as csv
                timeformat 'auto';
            """.format(
                table=table,
                fields=", ".join(fields),
                s3_object=s3_object,
            ))

            aws_creds = "aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}".format(
                aws_key=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret=os.getenv("AWS_SECRET_ACCESS_KEY")
            )
            print((copy_cmd.bindparams(creds=aws_creds)))
            safe_commit(db)
            db.session.execute(copy_cmd.bindparams(creds=aws_creds))
            safe_commit(db)
        
        # make tables_no_bronze tables
        print("making no_bronze tables")
        for table, qry in tables_no_bronze.items():
            update_table(table, qry)
