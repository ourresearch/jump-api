# coding: utf-8
"""Sync every OpenAlex institution into Redshift (oxjob #1717):
    openalex_ids                  one row per institution: OpenAlex ID, ROR, GRID, name, country
    openalex_institution_lineage  (institution_id, ancestor_id) for the institution itself and each
                                  ancestor in its OpenAlex lineage
Unsub maps an institution's RORs to OpenAlex IDs through openalex_ids, and picks up child
institutions through the lineage table (see institution_metrics.py).

Loads full copies into *_new tables and swaps them in one transaction. Run weekly:
    python openalex_institution_ids_to_unsub.py
"""
import datetime
import os
from time import sleep, time

import requests
from psycopg2.extras import execute_values

from institution_metrics import table_exists, transaction
from util import elapsed

API_URL = "https://api.openalex.org/institutions"
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY")


def openalex_number(openalex_url):
    return int(openalex_url.rsplit("/", 1)[-1].lstrip("I"))


def get_page(cursor):
    params = {"cursor": cursor, "per-page": 200, "select": "id,display_name,ids,geo,lineage", "api_key": OPENALEX_API_KEY}
    for attempt in range(6):
        response = requests.get(API_URL, params=params, timeout=60)
        if response.status_code == 200:
            return response.json()
        sleep(2 ** attempt)
    response.raise_for_status()


def fetch_institutions():
    institutions = []
    cursor = "*"
    while cursor:
        page = get_page(cursor)
        institutions += page["results"]
        cursor = page["meta"].get("next_cursor")
        if len(institutions) % 10000 < 200:
            print("fetched {} institutions".format(len(institutions)), flush=True)
    return institutions


def sync_institutions():
    start = time()
    institutions = fetch_institutions()
    now = datetime.datetime.utcnow()

    id_rows = []
    lineage_rows = set()
    for record in institutions:
        institution_id = openalex_number(record["id"])
        ror = record["ids"].get("ror")
        geo = record.get("geo") or {}
        id_rows.append((now, record["display_name"], str(institution_id),
                        ror.replace("https://ror.org/", "") if ror else None,
                        record["ids"].get("mag"), record["ids"].get("grid"),
                        geo.get("country"), geo.get("country_code")))
        lineage_rows.add((institution_id, institution_id))
        for ancestor in record.get("lineage") or []:
            lineage_rows.add((institution_id, openalex_number(ancestor)))
    print("{} institutions, {} lineage rows ({}s)".format(len(id_rows), len(lineage_rows), elapsed(start)), flush=True)

    # a short or failed crawl must not replace a good copy
    if len(id_rows) < 100000:
        raise ValueError("only {} institutions fetched; keeping the current tables".format(len(id_rows)))

    with transaction() as cursor:
        cursor.execute("drop table if exists openalex_ids_new")
        cursor.execute("create table openalex_ids_new (like openalex_ids)")
        execute_values(cursor, """insert into openalex_ids_new
            (updated, name, institution_id, ror_id, mag_id, grid_id, country, country_code) values %s""",
            id_rows, page_size=1000)
        cursor.execute("drop table if exists openalex_institution_lineage_new")
        cursor.execute("""create table openalex_institution_lineage_new (
            institution_id bigint not null, ancestor_id bigint not null) diststyle all sortkey(ancestor_id)""")
        execute_values(cursor, "insert into openalex_institution_lineage_new (institution_id, ancestor_id) values %s",
            sorted(lineage_rows), page_size=1000)
        for table in ["openalex_ids", "openalex_institution_lineage"]:
            cursor.execute("drop table if exists {}_old".format(table))
            if table_exists(cursor, table):
                cursor.execute("alter table {table} rename to {table}_old".format(table=table))
            cursor.execute("alter table {table}_new rename to {table}".format(table=table))
    with transaction() as cursor:
        for table in ["openalex_ids", "openalex_institution_lineage"]:
            cursor.execute("drop table if exists {}_old".format(table))
    print("synced OpenAlex institutions ({}s)".format(elapsed(start)), flush=True)


if __name__ == "__main__":
    sync_institutions()
