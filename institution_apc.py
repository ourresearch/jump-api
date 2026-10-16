# coding: utf-8
"""Fill jump_apc_institutional_authorships (the APC report) from the OpenAlex API (oxjobs #518, #1717).

Per gold or hybrid work since 2014 with an OpenAlex list price, for the OpenAlex institutions an Unsub
institution's RORs resolve to (jump_institution_openalex_v1), plus their lineage descendants unless
jump_institution_lineage_policy turns that off:
    apc                           apc_list.value_usd
    num_authors_from_uni          authors with at least one of those institutions
    is_corresponding_institution  one of those institutions is a corresponding institution
    estimated_apc_paid            apc if corresponding; 0 if another institution is; otherwise
                                  apc * num_authors_from_uni / num_authors_total
Replaces #518's one-off apc_loader.py, which used only each institution's first ROR, and the legacy
GRID view that apc_institutional_update.py and institutional_new_data.py filled the table from.

    python institution_apc.py --all                 # every non-consortium institution with a live package
    python institution_apc.py --institution institution-xxxx [--institution ...]
"""
import argparse
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from time import sleep, time

import requests
from psycopg2.extras import execute_values

from institution_metrics import SCOPE, transaction
from util import elapsed

OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY")
COLUMNS = ["institution_id", "doi", "num_authors_total", "num_authors_from_uni", "journal_name", "issn_l",
           "year", "oa_status", "apc", "publisher", "is_corresponding_institution", "estimated_apc_paid"]
THREADS = 8


def openalex_number(openalex_url):
    return int(openalex_url.rsplit("/", 1)[-1].lstrip("I"))


def get(params):
    params = dict(params, api_key=OPENALEX_API_KEY)
    for attempt in range(6):
        response = requests.get("https://api.openalex.org/works", params=params, timeout=120)
        if response.status_code == 200:
            return response.json()
        sleep(2 ** attempt)
    response.raise_for_status()


def account_ids(institution_ids):
    """{institution_id: (set of OpenAlex IDs, include_descendants)} for these Unsub institutions."""
    with transaction() as cursor:
        cursor.execute("""
            select m.institution_id, m.openalex_id, coalesce(p.include_descendants, true)
            from jump_institution_openalex_v1 m
            left join jump_institution_lineage_policy p on p.institution_id = m.institution_id
            where m.institution_id in %s""", (tuple(institution_ids),))
        accounts = {}
        for institution_id, openalex_id, include_descendants in cursor.fetchall():
            ids, _ = accounts.get(institution_id, (set(), include_descendants))
            accounts[institution_id] = (ids | {openalex_id}, include_descendants)
        return accounts


def rows_for(institution_id, openalex_ids, include_descendants):
    field = "authorships.institutions.lineage" if include_descendants else "authorships.institutions.id"
    params = {
        "filter": "{}:{},open_access.oa_status:gold|hybrid,from_publication_date:2014-01-01".format(
            field, "|".join("I{}".format(i) for i in sorted(openalex_ids))),
        "select": "doi,publication_year,authorships,corresponding_institution_ids,primary_location,open_access,apc_list",
        "per-page": 200,
    }

    def ours(institution):
        # with lineage, an author at a descendant counts; without, only the institutions themselves
        ids = institution.get("lineage") if include_descendants else [institution.get("id")]
        return any(openalex_number(i) in openalex_ids for i in ids or [] if i)

    rows = []
    cursor = "*"
    while cursor:
        page = get(dict(params, cursor=cursor))
        cursor = page["meta"].get("next_cursor")
        if not page["results"]:
            break
        for work in page["results"]:
            apc = (work.get("apc_list") or {}).get("value_usd")
            authorships = work.get("authorships") or []
            if apc is None or not authorships or not work.get("doi"):
                continue
            num_ours = sum(1 for a in authorships if any(ours(i) for i in a.get("institutions") or []))
            institutions_on_work = {i["id"]: i for a in authorships for i in a.get("institutions") or [] if i.get("id")}
            corresponding = work.get("corresponding_institution_ids") or []
            is_corresponding = any(ours(institutions_on_work.get(c) or {"id": c, "lineage": [c]}) for c in corresponding)
            share = 1.0 if is_corresponding else (0.0 if corresponding else num_ours / len(authorships))
            source = (work.get("primary_location") or {}).get("source") or {}
            rows.append((institution_id, work["doi"].replace("https://doi.org/", ""), len(authorships), num_ours,
                         source.get("display_name"), source.get("issn_l"), work.get("publication_year"),
                         (work.get("open_access") or {}).get("oa_status"), apc, source.get("host_organization_name"),
                         is_corresponding, round(apc * share, 2)))
    return rows


def refresh(institution_id, openalex_ids, include_descendants):
    rows = rows_for(institution_id, openalex_ids, include_descendants)
    with transaction() as cursor:
        cursor.execute("delete from jump_apc_institutional_authorships where institution_id = %s", (institution_id,))
        if rows:
            execute_values(cursor, "insert into jump_apc_institutional_authorships ({}) values %s".format(
                ", ".join(COLUMNS)), rows, page_size=1000)
    return len(rows)


def refresh_institutions(institution_ids):
    if not institution_ids:
        return
    start = time()
    accounts = account_ids(institution_ids)
    lock = threading.Lock()
    done = {"n": 0, "failed": 0}

    def work(institution_id):
        ids, include_descendants = accounts[institution_id]
        try:
            n = refresh(institution_id, ids, include_descendants)
            message = "{} rows".format(n)
        except Exception as e:
            message = "FAILED: {}".format(str(e)[:200])
            with lock:
                done["failed"] += 1
        with lock:
            done["n"] += 1
            print("institution_apc: [{}/{}] {} {}".format(done["n"], len(accounts), institution_id, message), flush=True)

    with ThreadPoolExecutor(THREADS) as executor:
        list(executor.map(work, sorted(accounts)))
    print("institution_apc: refreshed {} institutions, {} failed ({}s)".format(len(accounts), done["failed"], elapsed(start)), flush=True)


def all_institutions():
    with transaction() as cursor:
        cursor.execute("""select s.institution_id from ({}) s
            join jump_institution i on i.id = s.institution_id
            where not coalesce(i.is_consortium, false)""".format(SCOPE))
        return [row[0] for row in cursor.fetchall()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fill the APC report table from the OpenAlex API (oxjobs #518, #1717)")
    parser.add_argument("--all", action="store_true", help="every non-consortium institution with a live package")
    parser.add_argument("--institution", action="append", help="refresh this institution (repeatable)")
    args = parser.parse_args()
    refresh_institutions(all_institutions() if args.all else args.institution or [])
