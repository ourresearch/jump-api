# coding: utf-8
"""Per-institution citation and authorship counts, keyed on OpenAlex institution IDs (oxjob #1717).

jump_inst_authorship / jump_inst_citing hold one row per (Unsub institution, issn_l, year):
distinct works (authorships) and distinct cited works (citations) for the OpenAlex institutions
the institution's RORs resolve to, plus their lineage descendants unless
jump_institution_lineage_policy turns that off. Counting distinct works per Unsub institution,
instead of summing per-key rows like the GRID tables did, means a paper counts once even when it
has authors at several of the institution's RORs, at a parent and its child, or with several
affiliation strings.

Sources, same shape whether built from the old extract or loaded from Walden
(see openalex_institution_metrics.sql):
    oa_work_institution (doi, openalex_id, is_direct)   lineage-expanded
    oa_work             (doi, issn_l, year)
    oa_work_refs        (citing_doi, cited_doi)

    python institution_metrics.py                  # institutions whose RORs or lineage policy changed
    python institution_metrics.py --all            # full rebuild, swapped in at the end
    python institution_metrics.py --institution institution-xxxx [--institution ...]
"""
import argparse
import datetime
import os
import urllib.parse
from contextlib import contextmanager
from time import time

import psycopg2

from util import elapsed

# the database app.py uses; not imported from app, which loads the common package data on import
DATABASE_URL = os.getenv("DATABASE_URL_REDSHIFT_TEST") if os.getenv("TESTING_DB") else os.getenv("DATABASE_URL_REDSHIFT")

# scenario.py reads the last 5 complete years (journal.py historical_years_by_year); keep one more
YEARS_KEPT = 6

# above this many changed institutions a full rebuild is cheaper than refreshing them as a set
MAX_INCREMENTAL = 50

SCOPE = """
    select distinct p.institution_id
    from jump_account_package p
    join jump_institution i on i.id = p.institution_id
    where not coalesce(p.is_deleted, false)
    and not coalesce(i.is_demo_institution, false)
"""

# what the counts depend on: the OpenAlex IDs an institution's RORs resolve to, and its lineage policy
SIGNATURES = """
    select m.institution_id,
        md5(listagg(distinct m.openalex_id::varchar, ',') within group (order by m.openalex_id::varchar)
            || '|' || max(case when p.include_descendants = false then 'self' else 'lineage' end)) as signature
    from jump_institution_openalex_v1 m
    left join jump_institution_lineage_policy p on p.institution_id = m.institution_id
    where m.institution_id in ({scope})
    group by 1
"""

ACCOUNT_WORKS = """
    create temp table tmp_account_works distkey(doi) as
    select distinct m.institution_id, w.doi, w.issn_l, w.year
    from jump_institution_openalex_v1 m
    left join jump_institution_lineage_policy p on p.institution_id = m.institution_id
    join oa_work_institution wi on wi.openalex_id = m.openalex_id
        and (coalesce(p.include_descendants, true) or wi.is_direct)
    join oa_work w on w.doi = wi.doi
    where w.year >= {min_year}
    and m.institution_id in ({scope})
"""

AUTHORSHIP = """
    select institution_id, issn_l, year, count(distinct doi)::int as num_authorships
    from tmp_account_works
    group by 1, 2, 3
"""

CITING = """
    select a.institution_id, cited.issn_l, a.year, count(distinct r.cited_doi)::int as num_citations
    from tmp_account_works a
    join oa_work_refs r on r.citing_doi = a.doi
    join oa_work cited on cited.doi = r.cited_doi
    group by 1, 2, 3
"""

TABLES = ["jump_inst_authorship", "jump_inst_citing", "jump_inst_metrics_built"]


@contextmanager
def transaction():
    # a connection of our own, not the app pool (autocommit, and get_db_cursor swallows errors):
    # each call is one transaction that commits at the end or rolls back on any error
    redshift_url = urllib.parse.urlparse(DATABASE_URL)
    connection = psycopg2.connect(database=redshift_url.path[1:], user=redshift_url.username,
                                  password=redshift_url.password, host=redshift_url.hostname,
                                  port=redshift_url.port, connect_timeout=30)
    try:
        with connection:
            with connection.cursor() as cursor:
                yield cursor
    finally:
        connection.close()


def log(message, start=None):
    suffix = " ({}s)".format(elapsed(start)) if start else ""
    print("institution_metrics: {}{}".format(message, suffix), flush=True)


def min_year():
    return datetime.datetime.utcnow().year - YEARS_KEPT


def table_exists(cursor, table):
    cursor.execute("select 1 from pg_tables where schemaname = 'public' and tablename = %s", (table,))
    return cursor.fetchone() is not None


def rebuild_all():
    start = time()
    with transaction() as cursor:
        cursor.execute(ACCOUNT_WORKS.format(min_year=min_year(), scope=SCOPE))
        log("collected works for every institution", start)
        for table, select in [("jump_inst_authorship", AUTHORSHIP), ("jump_inst_citing", CITING)]:
            cursor.execute("drop table if exists {}_new".format(table))
            cursor.execute("create table {}_new distkey(institution_id) sortkey(institution_id, issn_l) as {}".format(table, select))
            log("built {}_new".format(table), start)
        cursor.execute("drop table if exists jump_inst_metrics_built_new")
        cursor.execute("create table jump_inst_metrics_built_new as select s.*, sysdate as built from ({}) s".format(
            SIGNATURES.format(scope=SCOPE)))
        # swapped in the same transaction, so readers see either all old tables or all new ones
        for table in TABLES:
            cursor.execute("drop table if exists {}_old".format(table))
            if table_exists(cursor, table):
                cursor.execute("alter table {table} rename to {table}_old".format(table=table))
            cursor.execute("alter table {table}_new rename to {table}".format(table=table))
    with transaction() as cursor:
        for table in TABLES:
            cursor.execute("drop table if exists {}_old".format(table))
    log("rebuilt every institution", start)


def refresh(institution_ids):
    if not institution_ids:
        return
    start = time()
    # institutions outside SCOPE (no live package, or demo) end up with no rows
    scope = "select institution_id from ({}) s where institution_id in ({})".format(SCOPE, ", ".join(["%s"] * len(institution_ids)))
    params = tuple(institution_ids)
    with transaction() as cursor:
        cursor.execute(ACCOUNT_WORKS.format(min_year=min_year(), scope=scope), params)
        for table, select in [("jump_inst_authorship", AUTHORSHIP), ("jump_inst_citing", CITING)]:
            cursor.execute("delete from {} where institution_id in %s".format(table), (params,))
            cursor.execute("insert into {} {}".format(table, select))
        cursor.execute("delete from jump_inst_metrics_built where institution_id in %s", (params,))
        cursor.execute("insert into jump_inst_metrics_built select s.*, sysdate from ({}) s".format(
            SIGNATURES.format(scope=scope)), params)
    log("refreshed {} institutions".format(len(institution_ids)), start)


def changed_institutions():
    # new or changed RORs or lineage policy, institutions that lost their RORs or left SCOPE,
    # and institutions whose ROR only now resolves in openalex_ids
    with transaction() as cursor:
        cursor.execute("""
            select coalesce(current_.institution_id, built.institution_id)
            from ({signatures}) current_
            full outer join jump_inst_metrics_built built on built.institution_id = current_.institution_id
            where coalesce(built.signature, '') <> coalesce(current_.signature, '')
        """.format(signatures=SIGNATURES.format(scope=SCOPE)))
        return [row[0] for row in cursor.fetchall()]


def refresh_changed():
    with transaction() as cursor:
        built_before = table_exists(cursor, "jump_inst_metrics_built")
    if not built_before:
        rebuild_all()
        return
    institution_ids = changed_institutions()
    log("{} institutions changed".format(len(institution_ids)))
    if len(institution_ids) > MAX_INCREMENTAL:
        rebuild_all()
    else:
        refresh(institution_ids)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rebuild per-institution citation and authorship counts (oxjob #1717)")
    parser.add_argument("--all", action="store_true", help="rebuild every institution")
    parser.add_argument("--institution", action="append", help="refresh this institution (repeatable)")
    args = parser.parse_args()

    if args.all:
        rebuild_all()
    elif args.institution:
        refresh(args.institution)
    else:
        refresh_changed()
