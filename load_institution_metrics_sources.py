# coding: utf-8
"""Load Walden's monthly export of the institution-metrics sources into Redshift (oxjob #1717, Phase 2).

Walden's Unsub_Institution_Metrics_Export job writes Parquet to
    s3://openalex-sandbox/unsub/institution_metrics/<run_date>/{oa_work,oa_work_institution,oa_work_refs}/
plus a _MANIFEST.json with row counts, then the run date to .../LATEST. When LATEST is newer than the last
run loaded here, each dataset is COPYed into <name>_staging, checked, and all three are swapped in for the
current oa_work* tables (or, the first time, the Phase 1 views over the old extract) in one transaction.
institution_metrics.py --all then rebuilds the counts from them.

    python load_institution_metrics_sources.py           # load LATEST if it hasn't been loaded
    python load_institution_metrics_sources.py --force   # load LATEST even if it has
"""
import argparse
import json
import os
import sys
from time import time

import boto3

from institution_metrics import table_exists, transaction
from util import elapsed

BUCKET = "openalex-sandbox"
PREFIX = "unsub/institution_metrics"

TABLES = {
    "oa_work": "(doi varchar(1024) not null, issn_l varchar(20), year int) distkey(doi)",
    "oa_work_institution": "(doi varchar(1024) not null, openalex_id bigint not null, is_direct boolean) distkey(doi) sortkey(openalex_id)",
    "oa_work_refs": "(citing_doi varchar(1024) not null, cited_doi varchar(1024) not null) distkey(citing_doi)",
}

# a first load (replacing the Phase 1 views) must at least reach these; later loads must reach 80% of the last one
FIRST_LOAD_MINIMUM = {"oa_work": 50000000, "oa_work_institution": 20000000, "oa_work_refs": 300000000}
SHRINK_LIMIT = 0.8


def log(message, start=None):
    suffix = " ({}s)".format(elapsed(start)) if start else ""
    print("load_institution_metrics_sources: {}{}".format(message, suffix), flush=True)


def s3_text(key):
    return boto3.client("s3").get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8").strip()


def relation_kind(cursor, name):
    cursor.execute("""select c.relkind from pg_class c join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public' and c.relname = %s""", (name,))
    row = cursor.fetchone()
    return row[0] if row else None


def last_load(cursor):
    cursor.execute("""create table if not exists jump_inst_sources_loaded (
        run_date varchar(10), counts varchar(1000), loaded timestamp default sysdate)""")
    cursor.execute("select run_date, counts from jump_inst_sources_loaded order by loaded desc limit 1")
    return cursor.fetchone()


def load(force=False):
    start = time()
    run_date = s3_text("{}/LATEST".format(PREFIX))
    manifest = json.loads(s3_text("{}/{}/_MANIFEST.json".format(PREFIX, run_date)))
    with transaction() as cursor:
        previous = last_load(cursor)
    if previous and previous[0] >= run_date and not force:
        log("run {} already loaded; nothing to do".format(run_date))
        return
    log("loading run {} ({})".format(run_date, manifest["counts"]))

    credentials = "aws_access_key_id={};aws_secret_access_key={}".format(
        os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
    previous_counts = json.loads(previous[1]) if previous else None
    with transaction() as cursor:
        for name, columns in TABLES.items():
            cursor.execute("drop table if exists {}_staging".format(name))
            cursor.execute("create table {}_staging {}".format(name, columns))
            cursor.execute("copy {}_staging from 's3://{}/{}/{}/{}/' credentials '{}' format as parquet".format(
                name, BUCKET, PREFIX, run_date, name, credentials))
            cursor.execute("select count(*) from {}_staging".format(name))
            loaded = cursor.fetchone()[0]
            expected = manifest["counts"][name]
            floor = previous_counts[name] * SHRINK_LIMIT if previous_counts else FIRST_LOAD_MINIMUM[name]
            log("{}: {:,} rows copied (manifest {:,}, floor {:,.0f})".format(name, loaded, expected, floor), start)
            if loaded != expected or loaded < floor:
                raise ValueError("{} failed its check; keeping the current sources".format(name))
        # swapped in the same transaction, so institution_metrics.py never sees a mix of runs
        for name in TABLES:
            kind = relation_kind(cursor, name)
            if kind == "v":
                cursor.execute("drop view {}".format(name))
            elif kind == "r":
                cursor.execute("drop table if exists {}_old".format(name))
                cursor.execute("alter table {name} rename to {name}_old".format(name=name))
            cursor.execute("alter table {name}_staging rename to {name}".format(name=name))
        cursor.execute("insert into jump_inst_sources_loaded (run_date, counts) values (%s, %s)",
                       (run_date, json.dumps(manifest["counts"])))
    with transaction() as cursor:
        for name in TABLES:
            if table_exists(cursor, "{}_old".format(name)):
                cursor.execute("drop table {}_old".format(name))
    log("loaded run {}; run institution_metrics.py --all to rebuild the counts".format(run_date), start)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Walden's institution-metrics sources into Redshift (oxjob #1717)")
    parser.add_argument("--force", action="store_true", help="load LATEST even if it was already loaded")
    args = parser.parse_args()
    try:
        load(force=args.force)
    except ValueError as e:
        log(str(e))
        sys.exit(1)
