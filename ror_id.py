import datetime
import re

import requests
from sqlalchemy import ForeignKey

from app import db
from app import get_db_cursor
from app import OPENALEX_API_KEY

ROR_PATTERN = re.compile(r"^0[a-z0-9]{6}[0-9]{2}$")


class RorId(db.Model):
    __tablename__ = 'jump_ror_id'
    institution_id = db.Column(db.Text, ForeignKey('jump_institution.id'), primary_key=True)
    ror_id = db.Column(db.Text, primary_key=True)

    def __repr__(self):
        return "<{} {}, {}>".format(self.__class__.__name__, self.institution_id, self.ror_id)


class RorGridCrosswalk(db.Model):
    __tablename__ = 'ror'
    ror_id = db.Column(db.Text, primary_key=True)
    grid_id = db.Column(db.Text, primary_key=True)

    def __repr__(self):
        return "<{} {} -> {}>".format(self.__class__.__name__, self.ror_id, self.grid_id)


def normalize_ror(ror_id):
    return re.sub(r"^https?://ror\.org/", "", (ror_id or "").strip()).lower()


def openalex_id_for_ror(ror_id):
    """OpenAlex institution ID (int) for a bare ROR, or None if OpenAlex has no such institution.

    Checks the weekly copy in openalex_ids first. A ROR newer than that copy is looked up in the
    API and added to openalex_ids and openalex_institution_lineage, so institution_metrics.py can
    build its counts without waiting for the next sync (oxjob #1717).
    """
    if not ROR_PATTERN.match(ror_id or ""):
        return None
    with get_db_cursor() as cursor:
        cursor.execute("select institution_id from openalex_ids where ror_id = %s limit 1", (ror_id,))
        row = cursor.fetchone()
    if row:
        return int(row[0])

    response = requests.get("https://api.openalex.org/institutions/ror:{}".format(ror_id),
                            params={"select": "id,display_name,ids,geo,lineage", "api_key": OPENALEX_API_KEY},
                            timeout=20)
    if response.status_code != 200:
        return None
    record = response.json()
    openalex_id = int(record["id"].rsplit("/", 1)[-1].lstrip("I"))
    geo = record.get("geo") or {}
    ancestors = {openalex_id} | {int(a.rsplit("/", 1)[-1].lstrip("I")) for a in record.get("lineage") or []}
    with get_db_cursor() as cursor:
        cursor.execute("""insert into openalex_ids (updated, name, institution_id, ror_id, mag_id, grid_id, country, country_code)
            values (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (datetime.datetime.utcnow(), record["display_name"], str(openalex_id), ror_id,
             record["ids"].get("mag"), record["ids"].get("grid"), geo.get("country"), geo.get("country_code")))
        cursor.execute("delete from openalex_institution_lineage where institution_id = %s", (openalex_id,))
        for ancestor_id in sorted(ancestors):
            cursor.execute("insert into openalex_institution_lineage (institution_id, ancestor_id) values (%s, %s)",
                           (openalex_id, ancestor_id))
    return openalex_id
