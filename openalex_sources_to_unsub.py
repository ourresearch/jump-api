import datetime

import requests
from sqlalchemy import update

from app import db, OPENALEX_API_KEY


class OpenalexDBRaw(db.Model):
	__tablename__ = "openalex_journals"
	issn_l = db.Column(db.Text, primary_key=True)
	issn = db.Column(db.Text)
	display_name = db.Column(db.Text)
	is_oa = db.Column(db.Boolean)
	is_in_doaj = db.Column(db.Boolean)
	publisher = db.Column(db.Text)
	counts_by_year = db.Column(db.Text)
	x_concepts = db.Column(db.Text)
	updated_date = db.Column(db.DateTime)
	id = db.Column(db.Text)


def create_or_update_sources():
    count = 0
    date_start = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%dT00:00:00Z")
    date_end = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    cursor = "*"

    # initial run
    url = f"https://api.openalex.org/sources?filter=issn_l:!null,from_updated_date:{date_start},to_updated_date:{date_end}&api_key={OPENALEX_API_KEY}&cursor={cursor}&per-page=200"
    response = requests.get(url)

    if response.status_code == 200:
        for record in response.json()["results"]:
            create_or_update_record_by_issn_l(record["issn_l"], record)
            count += 1
            break

    cursor = response.json()["meta"]["next_cursor"] if "next_cursor" in response.json()["meta"] else None

    # loop through all pages
    while cursor:
        url = f"https://api.openalex.org/sources?filter=issn_l:!null,from_updated_date:{date_start},to_updated_date:{date_end}&api_key={OPENALEX_API_KEY}&cursor={cursor}&per-page=200"
        response = requests.get(url)
        if response.status_code == 200:
            for record in response.json()["results"]:
                create_or_update_record_by_issn_l(record["issn_l"], record)
                count += 1

            cursor = response.json()["meta"]["next_cursor"] if "next_cursor" in response.json()["meta"] else None
            print(f"Processed {count} records")
        db.session.commit()


def create_or_update_record_by_issn_l(issn_l, new_data):
    existing_record = db.session.query(OpenalexDBRaw).filter_by(issn_l=new_data["issn_l"]).first()
    if not existing_record:
        print(f"Record with issn_l {issn_l} does not exist. Creating new record.")
        create_new_record(new_data)
        return

    stmt = (
        update(OpenalexDBRaw)
        .where(OpenalexDBRaw.issn_l == issn_l)
        .values(
            id=new_data["id"],
            issn=str(new_data["issn"]),
            display_name=new_data["display_name"],
            is_oa=new_data["is_oa"],
            is_in_doaj=new_data["is_in_doaj"],
            publisher=new_data["host_organization_name"],
            counts_by_year=str(new_data["counts_by_year"]),
            x_concepts=str(new_data["x_concepts"]),
            updated_date=new_data["updated_date"]
        )
    )
    try:
        db.session.execute(stmt)
        db.session.commit()
        print(f"All records with issn_l {issn_l} have been updated.")
    except Exception as e:
        db.session.rollback()
        print(f"Error updating records with issn_l {issn_l}: {e}")


def create_new_record(new_data):
    new_record = OpenalexDBRaw(
        id=new_data["id"],
        issn_l=new_data["issn_l"],
        issn=str(new_data["issn"]),
        display_name=new_data["display_name"],
        is_oa=new_data["is_oa"],
        is_in_doaj=new_data["is_in_doaj"],
        publisher=new_data["host_organization_name"],
        counts_by_year=str(new_data["counts_by_year"]),
        x_concepts=str(new_data["x_concepts"]),
        updated_date=new_data["updated_date"]
    )
    try:
        db.session.add(new_record)
        db.session.commit()
        print(f"New record with issn_l {new_data['issn_l']} has been created.")
    except Exception as e:
        db.session.rollback()
        print(f"Error creating new record with issn_l {new_data['issn_l']}: {e}")


if __name__ == "__main__":
    create_or_update_sources()
