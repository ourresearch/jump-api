import datetime

import requests

from app import db, OPENALEX_API_KEY


class OpenalexID(db.Model):
    __tablename__ = "openalex_ids"
    name = db.Column(db.Text)
    institution_id = db.Column(db.Integer, primary_key=True)
    ror_id = db.Column(db.Text)
    mag_id = db.Column(db.Text)
    grid_id = db.Column(db.Text)
    country = db.Column(db.Text)
    country_code = db.Column(db.Text)
    updated = db.Column(db.DateTime)


def create_or_update_institution_ids():
    count = 0
    cursor = "*"

    # initial run
    url = f"https://api.openalex.org/institutions?cursor={cursor}&per-page=200&api_key={OPENALEX_API_KEY}&select=id,display_name,ids,geo"
    response = requests.get(url)

    if response.status_code == 200:
        for record in response.json()["results"]:
            institution_id = int(record["id"].replace("https://openalex.org/I", ""))
            create_or_update_record_by_institution_id(institution_id, record)
            count += 1
            print(f"Processed {count} institution_id records")
    print("committing")
    db.session.commit()

    cursor = response.json()["meta"]["next_cursor"] if "next_cursor" in response.json()["meta"] else None

    # loop through all pages
    while cursor:
        url = f"https://api.openalex.org/institutions?cursor={cursor}&per-page=200&api_key={OPENALEX_API_KEY}&select=id,display_name,ids,geo"
        response = requests.get(url)
        if response.status_code == 200:
            for record in response.json()["results"]:
                institution_id = int(record["id"].replace("https://openalex.org/I", ""))
                create_or_update_record_by_institution_id(institution_id, record)
                count += 1
                print(f"Processed {count} institution_id records")

            cursor = response.json()["meta"]["next_cursor"] if "next_cursor" in response.json()["meta"] else None
        print("committing")
        db.session.commit()
    db.session.commit()


def create_or_update_record_by_institution_id(institution_id, new_data):
    existing_record = db.session.query(OpenalexID).filter_by(institution_id=institution_id).first()
    if not existing_record:
        print(f"Record with institution_id {institution_id} does not exist. Creating new record.")
        create_new_record(new_data, institution_id)
        return

    existing_record.name = new_data["display_name"]
    existing_record.ror_id = new_data["ids"]["ror"].replace("https://ror.org/", "") if new_data["ids"].get("ror") else None
    existing_record.mag_id = new_data["ids"].get("mag")
    existing_record.grid_id = new_data["ids"].get("grid")
    existing_record.country = new_data["geo"].get("country")
    existing_record.country_code = new_data["geo"].get("country_code")
    existing_record.updated = datetime.datetime.now()


def create_new_record(new_data, institution_id):
    new_record = OpenalexID(
        name=new_data["display_name"],
        institution_id=institution_id,
        ror_id=new_data["ids"]["ror"].replace("https://ror.org/", "") if new_data["ids"].get("ror") else None,
        mag_id=new_data["ids"].get("mag"),
        grid_id=new_data["ids"].get("grid"),
        country=new_data["geo"].get("country"),
        country_code=new_data["geo"].get("country_code"),
        updated=datetime.datetime.now(),
    )
    db.session.add(new_record)


if __name__ == "__main__":
    create_or_update_institution_ids()