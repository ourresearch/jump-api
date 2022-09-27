import os
import argparse
from collections import defaultdict
import json
import gzip

from app import get_db_cursor
from app import s3_client

def get_embargo_data_from_db():
    command = "select issn_l, embargo from journal_delayed_oa_active"
    embargo_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        embargo_rows = cursor.fetchall()
    embargo_dict = dict((a["issn_l"], round(a["embargo"])) for a in embargo_rows)
    return embargo_dict

def get_unpaywall_downloads_from_db():
    command = """
        select issn_l,num_papers_2021,downloads_total,downloads_0y,downloads_1y,downloads_2y,downloads_3y,downloads_4y
        from jump_unpaywall_downloads_new
        where issn_l in (select distinct issn_l from jump_counter)
    """
    big_view_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        big_view_rows = cursor.fetchall()
    unpaywall_downloads_dict = dict((row["issn_l"], dict(row)) for row in big_view_rows)
    return unpaywall_downloads_dict

def get_num_papers_from_db():
    command = "select issn_l, year, num_papers from jump_num_papers_oa where year >= 2014"
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    lookup_dict = defaultdict(dict)
    for row in rows:
        lookup_dict[row["issn_l"]][row["year"]] = row["num_papers"]
    return lookup_dict

def get_oa_data_from_db():
    oa_dict = {}
    for submitted in ["with_submitted", "no_submitted"]:
        for bronze in ["with_bronze", "no_bronze"]:
            key = "{}_{}".format(submitted, bronze)

            command = """select * from jump_oa_{}  
                        where year_int >= 2015  
                            """.format(key)

            with get_db_cursor() as cursor:
                cursor.execute(command)
                rows = cursor.fetchall()
            for x in rows:
                x['year_int'] = int(x['year_int'])
            lookup_dict = defaultdict(list)
            for row in rows:
                lookup_dict[row["issn_l"]] += [dict(row)]
            oa_dict[key] = lookup_dict
    return oa_dict

def get_society_data_from_db():
    command = "select issn_l, is_society_journal from jump_society_journals_input where is_society_journal is not null"
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    lookup_dict = defaultdict(list)
    for row in rows:
        lookup_dict[row["issn_l"]] = row["is_society_journal"]
    return lookup_dict

def get_social_networks_data_from_db():
    command = """select issn_l, asn_only_rate::float from jump_mturk_asn_rates
                    """
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    lookup_dict = {}
    for row in rows:
        lookup_dict[row["issn_l"]] = row["asn_only_rate"]
    return lookup_dict

def write_json(title, data):
    print("dumping")
    with open(title, 'w') as f:
        json.dump(data, f)
    print("done dumping")

def gather_common_data():
    my_data = {}
    my_data["embargo_dict"] = get_embargo_data_from_db()
    my_data["unpaywall_downloads_dict_raw"] = get_unpaywall_downloads_from_db()
    my_data["social_networks"] = get_social_networks_data_from_db()
    my_data["oa"] = get_oa_data_from_db()
    my_data["society"] = get_society_data_from_db()
    my_data["num_papers"] = get_num_papers_from_db()
    return my_data

def upload_common_data():
    print("gathering data from database")
    data = gather_common_data()

    try:
        os.remove('data/common_package_data_for_all_newjumpunpaywall.json.gz')
    except OSError:
        pass

    with gzip.open('data/common_package_data_for_all_newjumpunpaywall.json.gz', 'w') as f:
        f.write(json.dumps(data, default=str).encode('utf-8'))

    print("uploading to S3")
    s3_client.upload_file(
        Filename="data/common_package_data_for_all_newjumpunpaywall.json.gz", 
        Bucket="unsub-cache", 
        Key="common_package_data_for_all_newjumpunpaywall.json.gz")

    print("done!")

# heroku local:run python common_data.py --run
# heroku run --size=performance-l python common_data.py --run -r heroku
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", help="Prepare common data and upload to S3", action="store_true", default=False)
    parsed_args = parser.parse_args()

    if parsed_args.run:
        upload_common_data()
