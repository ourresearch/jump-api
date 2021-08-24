# coding: utf-8

import argparse
import logging
from datetime import datetime
import re

import shortuuid
import glob
from sqlalchemy.orm.exc import MultipleResultsFound
from werkzeug.security import generate_password_hash

from app import db
from app import logger
from app import get_db_cursor
from grid_id import GridId
from institution import Institution
from package import Package
from permission import Permission
from permission import UserInstitutionPermission
from perpetual_access import PerpetualAccessInput
from ror_id import RorId, RorGridCrosswalk
from saved_scenario import SavedScenario
from user import User

from util import get_sql_answer
from util import read_csv_file

# heroku local:run python init_institution.py

#  python init_institution.py --institutions  (with --commit to commit it)
#  python init_institution.py --users  (with --commit to commit it)
#  python init_institution.py --users --institutions --commit

# configuration here

institution_name = None
institution_rows = []

# institution_name = "University of Pittsburgh"
#
# institution_rows = [{
#     "institution_name": institution_name,
#     "username": "pitt",
#     "ror_id_list": ["01an3r305"]
#     }]


user_rows = [
    {
        "email": None,
        "name": None,
        "jisc_id": "sg",
        "email_and_name": "Admin <vallison@sgul.ac.uk>",
        "password": "",
        "institution_name": institution_name,
        "permissions": ["view", "modify", "admin"]  # default is view, modify, admin
    }
    # ,{
    #     "email": None,
    #     "name": None,
    #     "email_and_name": "Admin <team+abc@ourresearch.org>",
    #     "password": "",
    #     "institution_name": institution_name,
    #     "permissions": ["view", "modify", "admin", ]  # default is view, modify, admin
    # },
]



# files can be xls, xlsx, or csv
files = {
    "prices": None,             # "/path/to/journal-prices.xlsx",
    "perpetual_access": None,   # "/path/to/perpetual-access.xls",
    "counter": None,            # "/path/to/counter.csv",
}

# configuration above


def add_institution(institution_name, old_username, ror_id_list, is_consortium=False):
    logger.info("initializing institution {}".format(institution_name))


    my_institutions = db.session.query(Institution).filter(Institution.display_name == institution_name,
                                                           Institution.id.notlike('%jisc%')).all()

    if my_institutions:
        my_institution = my_institutions[0]
        logger.info("  *** using existing institution {} ***".format(my_institution))

    if is_consortium:
        print("SETTING UP AS A CONSORTIUM ACCOUNT")

    else:
        my_institution = Institution()
        my_institution.display_name = institution_name
        my_institution.old_username = old_username
        my_institution.is_demo_institution = False
        my_institution.is_consortium = is_consortium
        db.session.add(my_institution)
        logger.info("  adding {}".format(my_institution))

    if not ror_id_list:
        return

    for ror_id in ror_id_list:
        add_ror(ror_id, my_institution.id)


def add_ror(ror_id, institution_id):
    logger.info("adding ROR IDs, if needed")

    if not db.session.query(RorId).filter(RorId.institution_id == institution_id, RorId.ror_id==ror_id).all():
        db.session.add(RorId(institution_id=institution_id, ror_id=ror_id))
        logger.info("  adding ROR ID {} for {}".format(ror_id, institution_id))
    else:
        logger.info("  ROR ID already there")

    db.session.commit()

    # add grid ids
    logger.info("adding GRID IDs, if needed")
    logger.info("  looking up GRID IDs")
    grid_ids = [x.grid_id for x in RorGridCrosswalk.query.filter(RorGridCrosswalk.ror_id == ror_id).all()]

    if not grid_ids:
        raise ValueError("at least one ror id corresponding to a grid id is required)")

    for g_id in grid_ids:
        if not db.session.query(GridId).filter(GridId.institution_id == institution_id, GridId.grid_id==g_id).all():
            db.session.add(GridId(institution_id=institution_id, grid_id=g_id))
            logger.info("  adding GRID ID {} for {}".format(g_id, institution_id))
        else:
            logger.info("  GRID ID already there")

        db.session.commit()


        # jump_citing
        logger.info("  populating jump_citing for GRID ID {}".format(g_id))

        num_citing_rows = get_sql_answer(db, "select count(*) from jump_citing where grid_id = '{}'".format(g_id))
        num_citing_rows_view = get_sql_answer(db, "select count(*) from jump_citing_view where grid_id = '{}'".format(g_id))

        logger.info("num_citing_rows: {}, num_citing_rows_view {}".format(num_citing_rows, num_citing_rows_view))

        if num_citing_rows:
            logger.info("    {} jump_citing rows already exist for grid id '{}'".format(num_citing_rows, g_id))
        else:
            with get_db_cursor() as cursor:
                cursor.execute(
                    "delete from jump_citing where grid_id = '{}'".format(g_id)
                )
                cursor.execute(
                    "insert into jump_citing (select * from jump_citing_view where grid_id = '{}')".format(g_id)
                )
            logger.info("    created jump_citing rows for grid id {}".format(g_id))

        # jump_authorship

        logger.info("  populating jump_authorship for GRID ID  {}".format(g_id))

        num_authorship_rows = get_sql_answer(db, "select count(*) from jump_authorship where grid_id = '{}'".format(g_id))
        num_authorship_rows_view = get_sql_answer(db, "select count(*) from jump_authorship_view where grid_id = '{}'".format(g_id))

        logger.info("num_authorship_rows: {}, num_authorship_rows_view {}".format(num_authorship_rows, num_authorship_rows_view))

        if num_authorship_rows:
            logger.info("    {} jump_authorship rows already exist for grid id {}".format(num_authorship_rows, g_id))
        else:
            with get_db_cursor() as cursor:
                cursor.execute(
                    "delete from jump_authorship where grid_id = '{}'".format(g_id)
                )
                cursor.execute(
                    "insert into jump_authorship (select * from jump_authorship_view where grid_id = '{}')".format(g_id)
                )
            logger.info("    created jump_authorship rows for grid id {}".format(g_id))

        my_packages = Package.query.filter(Package.institution_id==institution_id)
        for my_package in my_packages:
            rows_inserted = my_package.update_apc_authorships()
            logger.info("    inserted apc rows for package {}".format(my_package))


def add_user(user_info):
    # create users and permissions

    email = user_info.get("email", None)
    user_name = user_info.get("name", None)
    if "email_and_name" in user_info:
        user_name, email = re.findall("(.*)<(.*)>", user_info["email_and_name"])[0]
    email = email.strip()
    user_name = user_name.strip()


    logger.info("\ninitializing user {}".format(email))

    # don't use a jisc institution in this script
    if user_info.get("jisc_id", None) != None:
        institution_id = "institution-jisc{}".format(user_info["jisc_id"])
        my_institution = Institution.query.get(institution_id)
        print(my_institution)
    else:
        my_institutions = db.session.query(Institution).filter(Institution.display_name == user_info["institution_name"],
                                                               Institution.id.notlike('%jisc%')).all()

        if my_institutions:
            my_institution = my_institutions[0]
            logger.info("  *** using existing institution {} ***".format(my_institution))
        else:
            logger.info("  *** FAILED: institution {} doesn't exist, exiting ***".format(user_info["institution_name"]))
            return

    my_user = db.session.query(User).filter(User.email.ilike(email)).scalar()

    if my_user:
        logger.info("  *** user {} already exists. updating display name but not modifying password. ***".format(my_user))

    else:
        my_user = User()
        my_user.email = email
        my_user.password_hash = generate_password_hash(user_info.get("password", ""))

    my_user.display_name = user_name
    db.session.merge(my_user)
    logger.info("  saving {}".format(my_user))

    logger.info("  updating permissions and linking to institution")

    permission_names = user_info.get("permissions", None)
    if not permission_names:
        permission_names = ["view", "modify", "admin"]

    existing_permissions = db.session.query(UserInstitutionPermission).filter(
        UserInstitutionPermission.user_id == my_user.id,
        UserInstitutionPermission.institution_id == my_institution.id
    ).all()

    for ep in existing_permissions:
        logger.info("  *** removing existing user permission {} ***".format(ep))
        db.session.delete(ep)

    for permission_name in permission_names:
        perm = Permission.get(permission_name)
        if not perm:
            raise ValueError("unknown permission {}".format(permission_name))
        user_perm = UserInstitutionPermission(
            user_id=my_user.id,
            institution_id=my_institution.id,
            permission_id=perm.id
        )
        db.session.add(user_perm)
        db.session.flush()
        logger.info("  adding {}".format(user_perm))


# python init_institution.py --institutions --users --commit


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", help="Commit changes.", action="store_true", default=False)
    parser.add_argument("--institutions", help="true if want to add institutions", action="store_true", default=False)
    parser.add_argument("--users", help="true if want to add users", action="store_true", default=False)
    parser.add_argument("--is_consortium", help="true if is a consortium", action="store_true", default=False)
    parsed_args = parser.parse_args()
    commit = parsed_args.commit

    # ror_id = "00xmkp704"
    # institution_id = "institution-dbnbdwsYditg"
    # add_ror(ror_id, institution_id)
    # print 1/0


    if parsed_args.institutions:

        for row in institution_rows:
            my_institution = add_institution(row["institution_name"], row["username"], row["ror_id_list"], parsed_args.is_consortium)

            if commit:
                logger.info("commit")
                db.session.commit()
            else:
                logger.info("rollback, run with --commit to commit")
                db.session.rollback()

    if parsed_args.users:

        for row in user_rows:
            my_user = add_user(row)

            if commit:
                logger.info("commit")
                db.session.commit()
            else:
                logger.info("rollback, run with --commit to commit")
                db.session.rollback()

    if commit:
        logger.info("commit")
        db.session.commit()
    else:
        logger.info("rollback, run with --commit to commit")
        db.session.rollback()


