# coding: utf-8

import argparse
import textwrap

from app import app
from app import db
from app import logger
from app import get_db_cursor
from views import lookup_user

def user_delete(email=None, id=None, inst=None, perm_only=False):
    if email:
        email = email.strip()
    if id:
        id = id.strip()

    user = lookup_user(email=email, user_id=id)
    
    if not user:
        logger.info("  *** user {} does not exist, exiting ***".format(email or id))
        return

    logger.info("  deleting user permissions from `jump_user_institution_permission` table")
    if inst:
        query = "delete from jump_user_institution_permission where user_id = '{}' and institution_id = '{}';".format(user.id, inst)
    else:    
        query = "delete from jump_user_institution_permission where user_id = '{}';".format(user.id)
    with app.app_context():
        with get_db_cursor() as cursor:
            cursor.execute(query)
    
    if not perm_only:
        logger.info("  deleting user from `jump_user` table")
        query = "delete from jump_user where id = '{}';".format(user.id)
        with app.app_context():
            with get_db_cursor() as cursor:
                cursor.execute(query)

    logger.info("  commit")
    db.session.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
            Delete user from Unsub: deletes user from tables jump_user and jump_user_institution_permission

            Examples
            --------

            Notes:
                - prefix `heroku local:run` required to make sure environment variables are loaded

            # Show this help
            heroku local:run python user_delete.py -h

            # Delete a user by email
            heroku local:run python user_delete.py --email foo@bar.org

            # Delete a user by user id
            heroku local:run python user_delete.py --id user-x8g019bx7s9

            # Delete a user by email and institution id - delete jump_user_institution_permission entry only (leave jump_user)
            heroku local:run python user_delete.py --email foo@bar.org --inst institution-a79a8 --perm_only
            '''))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--email", help="An Unsub user email", type=str)
    group.add_argument("--id", help="An Unsub user ID", type=str)
    parser.add_argument("--inst", help="An Unsub institution ID", type=str)
    parser.add_argument("--perm_only", help="Only delete entries from permissions table? (default False)", 
        action="store_true", default=False)
    args = parser.parse_args()

    user_delete(email = args.email, id = args.id, inst = args.inst, perm_only = args.perm_only)

# # deleting users within a Pyton REPL
# # first get csv of user info from unsub-scripts/lookup_by.py script
# from user_delete import user_delete
# import pandas as pd
# df = pd.read_csv("final.csv")
# for index, row in df.iterrows():
#     print("deleting permissions for user '{}' at institution '{}'".format(row['user_id'], row["institution_display_name"]))
#     user_delete(id = row['user_id'], inst = row['institution_id'], perm_only = True)
