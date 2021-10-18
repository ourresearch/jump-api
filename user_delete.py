# coding: utf-8

import argparse
import textwrap

from app import app
from app import db
from app import logger
from app import get_db_cursor
from views import lookup_user

def user_delete(email=None, id=None):
    if email:
        email = email.strip()
    if id:
        id = id.strip()

    user = lookup_user(email=email, user_id=id)
    
    if not user:
        logger.info("  *** user {} does not exist, exiting ***".format(email or id))
        return

    logger.info("  deleting user permissions from `jump_user_institution_permission` table")
    query = "delete from jump_user_institution_permission where user_id = '{}';".format(user.id)
    with app.app_context():
        with get_db_cursor() as cursor:
            cursor.execute(query)
    
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
            '''))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--email", help="An Unsub user email", type=str)
    group.add_argument("--id", help="An Unsub user ID", type=str)
    args = parser.parse_args()

    user_delete(email = args.email, id = args.id)
