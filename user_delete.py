# coding: utf-8

import click
import textwrap
from views import lookup_user, user_delete_one

# Delete user from Unsub: deletes user from tables jump_user and jump_user_institution_permission
# Examples
# --------
# Notes:
#     - prefix `heroku local:run` required to make sure environment variables are loaded
# # Shows help
# python user_delete.py --help
# # Delete a user by email
# heroku local:run python user_delete.py --email foo@bar.org
# # Delete a user by user id
# heroku local:run python user_delete.py --id user-x8g019bx7s9
# # Delete a user by email and institution id - delete jump_user_institution_permission entry only (leave jump_user)
# heroku local:run python user_delete.py --email foo@bar.org --inst institution-a79a8 --perm_only
            
@click.command()
@click.option("--email", help="An Unsub user email", type=str, multiple=True)
@click.option("--id", help="An Unsub user ID", type=str, multiple=True)
@click.option("--inst", help="An Unsub institution ID", type=str)
@click.option("--perm_only", help="Only delete entries from permissions table? (default False)",
    default=False, is_flag=True)
def user_delete(email=None, id=None, inst=None, perm_only=False):
    if email and id:
        click.echo("supply only email or id")
        raise click.Abort()
    
    if email:
        for mail in email:
            user_delete_one(email=mail, id=None, inst=inst, perm_only=perm_only)
    if id:
        for x in id:
            user_delete_one(email=None, id=x, inst=inst, perm_only=perm_only)

if __name__ == "__main__":
    user_delete()

# # deleting users within a Pyton REPL
# # first get csv of user info from unsub-scripts/lookup_by.py script
# from user_delete import user_delete
# import pandas as pd
# df = pd.read_csv("final.csv")
# for index, row in df.iterrows():
#     print("deleting permissions for user '{}' at institution '{}'".format(row['user_id'], row["institution_display_name"]))
#     user_delete(id = row['user_id'], inst = row['institution_id'], perm_only = True)
