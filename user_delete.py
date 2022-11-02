# coding: utf-8
import click

@click.command()
@click.option("--email", help="An Unsub user email", type=str, multiple=True)
@click.option("--id", help="An Unsub user ID", type=str, multiple=True)
@click.option("--inst", help="An Unsub institution ID", type=str)
@click.option("--perm_only", help="Only delete entries from permissions table? (default False)",
	default=False, is_flag=True)
def user_delete(email=None, id=None, inst=None, perm_only=False):
	"""
	Delete user from Unsub: deletes user from tables jump_user and jump_user_institution_permission

	Note: `heroku local:run` may be required to make sure environment variables are loaded

	Examples:

		python user_delete.py --help

		heroku local:run python user_delete.py --email foo@bar.org

		heroku local:run python user_delete.py --id user-x8g019bx7s9

		heroku local:run python user_delete.py --email foo@bar.org --inst institution-a79a8 --perm_only
	"""
	import textwrap
	from views import lookup_user, user_delete_one

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
