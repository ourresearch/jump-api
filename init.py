# coding: utf-8
import click
from admin_actions import add_institution, add_ror, add_user

@click.group()
def cli():
	"""
	Create institutions, users and add ROR IDs.

	Note: Quotes are required around any flag inputs with spaces.

	Examples:

		python init.py --help

		python init.py inst --help

		python init.py inst --name='Forest College' --ror=05fs6jp91
		
		python init.py user --name='Scott Chamberlain' --email=myrmecocystus@gmail.com --institution='institution-Z9vU94XpmwKF'
		
		python init.py ror --ror=05fs6jp91 --institution=institution-Z9vU94XpmwKF
		python init.py ror --ror=005p9kw61 --ror=00hpz7z43 --institution=institution-Z9vU94XpmwKF
	"""

@cli.command(short_help='Create an institution Unsub account')
@click.option("--name", help="Name of the institution", type=str)
@click.option("--ror", help="One or more ROR IDs (can be passed multiple times)", 
	type=str, multiple=True, required=True)
def inst(name, ror):
	click.echo(f"Creating Unsub account for '{name}' w/ ROR ID(s) {ror}")
	add_institution(name, list(ror), cli = True)


@cli.command(short_help='create a user and associate them with an institution')
@click.option("--name", help="Full name (first last) of the person", type=str, default="Admin", required=True)
@click.option("--email", help="Email for the person", type=str, required=True)
@click.option("--institution", help="An institution ID", type=str)
@click.option("--permissions", help="Permissions", default="view,modify,admin", 
	show_default=True, type=str, required=True)
@click.option("--password", help="Password to associate with the user (default: no password set)", 
	default="", type=str, show_default=True)
@click.option("--jiscid", help="Jisc institution ID", type=str)
def user(name, email, institution, permissions, password, jiscid):
	click.echo(f"Adding user {name} ({email}) to {institution}")
	permissions = permissions.split(",")
	add_user(name, email, institution, permissions, password, jiscid, cli = True)


@cli.command(short_help='add a ROR ID to an institution/account')
@click.option("--ror", help="One or more ROR IDs (can be passed multiple times)", 
	type=str, multiple=True, required=True)
@click.option("--institution", help="An institution ID", type=str, required=True)
def ror(ror, institution):
	click.echo(f"Adding ROR ID(s) {ror} to {institution}")
	for x in ror:
		add_ror(x, institution, cli = True)

if __name__ == "__main__":
	cli()
