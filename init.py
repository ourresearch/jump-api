# coding: utf-8
import click
from datetime import datetime
import re
import shortuuid
from werkzeug.security import generate_password_hash

from app import db
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

def add_institution(institution_name, old_username, ror_id_list, is_consortium=False):
	click.echo("initializing institution {}".format(institution_name))

	my_institutions = db.session.query(Institution).filter(
		Institution.display_name == institution_name,
		Institution.id.notlike('%jisc%')).all()

	if my_institutions:
		my_institution = my_institutions[0]
		click.echo(f"  *** using existing institution {my_institution} ***")

	if is_consortium:
		click.echo("Setting up as a consortium account")
	else:
		my_institution = Institution()
		my_institution.display_name = institution_name
		my_institution.old_username = old_username
		my_institution.is_demo_institution = False
		my_institution.is_consortium = is_consortium
		db.session.add(my_institution)
		click.echo(f"  adding {my_institution}")

	if not ror_id_list:
		return

	for ror_id in ror_id_list:
		add_ror(ror_id, my_institution.id)

	click.echo("populating institutional apc data")
	with get_db_cursor() as cursor:
		qry = """
			insert into jump_apc_institutional_authorships (
				select * from jump_apc_institutional_authorships_view
			    where institution_id = %s
			    and issn_l in (select issn_l from openalex_computed)
			)
		"""
		cursor.execute(qry, (my_institution.id,))

	db.session.commit()


def add_ror(ror_id, institution_id):
	click.echo("adding ROR IDs, if needed")

	if not db.session.query(RorId).filter(RorId.institution_id == institution_id, RorId.ror_id==ror_id).all():
		db.session.add(RorId(institution_id=institution_id, ror_id=ror_id))
		click.echo(f"  adding ROR ID {ror_id} for {institution_id}")
	else:
		click.echo("  ROR ID already there")

	db.session.commit()

	# add grid ids
	click.echo("adding GRID IDs, if needed")
	click.echo("  looking up GRID IDs")
	grid_ids = [x.grid_id for x in RorGridCrosswalk.query.filter(RorGridCrosswalk.ror_id == ror_id).all()]

	if not grid_ids:
		raise ValueError("at least one ror id corresponding to a grid id is required)")

	for g_id in grid_ids:
		if not db.session.query(GridId).filter(GridId.institution_id == institution_id, GridId.grid_id==g_id).all():
			db.session.add(GridId(institution_id=institution_id, grid_id=g_id))
			click.echo(f"  adding GRID ID {g_id} for {institution_id}")
		else:
			click.echo("  GRID ID already there")

		db.session.commit()

		# jump_citing
		click.echo("  populating jump_citing for GRID ID {}".format(g_id))

		num_citing_rows = get_sql_answer(db, f"select count(*) from jump_citing where grid_id = '{g_id}'")
		num_citing_rows_view = get_sql_answer(db, f"select count(*) from jump_citing_view where grid_id = '{g_id}'")

		click.echo(f"num_citing_rows: {num_citing_rows}, num_citing_rows_view {num_citing_rows_view}")

		if num_citing_rows:
			click.echo(f"    {num_citing_rows} jump_citing rows already exist for grid id '{g_id}'")
		else:
			with get_db_cursor() as cursor:
				cursor.execute(
					f"delete from jump_citing where grid_id = '{g_id}'"
				)
				cursor.execute(
					f"insert into jump_citing (select * from jump_citing_view where grid_id = '{g_id}')"
				)
			click.echo(f"    created jump_citing rows for grid id {g_id}")

		# jump_authorship

		click.echo(f"  populating jump_authorship for GRID ID  {g_id}")

		num_authorship_rows = get_sql_answer(db, f"select count(*) from jump_authorship where grid_id = '{g_id}'")
		num_authorship_rows_view = get_sql_answer(db, f"select count(*) from jump_authorship_view where grid_id = '{g_id}'")

		click.echo(f"num_authorship_rows: {num_authorship_rows}, num_authorship_rows_view {num_authorship_rows_view}")

		if num_authorship_rows:
			click.echo(f"    {num_authorship_rows} jump_authorship rows already exist for grid id {g_id}")
		else:
			with get_db_cursor() as cursor:
				cursor.execute(
					f"delete from jump_authorship where grid_id = '{g_id}'"
				)
				cursor.execute(
					f"insert into jump_authorship (select * from jump_authorship_view where grid_id = '{g_id}')"
				)
			click.echo(f"    created jump_authorship rows for grid id {g_id}")

		my_packages = Package.query.filter(Package.institution_id==institution_id)
		for my_package in my_packages:
			rows_inserted = my_package.update_apc_authorships()
			click.echo(f"    inserted apc rows for package {my_package}")

def add_user(user_name, email, institution = None, permissions = None, password = None, jiscid = None):
	email = email.strip()
	user_name = user_name.strip()
	
	click.echo(f"initializing user {email}")

	if jiscid is not None:
		institution_id = "institution-jisc" + jiscid
		my_institution = Institution.query.get(institution_id)
		click.echo(my_institution)
	else:
		my_institutions = db.session.query(Institution).filter(
			Institution.id == institution,
			Institution.id.notlike('%jisc%')).all()

		if my_institutions:
			my_institution = my_institutions[0]
			click.echo(f"  *** using existing institution {my_institution} ***")
		else:
			click.echo(f"  *** FAILED: institution {institution} doesn't exist, exiting ***")
			return

	my_user = db.session.query(User).filter(User.email.ilike(email)).scalar()

	if my_user:
		click.echo(f"  *** user {my_user} already exists. updating display name but not modifying password. ***")
	else:
		my_user = User()
		my_user.email = email
		my_user.password_hash = generate_password_hash(password or "")

	my_user.display_name = user_name
	db.session.merge(my_user)
	click.echo(f"  saving {my_user}")
	click.echo("  updating permissions and linking to institution")

	permission_names = permissions
	if not permission_names:
		permission_names = ["view", "modify", "admin"]

	existing_permissions = db.session.query(UserInstitutionPermission).filter(
		UserInstitutionPermission.user_id == my_user.id,
		UserInstitutionPermission.institution_id == my_institution.id
	).all()

	for ep in existing_permissions:
		click.echo(f"  *** removing existing user permission {ep} ***")
		db.session.delete(ep)

	for permission_name in permission_names:
		perm = Permission.get(permission_name)
		if not perm:
			raise ValueError(f"unknown permission '{permission_name}'")
		user_perm = UserInstitutionPermission(
			user_id=my_user.id,
			institution_id=my_institution.id,
			permission_id=perm.id
		)
		db.session.add(user_perm)
		db.session.flush()
		click.echo(f"  adding {user_perm}")

	db.session.commit()


@click.group()
def cli():
	"""
	Create institutions, users and add ROR IDs.

	Note: Quotes are required around any flag inputs with spaces.

	Examples:

		python init.py --help

		python init.py inst --help

		python init.py inst --name='Forest College' --shortname=forcoll --ror=05fs6jp91
		
		python init.py user --name='Scott Chamberlain' --email=myrmecocystus@gmail.com --institution='institution-Z9vU94XpmwKF'
		
		python init.py ror --ror=05fs6jp91 --institution=institution-Z9vU94XpmwKF
		python init.py ror --ror=005p9kw61 --ror=00hpz7z43 --institution=institution-Z9vU94XpmwKF
	"""

@cli.command(short_help='Create an institution Unsub account')
@click.option("--name", help="Name of the institution", type=str)
@click.option("--shortname", help="Shortname of the institution (e.g., amnh)", type=str)
@click.option("--ror", help="One or more ROR IDs (can be passed multiple times)", 
	type=str, multiple=True, required=True)
@click.option("--is_consortium", help="True if is a consortium", type=bool, default=False)
def inst(name, shortname, ror, is_consortium):
	click.echo(f"Creating Unsub account for '{name}' w/ shortname '{shortname}' and ROR ID(s) {ror}")
	add_institution(name, shortname, list(ror), is_consortium)


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
	add_user(name, email, institution, permissions, password, jiscid)


@cli.command(short_help='add a ROR ID to an institution/account')
@click.option("--ror", help="One or more ROR IDs (can be passed multiple times)", 
	type=str, multiple=True, required=True)
@click.option("--institution", help="An institution ID", type=str, required=True)
def ror(ror, institution):
	click.echo(f"Adding ROR ID(s) {ror} to {institution}")
	for x in ror:
		add_ror(x, institution)

if __name__ == "__main__":
	cli()
