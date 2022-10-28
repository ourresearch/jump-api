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

def add_institution(institution_name, ror_id_list, cli=False):
	if cli:
		click.echo("initializing institution {}".format(institution_name))

	my_institutions = db.session.query(Institution).filter(
		Institution.display_name == institution_name,
		Institution.id.notlike('%jisc%')).all()

	if my_institutions:
		my_institution = my_institutions[0]
		if cli:
			click.echo(f"  *** using existing institution {my_institution} ***")

	my_institution = Institution()
	my_institution.display_name = institution_name
	my_institution.is_demo_institution = False
	my_institution.is_consortium = False
	db.session.add(my_institution)
	if cli:
		click.echo(f"  adding {my_institution}")

	if not ror_id_list:
		return

	for ror_id in ror_id_list:
		add_ror(ror_id, my_institution.id)

	if cli:
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

	return my_institution.id

def add_ror(ror_id, institution_id, cli=False):
	if cli:
		click.echo("adding ROR IDs, if needed")

	if not db.session.query(RorId).filter(RorId.institution_id == institution_id, RorId.ror_id==ror_id).all():
		db.session.add(RorId(institution_id=institution_id, ror_id=ror_id))
		if cli:
			click.echo(f"  adding ROR ID {ror_id} for {institution_id}")
	else:
		if cli:
			click.echo("  ROR ID already there")

	db.session.commit()

	# add grid ids
	if cli:
		click.echo("adding GRID IDs, if needed")
		click.echo("  looking up GRID IDs")
	grid_ids = [x.grid_id for x in RorGridCrosswalk.query.filter(RorGridCrosswalk.ror_id == ror_id).all()]

	if not grid_ids:
		raise ValueError("at least one ror id corresponding to a grid id is required)")

	for g_id in grid_ids:
		if not db.session.query(GridId).filter(GridId.institution_id == institution_id, GridId.grid_id==g_id).all():
			db.session.add(GridId(institution_id=institution_id, grid_id=g_id))
			if cli:
				click.echo(f"  adding GRID ID {g_id} for {institution_id}")
		else:
			if cli:
				click.echo("  GRID ID already there")

		db.session.commit()

		# jump_citing
		if cli:
			click.echo("  populating jump_citing for GRID ID {}".format(g_id))

		num_citing_rows = get_sql_answer(db, f"select count(*) from jump_citing where grid_id = '{g_id}'")
		num_citing_rows_view = get_sql_answer(db, f"select count(*) from jump_citing_view where grid_id = '{g_id}'")

		if cli:
			click.echo(f"    num_citing_rows: {num_citing_rows}, num_citing_rows_view {num_citing_rows_view}")

		if num_citing_rows:
			if cli:
				click.echo(f"    {num_citing_rows} jump_citing rows already exist for grid id '{g_id}'")
		else:
			with get_db_cursor() as cursor:
				cursor.execute(
					f"delete from jump_citing where grid_id = '{g_id}'"
				)
				cursor.execute(
					f"insert into jump_citing (select * from jump_citing_view where grid_id = '{g_id}')"
				)
			if cli:
				click.echo(f"    created jump_citing rows for grid id {g_id}")

		# jump_authorship
		if cli:
			click.echo(f"  populating jump_authorship for GRID ID  {g_id}")

		num_authorship_rows = get_sql_answer(db, f"select count(*) from jump_authorship where grid_id = '{g_id}'")
		num_authorship_rows_view = get_sql_answer(db, f"select count(*) from jump_authorship_view where grid_id = '{g_id}'")

		if cli:
			click.echo(f"    num_authorship_rows: {num_authorship_rows}, num_authorship_rows_view {num_authorship_rows_view}")

		if num_authorship_rows:
			if cli:
				click.echo(f"    {num_authorship_rows} jump_authorship rows already exist for grid id {g_id}")
		else:
			with get_db_cursor() as cursor:
				cursor.execute(
					f"delete from jump_authorship where grid_id = '{g_id}'"
				)
				cursor.execute(
					f"insert into jump_authorship (select * from jump_authorship_view where grid_id = '{g_id}')"
				)
			if cli:
				click.echo(f"    created jump_authorship rows for grid id {g_id}")

		my_packages = Package.query.filter(Package.institution_id==institution_id)
		for my_package in my_packages:
			rows_inserted = my_package.update_apc_authorships()
			if cli:
				click.echo(f"    inserted apc rows for package {my_package}")

def add_user(user_name, email, institution = None, permissions = None, password = None, jiscid = None, cli = False):
	email = email.strip()
	user_name = user_name.strip()
	
	if cli:
		click.echo(f"initializing user {email}")

	if jiscid is not None:
		institution_id = "institution-jisc" + jiscid
		my_institution = Institution.query.get(institution_id)
		if cli:
			click.echo(my_institution)
	else:
		my_institutions = db.session.query(Institution).filter(
			Institution.id == institution,
			Institution.id.notlike('%jisc%')).all()

		if my_institutions:
			my_institution = my_institutions[0]
			if cli:
				click.echo(f"  *** using existing institution {my_institution} ***")
		else:
			if cli:
				click.echo(f"  *** FAILED: institution {institution} doesn't exist, exiting ***")
			return

	my_user = db.session.query(User).filter(User.email.ilike(email)).scalar()

	if my_user:
		if cli:
			click.echo(f"  *** user {my_user} already exists. updating display name but not modifying password. ***")
	else:
		my_user = User()
		my_user.email = email
		my_user.password_hash = generate_password_hash(password or "")

	my_user.display_name = user_name
	db.session.merge(my_user)
	if cli:
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
		if cli:
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
		if cli:
			click.echo(f"  adding {user_perm}")

	db.session.commit()

	return my_user.id
