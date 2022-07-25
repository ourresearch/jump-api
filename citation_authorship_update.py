# coding: utf-8
import click
from psycopg2 import sql
import datetime
from app import get_db_cursor

# 7 days, as seconds
UPDATE_AFTER_DEFAULT = 604800
# update_after = 600

def grid_id_delete(grid_id, table):
	with get_db_cursor() as cursor:
		cmd = "delete from {} where grid_id=%s".format(table)
		click.echo(cursor.mogrify(cmd, (grid_id,)))
		cursor.execute(cmd, (grid_id,))

def grid_id_insert(grid_id, table, view):
	with get_db_cursor() as cursor:
		cmd = "insert into {} (select * from {} where grid_id = %s)".format(table, view)
		click.echo(cursor.mogrify(cmd, (grid_id,)))
		cursor.execute(cmd, (grid_id,))

def update_citing(grid_id):
	grid_id_delete(grid_id, "jump_citing")
	grid_id_insert(grid_id, "jump_citing", "jump_citing_view")

def update_authorship(grid_id):
	grid_id_delete(grid_id, "jump_authorship")
	grid_id_insert(grid_id, "jump_authorship", "jump_authorship_view")

def update_apc_authorships(package_id):
	with get_db_cursor() as cursor:
		cmd = "delete from jump_apc_authorships where package_id=%s"
		click.echo(cursor.mogrify(cmd, (package_id,)))
		cursor.execute(cmd, (package_id,))

	with get_db_cursor() as cursor:
		cmd = """insert into jump_apc_authorships (
        select * from jump_apc_authorships_view
        where package_id = %s and issn_l in
        (select issn_l from openalex_computed))
		"""
		click.echo(cursor.mogrify(cmd, (package_id,)))
		cursor.execute(cmd, (package_id,))

def check_updated(grid_id, table):
	with get_db_cursor() as cursor:
		cmd = "select * from {} where grid_id=%s order by updated desc limit 1".format(table)
		click.echo(cursor.mogrify(cmd, (grid_id,)))
		cursor.execute(cmd, (grid_id,))
		rows = cursor.fetchall()

	return rows

def check_updated_apc(pkg_id):
	with get_db_cursor() as cursor:
		cmd = "select * from jump_apc_authorships_updates where package_id=%s order by updated desc limit 1"
		click.echo(cursor.mogrify(cmd, (pkg_id,)))
		cursor.execute(cmd, (pkg_id,))
		rows = cursor.fetchall()

	return rows

def record_update(grid_id, table, err, institution_id = None, package_id = None):
	if table in ['jump_citing_updates','jump_authorship_updates',]:
		cols = ['updated', 'grid_id', 'error']
		values = (datetime.datetime.utcnow(), grid_id, err, )
	else:
		cols = ['updated', 'institution_id', 'package_id', 'error',]
		values = (datetime.datetime.utcnow(), institution_id, package_id, err, )

	with get_db_cursor() as cursor:
		qry = sql.SQL("insert into {} ({}) values ({})").format(
			sql.Identifier(table),
			sql.SQL(', ').join(map(sql.Identifier, cols)),
			sql.SQL(', ').join(sql.Placeholder() * len(cols)))
		click.echo(cursor.mogrify(qry, values))
		cursor.execute(qry, values)

@click.group()
def cli():
	"""
	Update jump_citing, jump_authorship, and jump_apc_authorships

	Examples:

		python citation_authorship_update.py --help
		python citation_authorship_update.py citing --help
		python citation_authorship_update.py citing
		python citation_authorship_update.py authorship
		python citation_authorship_update.py apc
	"""

@cli.command(short_help='Update jump_citing table for each grid_id')
@click.option("--update_after", help="update after (seconds)", type=int)
def citing(update_after=None):
	click.echo("Updating jump_citing")
	update_after = update_after or UPDATE_AFTER_DEFAULT

	with get_db_cursor() as cursor:
		cmd = "select DISTINCT(grid_id) from jump_citing"
		cursor.execute(cmd)
		rows = cursor.fetchall()

	jc_grid_ids_uniq = [w[0] for w in rows]
	# len(jc_grid_ids_uniq)

	# grid_id = jc_grid_ids_uniq[3]
	for grid_id in jc_grid_ids_uniq:
		click.echo(f"working on (update_citing, {grid_id})")
		res = check_updated(grid_id, "jump_citing_updates")
		
		update = False
		if not res:
			update = True
		else:
			if (datetime.datetime.utcnow() - res[0]['updated']).seconds > update_after:
				update = True
		
		if not update:
			click.echo(f"(update_citing, {grid_id}) already updated recently")
		else:
			mssg = None
			try:
				update_citing(grid_id)
			except Exception as err:
				mssg = str(err)
				click.echo(f"(update_citing, {grid_id}) failed: {err}")

			record_update(grid_id, 'jump_citing_updates', mssg)


@cli.command(short_help='Update jump_authorship table for each grid_id')
@click.option("--update_after", help="update after (seconds)", type=int)
def authorship(update_after=None):
	click.echo("Updating jump_authorship")
	update_after = update_after or UPDATE_AFTER_DEFAULT

	with get_db_cursor() as cursor:
		cmd = "select distinct(grid_id) from jump_grid_id where grid_id not ilike '%example%'"
		cursor.execute(cmd)
		authrows = cursor.fetchall()

	ja_grid_ids_uniq = [w[0] for w in authrows]
	# len(ja_grid_ids_uniq)

	# grid_id = ja_grid_ids_uniq[3]
	for grid_id in ja_grid_ids_uniq:
		click.echo(f"working on (update_authorship, {grid_id})")
		res = check_updated(grid_id, "jump_authorship_updates")
		
		update = False
		if not res:
			update = True
		else:
			if (datetime.datetime.utcnow() - res[0]['updated']).seconds > update_after:
				update = True
		
		if not update:
			click.echo(f"(update_authorship, {grid_id}) already updated recently")
		else:
			mssg = None
			try:
				update_authorship(grid_id)
			except Exception as err:
				mssg = str(err)
				click.echo(f"(update_authorship, {grid_id}) failed: {err}")

			record_update(grid_id, 'jump_authorship_updates', mssg)

@cli.command(short_help='Update jump_apc_authorships table for each package_id')
@click.option("--update_after", help="update after (seconds)", type=int)
def apc(update_after=None):
	click.echo("Updating jump_apc_authorships")
	update_after = update_after or UPDATE_AFTER_DEFAULT

	# exclude packages: deleted, demo, and consortial feeder (-f for some jisc pkgs, and others)
	with get_db_cursor() as cursor:
		cmd = """
		select package_id, institution_id from jump_account_package 
			where package_id in (select DISTINCT(package_id) from jump_apc_authorships)
			and not is_deleted
			and not is_demo
			and package_id not ilike '%-f'
			and package_id not in (select DISTINCT(member_package_id) from jump_consortium_members)
		"""
		cursor.execute(cmd)
		pkgrows = cursor.fetchall()

	for row in pkgrows:
		click.echo(f"working on (update_apc_authorships, {row['package_id']})")
		res = check_updated_apc(row['package_id'])
		
		update = False
		if not res:
			update = True
		else:
			if (datetime.datetime.utcnow() - res[0]['updated']).seconds > update_after:
				update = True
		
		if not update:
			click.echo(f"(update_apc_authorships, {row['package_id']}) already updated recently")
		else:
			mssg = None
			try:
				update_apc_authorships(row['package_id'])
			except Exception as err:
				mssg = str(err)
				click.echo(f"(update_apc_authorships, {row['package_id']}) failed: {err}")

			record_update(None, 'jump_apc_authorships_updates', mssg, 
				institution_id = row['institution_id'], package_id = row['package_id'])

if __name__ == "__main__":
	cli()
