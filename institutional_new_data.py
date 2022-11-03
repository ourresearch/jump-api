# purpose: add apc institutional data for newly added institutions
#  - The SQL involved can take a long time, longer than the Heroku
#   30 second time limit. Thus, this Heroku scheduled task gets
#   around that

import click
from app import get_db_cursor
from admin_actions import add_ror
from institution import Institution

@click.command()
def insert_institutional_data():
	# find institutions created in the last 30 min (& not demo or consortia)
	with get_db_cursor() as cursor:
		qry = """
			select DISTINCT(id) from jump_institution
			where not is_consortium
			and not is_demo_institution
			and datediff(minute, created, sysdate) <= 30
		"""
		cursor.execute(qry)
		rows = cursor.fetchall()

	institution_ids = [w[0] for w in rows]
	click.echo(f"Getting to work on {len(institution_ids)} institutions\n")

	# citations and authorships data
	for inst_id in institution_ids:
		click.echo(f"updating citation and authorship data for {inst_id}")

		with get_db_cursor() as cursor:
			qry = "select ror_id from institution_ror_added where institution_id = %s"
			cursor.execute(qry, (inst_id,))
			rorids = cursor.fetchall()		
		
		if rorids:
			rorids = [w[0] for w in rorids]
			for ror_id in rorids:
				add_ror(ror_id, inst_id, cli=True)

	# APCs
	for inst_id in institution_ids:
		click.echo(f"updating APC data for {inst_id}")

		with get_db_cursor() as cursor:
			qry = "delete from jump_apc_institutional_authorships where institution_id = %s"
			cursor.execute(qry, (inst_id,))

		with get_db_cursor() as cursor:
			qry = """
				insert into jump_apc_institutional_authorships (
					select * from jump_apc_institutional_authorships_view
				    where institution_id = %s
				    and issn_l in (select issn_l from openalex_computed)
				)
			"""
			cursor.execute(qry, (inst_id,))

	click.echo("Done!")


if __name__ == "__main__":
	insert_institutional_data()
