import click
from app import get_db_cursor


@click.command()
def update_apc_institutional_authorships():
	with get_db_cursor() as cursor:
		qry = """
			select DISTINCT(id) from jump_institution
			where not is_consortium
			and not is_demo_institution
			and id in (select DISTINCT(institution_id) from jump_debug_combo_view)
		"""
		cursor.execute(qry)
		rows = cursor.fetchall()

	institution_ids = [w[0] for w in rows]
	print(f"Getting to work on {len(institution_ids)} institutions\n")

	for inst_id in institution_ids:
		print(f"deleting & inserting data for {inst_id}")

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


if __name__ == "__main__":
	update_apc_institutional_authorships()
