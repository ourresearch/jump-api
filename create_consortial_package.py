import click
import shortuuid
import datetime
import subprocess
from enum import Enum
from psycopg2 import sql
from psycopg2.extras import execute_values

from app import db
from app import get_db_cursor
from saved_scenario import SavedScenario
from saved_scenario import save_raw_scenario_to_db
from util import safe_commit
from consortium import Consortium, get_consortium_ids
from package import Package
from saved_scenario import save_raw_member_institutions_included_to_db

class Pubs(Enum):
	elsevier = "Elsevier"
	springer = "SpringerNature"

def make_consortial_package(publisher, institution_id, pkg_name, currency):
	click.echo("    Adding new package to jump_account_package table")

	pkg_id = 'package-{}'.format(shortuuid.uuid()[0:12])

	if not pkg_name:
		pkg_name = publisher

	if not currency:
		currency = 'USD'

	with get_db_cursor() as cursor:
		cmd = """
			insert into jump_account_package (account_id, package_id, publisher, package_name, created, consortium_package_id, institution_id, is_demo, big_deal_cost, big_deal_cost_increase, is_deleted, updated, default_to_no_perpetual_access, currency)
			values
			(%(inst)s, %(pkg)s, %(pub)s, %(pkg_name)s, sysdate, null, %(inst)s, false, null, null, false, null, null, %(currency)s)
		"""
		cursor.execute(cmd, 
			{
				'inst': institution_id, 
				'pkg': pkg_id, 
				'pub': Pubs[publisher].value, 
				'pkg_name': pkg_name,
				'currency': currency,
			})

	return pkg_id

# create user and give permissions for new consortium
# after editing the file, run: heroku local:run python init_institution.py --users --commit

def scenario_create(package_id):
	new_scenario_id = shortuuid.uuid()[0:8]
	new_scenario_name = "First Scenario"
	new_saved_scenario = SavedScenario(False, new_scenario_id, None)
	new_saved_scenario.set_live_scenario()
	new_saved_scenario.package_id = package_id
	new_saved_scenario.is_base_scenario = False
	dict_to_save = new_saved_scenario.to_dict_saved_from_db()
	dict_to_save["name"] = new_scenario_name
	save_raw_scenario_to_db(new_scenario_id, dict_to_save, None)
	db.session.add(new_saved_scenario)
	safe_commit(db)

def copy_pkgs_as_feeders(pkgids, package_id_prefix = "package-"):
	feeder_pkg_ids = []
	for package_id in pkgids:
		with get_db_cursor() as cursor:
			cmd = "select account_id,publisher,package_name,institution_id,is_demo,big_deal_cost,is_deleted,currency,big_deal_cost_increase,package_description from jump_account_package where package_id = %s;"
			cursor.execute(cmd, (package_id,))
			rows = cursor.fetchall()

		new_package_id = package_id_prefix + shortuuid.uuid()[0:12]
		feeder_pkg_ids.append(new_package_id)
		data = tuple(rows[0] + [new_package_id, datetime.datetime.utcnow()])

		cols = ['account_id',
			 'publisher',
			 'package_name',
			 'institution_id',
			 'is_demo',
			 'big_deal_cost',
			 'is_deleted',
			 'currency',
			 'big_deal_cost_increase',
			 'package_description',
			 'package_id',
			 'created']
		with get_db_cursor() as cursor:
			qry = sql.SQL("INSERT INTO {} ({}) values ({})").format( 
				sql.Identifier('jump_account_package'),
				sql.SQL(', ').join(map(sql.Identifier, cols)),
				sql.SQL(', ').join(sql.Placeholder() * len(cols)))
			cursor.execute(qry, data)

	return feeder_pkg_ids

def copy_package(old_package_id, new_package_id):
	command = """
		insert into jump_counter (issn_l, package_id, journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type, created) (
			select issn_l, '{new_package_id}', journal_name, total, report_year, report_name, report_version, metric_type, yop, access_type, created
			from jump_counter
			where package_id = '{old_package_id}'
		);
		
		insert into jump_counter_input (issn, journal_name, total, package_id, report_year, report_name, report_version, metric_type, yop, access_type) (
			select issn, journal_name, total, '{new_package_id}', report_year, report_name, report_version, metric_type, yop, access_type
			from jump_counter_input
			where package_id = '{old_package_id}'
		);
		
		insert into jump_perpetual_access (package_id, issn_l, start_date, end_date, created) (
			select '{new_package_id}', issn_l, start_date, end_date, created
			from jump_perpetual_access
			where package_id = '{old_package_id}'
		);
		
		insert into jump_perpetual_access_input (package_id, issn, start_date, end_date) (
			select '{new_package_id}', issn, start_date, end_date
			from jump_perpetual_access_input
			where package_id = '{old_package_id}'
		);
		
		insert into jump_journal_prices (package_id, publisher, title, issn_l, price, created) (
			select '{new_package_id}', publisher, title, issn_l, price, created
			from jump_journal_prices
			where package_id = '{old_package_id}'
		);
		
		insert into jump_journal_prices_input (package_id, publisher, issn, price) (
			select '{new_package_id}', publisher, issn, price
			from jump_journal_prices_input
			where package_id = '{old_package_id}'
		);

		insert into jump_raw_file_upload_object (package_id, file, bucket_name, object_name, created, num_rows, error_details, error, to_delete_date) (
			select '{new_package_id}', file, bucket_name, object_name, created, num_rows, error_details, error, to_delete_date
			from jump_raw_file_upload_object
			where package_id = '{old_package_id}'
		);
	""".format(new_package_id=new_package_id, old_package_id=old_package_id)
	with get_db_cursor() as cursor:
		cursor.execute(command)

# Copy counter, pta and price data for the new feeder packages
def copy_pkgs_data(pkg_ids, feed_ids):
	all_pkg_ids = []
	for index, pkgid in enumerate(pkg_ids):
		all_pkg_ids.append({'old_package_id': pkg_ids[index], 'new_package_id': feed_ids[index]})

	for x in all_pkg_ids:
		click.echo(f"{x['old_package_id']} -> {x['new_package_id']}")
		copy_package(x['old_package_id'], x['new_package_id'])

# Associate feeder packages with consortium packages
def assoc_feeder_pkgs(institution, publisher, consortium_package_id, feed_ids):
	# get consortium short name
	with get_db_cursor() as cursor:
		cursor.execute("select old_username from jump_institution where id = %s", (institution,))
		short_name = cursor.fetchone()
	if short_name:
		consortium_short_name = short_name[0]
	else:
		raise Exception(f"no 'old_username' found for institution: {institution}")
	
	values = [(f"{consortium_short_name}_{publisher}", consortium_package_id, w,) for w in feed_ids]
	cols = ["consortium_short_name", "consortium_package_id", "member_package_id"]
	with get_db_cursor() as cursor:
		qry = sql.SQL("INSERT INTO jump_consortium_members ({}) VALUES %s").format(
			sql.SQL(', ').join(map(sql.Identifier, cols)))
		execute_values(cursor, qry, values)

# Recompute consortia
def recompute_consortium(consortium_package_id):
	consortium_ids = get_consortium_ids()
	for d in consortium_ids:
		if consortium_package_id == d["package_id"]:
			print("starting to recompute row {}".format(d))
			new_consortia = Consortium(d["scenario_id"])
			new_consortia.recompute_journal_dicts()
			print("recomputing {}".format(new_consortia))

def heroku_restart():
	subprocess.run(["heroku", "restart", "--remote", "heroku"])

# Set all members to "true" in the consortial dashboard so they are included in the scenario
def include_all_members(consortium_package_id, feed_ids):
	package = Package.query.filter(Package.package_id == consortium_package_id).scalar()
	scenario = package.saved_scenarios[0]
	save_raw_member_institutions_included_to_db(scenario.scenario_id, feed_ids, None)

def foo_bar(pkgname):
	print(f'pkgname: {pkgname}')

@click.command()
@click.option('--institution', help='Institution ID (e.g., "institution-aad8a66ad0na9")', required=True)
@click.option('--publisher', help='A publisher', required=True)
@click.option('--pkgid', help='Package IDs to feed into the consortial dashboard; flag can be supplied multiple times', multiple=True)
@click.option('--pkgidprefix', default="package-", help='Package IDs; flag can be supplied multiple times', required=True)
@click.option('--pkgname', help='Package name; by default the package name will be the value of --publisher')
@click.option('--currency', help='Package currency; by default the currency will be USD (options: USD, GBP)')
# for VIVA:
# heroku local:run python create_consortial_package.py --institution=institution-3tLYzP8JuYUf --publisher=springer --pkgid=package-YHV55FEuJCCr --pkgid=package-5GLcckM6ExH4 --pkgid=package-NHMnfCVKs4kc --pkgid=package-covfz2AoSLSA --pkgid=package-HFtEy7V9kpNm --pkgid=package-oXaqhaf38EqY --pkgid=package-5XK9GGwHWeNa --pkgid=package-oLD8eXCY3ysz --pkgid=package-KRr3YrDS59bK --pkgid=package-WxDawozhLReN --pkgid=package-FBM9Yeiix799
# for CRKN:
# heroku local:run python create_consortial_package.py --institution=institution-FeVtPAsVeKsK --publisher=springer --pkgid=package-N7PRDVAP4mdL --pkgid=package-iXxjTfXHUQvT --pkgid=package-WVmPikL2ykFN --pkgid=package-VCBiS69KY4e6 --pkgid=package-SngaXxBJcigc --pkgid=package-V8EYqqSXkgYi --pkgid=package-ZtYJwK97KXRp --pkgid=package-HwbRyHwXshTP --pkgid=package-kDaDr7pHiKDY --pkgid=package-NDNCw5aE2iEK --pkgid=package-2wobL7vDqUqx --pkgid=package-HZhoLdnEhcBU --pkgid=package-XFNWXDe2kzx9 --pkgid=package-aCaoBK7gemo2 --pkgid=package-C7FCJy5vSYyJ --pkgid=package-himw3PHajapj --pkgid=package-kWDYjwXK9gbs --pkgid=package-9VS4bXQpRhFc --pkgid=package-m6cjHqhBmhxZ --pkgid=package-bkHeEXuaBsoz --pkgid=package-L3sJqPkstJnk --pkgid=package-4WTgXLphoRQJ --pkgid=package-6GyQ3idSb3gr --pkgid=package-ixviLpJsU5xK --pkgid=package-Gqns2JwkohC9 --pkgid=package-KTvQfX9Ba9vL --pkgid=package-D6cCForPYpWF --pkgid=package-VqmuUZBzygYq --pkgid=package-MqPQHkMGbc7e --pkgid=package-4RmA5zqQ37Mk --pkgid=package-9sziriuGANKC --pkgid=package-LFuptcgYPj35 --pkgid=package-5EQZJR9ytLhf --pkgid=package-SXV7VwDMiNkQ --pkgid=package-nX34X8ZfnPNB --pkgid=package-AopqasoXDDMD --pkgid=package-mMjEUuiWEPv8 --pkgid=package-XZhv2jjPkpKC --pkgid=package-77igEayNDTyL --pkgid=package-RCXdWMupnt36 --pkgid=package-2qfkHj7LT6mJ --pkgid=package-UUgPJJ43C3Ka --pkgid=package-VcTCvAWMzhbA --pkgid=package-2erq2pje87NT --pkgid=package-8AhXa8LtErY8 --pkgid=package-mTtR7t573vP6 --pkgid=package-gKZGa6FdK4gz --pkgid=package-NBP8Mveu7spD --pkgid=package-ej624b4BWfP7 --pkgid=package-2ULJzArE43uR --pkgid=package-K2MaQ6mNUbDL --pkgid=package-PNe2ZQrChgLj --pkgid=package-bS6eF92xhoum --pkgid=package-EkEX85fdvZrN --pkgid=package-BnuucptQhSS3 --pkgid=package-7RsaWjFM5hZD --pkgid=package-ApV2Q2r27KX9 --pkgid=package-MsN2yK7Z8aKn --pkgid=package-KRgqisUCnt9h --pkgid=package-ZwZmukyiR9Rm --pkgid=package-77QHpgXwZmNg --pkgid=package-XtwRcjHJqgpA --pkgid=package-B2J3H85UQEKo
# for IReL:
# heroku local:run python create_consortial_package.py --institution=institution-RQNQTenWzaoB --publisher=elsevier --currency=GBP --pkgname='Science Direct - Big Deal spend' --pkgid=package-WUJ38hLb4qtV --pkgid=package-MjGeMzUx8iRL --pkgid=package-oDYCmhKMpxEC --pkgid=package-irBjHQdsxXTV --pkgid=package-6a25k9svUEvj --pkgid=package-LpaWn6Fno23c --pkgid=package-gFoiughSeK2m --pkgid=package-4brUuKZVnZHG --pkgid=package-6UcSR8TKGUfv --pkgid=package-LmXDzwMDkaqX --pkgid=package-XrGMv4qL58Ax --pkgid=package-BQtXdsNfyWrM --pkgid=package-JJJvxsuYQN7T --pkgid=package-SimipgAohnh2
def create_consortial_package(institution, publisher, pkgid, pkgidprefix, pkgname, currency):
	"""Create a consortium for internal testing purposes"""
	publisher = publisher.lower()

	click.echo(f"Using consortium institution ID: '{institution}'")

	consortium_package_id = 'package-nydgoDcUqmVd'
	# consortium_package_id = make_consortial_package(publisher, institution, pkgname, currency)
	# click.echo(f"Using package ID: '{consortium_package_id}'")

	# click.echo(f"Creating a scenario for package ID: '{consortium_package_id}'")
	# scenario_create(consortium_package_id)

	click.echo("Package IDs")
	package_ids = pkgid
	click.echo(f"   Using {len(package_ids)} packages")

	click.echo("Copying packages as feeder packages")
	feedids = copy_pkgs_as_feeders(package_ids, pkgidprefix)

	click.echo("Copying data (counter, pta, prices, uploads, apc) for each feeder package")
	copy_pkgs_data(package_ids, feedids)

	click.echo("Inserting feeder pkg ids into jump_consortium_members table")
	assoc_feeder_pkgs(institution, publisher, consortium_package_id, feedids)

	click.echo("Recomputing the consortium")
	recompute_consortium(consortium_package_id)

	click.echo("Restarting Heroku application")
	heroku_restart()

	click.echo("Include all member packages in the consortial dashboard so they are included in the scenario")
	include_all_members(consortium_package_id, feedids)

	click.echo("Done!")


if __name__ == '__main__':
	create_consortial_package()
