import pandas as pd
import shortuuid
import numpy as np

from saved_scenario import SavedScenario, save_raw_scenario_to_db
from app import db
from app import get_db_cursor
from util import safe_commit

# class Empty(object):
#   pass
# self = Empty()
# self.__class__ = ExcludedTitles
# package_id = 'package-m46xU5bqA4vH'
# x = ExcludedTitles(package_id = package_id)

class ExcludedTitles:
	def __init__(self, package_id):
		self.package_id = package_id
		from package import Package
		self.pkg = Package.query.get(self.package_id)
		self.temp_scenario = False
		self.selected_scenario_id = None
		self.report_csv = None
		self.error_message = None

	def scenario_create(self):
		new_scenario_id = shortuuid.uuid()[0:8]
		new_saved_scenario = SavedScenario(False, new_scenario_id, None)
		new_saved_scenario.package_id = self.package_id
		new_saved_scenario.is_base_scenario = False
		dict_to_save = new_saved_scenario.to_dict_saved_from_db()
		dict_to_save["name"] = "temp-scenario-delete-me"
		save_raw_scenario_to_db(new_scenario_id, dict_to_save, None)
		db.session.add(new_saved_scenario)
		safe_commit(db)
		return new_scenario_id

	def fetch_or_make_temp_scenario(self):
		if not self.pkg.saved_scenarios:
			print("no scenarios found, creating one")
			scenario_id = self.scenario_create()
			self.temp_scenario = True
		else:
			print("scenarios found, using one")
			scenario_id = self.pkg.saved_scenarios[0].scenario_id

		self.selected_scenario_id = scenario_id

	def cleanup_temp_scenario(self):
		if self.temp_scenario:
			with get_db_cursor() as cursor:
				qry = "delete from jump_package_scenario where scenario_id=%s"
				cursor.execute(qry, (self.selected_scenario_id,))
	
	def calculate(self):
		with get_db_cursor() as cursor:
			cmd = """
				select distinct(issn_l) from jump_counter where package_id = %s
				and report_name in ('trj2','trj3')
				and metric_type in ('Unique_Item_Requests','No_License')
			"""
			cursor.execute(cmd, (self.package_id,))
			rows = cursor.fetchall()

		if rows:
			issns = [x[0] for x in rows if x[0] is not None]
			issns.sort()
		else:
			# try jr1
			with get_db_cursor() as cursor:
				cmd = "select distinct(issn_l) from jump_counter where package_id = %s and report_name = 'jr1'"
				cursor.execute(cmd, (self.package_id,))
				rows = cursor.fetchall()

			if rows:
				issns = [x[0] for x in rows if x[0] is not None]
				issns.sort()
			else:
				self.error_message = (
					"No COUNTER usage data was found for package {}. "
					"Upload TRJ2/TRJ3 (COUNTER 5) or JR1 (COUNTER 4) reports under "
					"Setup before exporting the excluded titles list."
				).format(self.package_id)
				return

		if not issns:
			self.error_message = (
				"COUNTER files for package {} have no rows with a resolvable ISSN-L. "
				"This usually means the title-to-ISSN matching has not finished, or the uploaded "
				"reports do not contain ISSN values. Re-upload the COUNTER files or wait for "
				"ingest to finish, then try the export again."
			).format(self.package_id)
			return

		try:
			x = SavedScenario.query.get(self.selected_scenario_id)
			journals = x.journals if x is not None else None
		except Exception as e:
			print(e)
			raise Exception(e)

		if not journals:
			self.error_message = (
				"Scenario {} for package {} has no journals loaded yet. "
				"Open the scenario at least once (or upload a journal price list) so the "
				"package's journal set is populated, then try the export again."
			).format(self.selected_scenario_id, self.package_id)
			return

		issns_scenario = [w.issn_l for w in journals]
		issns_scenario.sort()
		diff = tuple(set(issns) - set(issns_scenario))

		if not diff:
			self.error_message = (
				"All {} ISSN-Ls in the COUNTER files for package {} are already represented "
				"in scenario {}, so there are no excluded titles to report."
			).format(len(issns), self.package_id, self.selected_scenario_id)
			return

		with get_db_cursor() as cursor:
			cmd = """
				select issn_l,publisher,is_gold_journal_in_most_recent_year,is_currently_publishing
				from openalex_computed
				where issn_l in %s
				"""
			cursor.execute(cmd, (diff,))
			rows = cursor.fetchall()

		df = pd.DataFrame(rows, columns=['issn_l','publisher','gold_oa','currently_publishing'])

		if df.empty:
			self.error_message = (
				"Found {} ISSN-Ls in COUNTER for package {} that are not in scenario {}, "
				"but none of them have matching rows in openalex_computed. The OpenAlex journal "
				"metadata table may be out of sync; please contact support."
			).format(len(diff), self.package_id, self.selected_scenario_id)
			return

		df['not_currently_publishing'] = ~df['currently_publishing']
		remainder = df[~(df['gold_oa'] | df['not_currently_publishing'])]

		remainder_issns = tuple(remainder['issn_l'].to_list())
		if remainder_issns:
			with get_db_cursor() as cursor:
				cmd = "select * from jump_journal_prices where package_id = %s and issn_l in %s"
				cursor.execute(cmd, (self.package_id, remainder_issns,))
				rows_prices = cursor.fetchall()
		else:
			# Every excluded title is either gold-OA or not-currently-publishing,
			# so there are no price rows to fetch.
			rows_prices = []

		price_not_available = remainder[~remainder['issn_l'].isin([x[3] for x in rows_prices])]
		price_not_available = price_not_available.drop(["gold_oa","publisher","currently_publishing","not_currently_publishing"], axis=1)
		price_not_available['price_not_available'] = True
		df2 = df.merge(price_not_available, how="left", on="issn_l")
		df2['price_not_available'] = df2['price_not_available'].replace(np.nan, False)

		# journal filter
		with get_db_cursor() as cursor:
			cmd = "select * from jump_journal_filter where package_id = %s"
			cursor.execute(cmd, (self.package_id,))
			rows_filter = cursor.fetchall()
		## if no journal filter uploaded, then no titles filtered = all are False
		if not rows_filter:
			df2['filtered_out'] = False
		## if journals are filtered, then assign filtered out titles as True, and all others as False
		else:
			issns_filtered = [w['issn_l'] for w in rows_filter]
			df2['filtered_out'] = [False if i in issns_filtered else True for i in df2['issn_l'].to_list()]

		df_nodups = df2.drop_duplicates()
		df_nodups.drop(['currently_publishing'], axis=1, inplace=True)
		self.report_csv = df_nodups.to_csv(index=False)
