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
			# print(f"    issns in counter files (trj2 & 3): {len(issns)}")
		else:
			# try jr1
			with get_db_cursor() as cursor:
				cmd = "select distinct(issn_l) from jump_counter where package_id = %s and report_name = 'jr1'"
				cursor.execute(cmd, (self.package_id,))
				rows = cursor.fetchall()
			
			if rows:
				issns = [x[0] for x in rows if x[0] is not None]
				issns.sort()
				# print(f"    issns in counter jr1 file: {len(issns)}")
			else:
				# print(f"    package not found: {self.package_id}")
				raise Exception("package not found")
		
		journals = None
		try:
			x = SavedScenario.query.get(self.selected_scenario_id)
			journals = x.journals
		except Exception as e:
			print(e)
			raise Exception(e)
		
		if journals:
			issns_scenario = [w.issn_l for w in journals]
			issns_scenario.sort()
			# print(f"    issns in scenario: {len(issns_scenario)}")
			diff = tuple(set(issns) - set(issns_scenario))
			# print(f"    issns diff: {len(diff)}")

			with get_db_cursor() as cursor:
				cmd = """
					select issn_l,publisher,is_gold_journal_in_most_recent_year,is_currently_publishing 
					from openalex_computed
					where issn_l in %s
					"""
				cursor.execute(cmd, (diff,))
				rows = cursor.fetchall()

			df = pd.DataFrame(rows, columns=['issn_l','publisher','gold_oa','currently_publishing'])
			df['not_currently_publishing'] = ~df['currently_publishing']
			remainder = df[~(df['gold_oa'] | df['not_currently_publishing'])]

			with get_db_cursor() as cursor:
				cmd = "select * from jump_journal_prices where package_id = %s and issn_l in %s"
				cursor.execute(cmd, (self.package_id, tuple(remainder['issn_l'].to_list()), ))
				rows_prices = cursor.fetchall()

			price_not_available = remainder[~remainder['issn_l'].isin([x[3] for x in rows_prices])]
			price_not_available = price_not_available.drop(["gold_oa","publisher","currently_publishing","not_currently_publishing"], axis=1)
			price_not_available['price_not_available'] = True
			df2 = df.merge(price_not_available, how = "left", on = "issn_l")
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
