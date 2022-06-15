# coding: utf-8

from cached_property import cached_property
from collections import OrderedDict
from datetime import datetime

from app import db
from app import get_db_cursor
from package import Package
from package_input import PackageInput


class FilterTitles(db.Model):
	__tablename__ = "jump_journal_filter"
	package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
	issn_l = db.Column(db.Text)
	type = db.Column(db.Text)
	title = db.Column(db.Text)
	publisher = db.Column(db.Text)

	def to_dict(self):
		return {
			"issn_l": self.issn_l,
			"type": self.type,
			"title": self.title,
			"publisher": self.publisher
		}

class FilterTitlesInput(db.Model, PackageInput):
	__tablename__ = "jump_journal_filter_input"
	created = db.Column(db.DateTime, default=datetime.utcnow)
	issn = db.Column(db.Text, primary_key=True)
	package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
	type = db.Column(db.Text)
	title = db.Column(db.Text)
	publisher = db.Column(db.Text)

	def import_view_name(self):
		return "jump_journal_filter_view"

	def destination_table(self):
		return FilterTitles.__tablename__

	def issn_columns(self):
		return ["print_issn", "online_issn"]

	def csv_columns(self):
		if "online_identifier" not in self.raw_column_names:
			return {
				"issn": {
					"normalize": self.normalize_issn,
					"name_snippets": ["issn", "online_identifier"],
					"required": True,
					"warn_if_blank": True,
				},
			}
		else:
			return {
				"print_issn": {
					"normalize": self.normalize_issn,
					"name_snippets": ["print identifier", "print_identifier"],
					"required": True,
					"warn_if_blank": True,
				},
				"online_issn": {
					"normalize": self.normalize_issn,
					"name_snippets": ["online identifier", "online_identifier"],
					"required": True,
					"warn_if_blank": True,
				},
				"type": {
					"normalize": self.strip_text,
					"name_snippets": ["publication_type"],
					"required": True,
					"warn_if_blank": False,
				},
				"title": {
					"normalize": self.strip_text,
					"name_snippets": ["publication_title"],
					"exact_name": True,
					"required": True,
					"warn_if_blank": False,
				},
				"publisher": {
					"normalize": self.strip_text,
					"name_snippets": ["publisher_name"],
					"required": True,
					"warn_if_blank": False,
				},
			}
	
	def file_type_label(self):
		return "filter"

	def update_subscriptions(self, package_id):
		from saved_scenario import save_raw_scenario_to_db

		pkg = Package.query.get(package_id)
		scenarios = pkg.saved_scenarios
		if len(scenarios) == 0:
			print(f"({self}) no scenarios found, not modifying subscriptions")
			return None

		for scenario in scenarios:
			saved = scenario.to_dict_saved_from_db()
			if saved['subrs']:
				print(f"({self}) modifying subscriptions")
				with get_db_cursor() as cursor:
					qry = "select issn_l from jump_journal_filter where package_id = %s"
					cursor.execute(qry, (package_id,))
					rows = cursor.fetchall()
				filter_issns = [w[0] for w in rows]
				saved["subrs"] = list(filter(lambda x: x in filter_issns, saved["subrs"]))

				save_raw_scenario_to_db(scenario.scenario_id, saved, None)
