from app import db
from datetime import datetime

class OpenalexDateLastDOI(db.Model):
	__tablename__ = "openalex_date_last_doi"
	updated = db.Column(db.DateTime)
	issn_l = db.Column(db.Text, primary_key=True)
	date_last_doi = db.Column(db.Text)

	def __init__(self, journal):
		self.updated = datetime.utcnow().isoformat()
		for attr in ("issn_l", "date_last_doi"):
			setattr(self, attr, getattr(journal, attr))
		super(OpenalexDateLastDOI, self).__init__()

	def get_values(self):
		return (
			self.updated,
			self.issn_l,
			self.date_last_doi,)

	@classmethod
	def get_insert_column_names(cls):
		return ["updated",
				"issn_l",
				"date_last_doi",]

	def __repr__(self):
		return "<{} ({}) '{}'>".format(self.__class__.__name__, self.issn_l, self.date_last_doi)

class OpenalexDateLastDOIFromOA(db.Model):
	__tablename__ = "openalex_date_last_doi_from_oa"
	updated = db.Column(db.DateTime)
	issn_l = db.Column(db.Text, primary_key=True)
	openalex_id = db.Column(db.Text)
	date_last_doi = db.Column(db.Text)

	def __init__(self, journal):
		self.updated = datetime.utcnow().isoformat()
		for attr in ("issn_l", "date_last_doi"):
			setattr(self, attr, getattr(journal, attr))
		super(OpenalexDateLastDOIFromOA, self).__init__()

	def get_values(self):
		return (
			self.updated,
			self.issn_l,
			self.openalex_id,
			self.date_last_doi,)

	@classmethod
	def get_insert_column_names(cls):
		return ["updated",
				"issn_l",
				"openalex_id",
				"date_last_doi",]

	def __repr__(self):
		return "<{} ({}) '{}'>".format(self.__class__.__name__, self.issn_l, self.date_last_doi)

