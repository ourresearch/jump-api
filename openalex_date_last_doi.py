from app import db
from datetime import datetime

class OpenalexDateLastDOI(db.Model):
	__tablename__ = "openalex_date_last_doi"
	created = db.Column(db.DateTime)
	issn_l = db.Column(db.Text, primary_key=True)
	date_last_doi = db.Column(db.Text)

	def __init__(self, journal):
		self.created = datetime.utcnow().isoformat()
		for attr in ("issn_l", "date_last_doi"):
			setattr(self, attr, getattr(journal, attr))
		super(OpenalexDateLastDOI, self).__init__()

	def get_values(self):
		return (
			self.created,
			self.issn_l,
			self.date_last_doi,)

	@classmethod
	def get_insert_column_names(cls):
		return ["created",
				"issn_l",
				"date_last_doi",]

	def __repr__(self):
		return "<{} ({}) '{}'>".format(self.__class__.__name__, self.issn_l, self.date_last_doi)
