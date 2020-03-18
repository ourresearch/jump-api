from app import db
from package_input import PackageInput


class PerpetualAccess(db.Model):
    __tablename__ = 'jump_perpetual_access'
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    issn_l = db.Column(db.Text, primary_key=True)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)

    def to_dict(self):
        return {
            'package_id': self.package_id,
            'issn_l': self.issn_l,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat(),
        }


class PerpetualAccessInput(db.Model, PackageInput):
    __tablename__ = 'jump_perpetual_access_input_by_package'
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    issn = db.Column(db.Text, primary_key=True)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)

    @classmethod
    def csv_columns(cls):
        return {
            'start_date': {
                'normalize': cls.normalize_date,
                'name_snippets': [u'start', u'begin']
            },
            'end_date': {
                'normalize': cls.normalize_date,
                'name_snippets': [u'end']
            },
            'issn': {
                'normalize': cls.normalize_issn,
                'name_snippets': [u'issn']
            }
        }
