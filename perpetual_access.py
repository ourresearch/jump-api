from app import db
from package_input import PackageInput
from scenario import refresh_perpetual_access_from_db

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
            'start_date': self.start_date and self.start_date.isoformat(),
            'end_date': self.end_date and self.end_date.isoformat(),
        }


class PerpetualAccessInput(db.Model, PackageInput):
    __tablename__ = 'jump_perpetual_access_input'
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    issn = db.Column(db.Text, primary_key=True)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)

    @classmethod
    def import_view_name(cls):
        return 'jump_perpetual_access_view'

    @classmethod
    def destination_table(cls):
        return PerpetualAccess.__tablename__

    @classmethod
    def translate_row(cls, row):
        return [row] if row['issn'] else []

    @classmethod
    def csv_columns(cls):
        return {
            'start_date': {
                'normalize': cls.normalize_date,
                'name_snippets': [u'start', u'begin'],
                'required': True
            },
            'end_date': {
                'normalize': cls.normalize_date,
                'name_snippets': [u'end'],
                'required': True
            },
            'issn': {
                'normalize': cls.normalize_issn,
                'name_snippets': [u'issn'],
                'required': True
            }
        }

    @classmethod
    def clear_caches(cls, my_package):
        super(PerpetualAccessInput, cls).clear_caches(my_package)
        refresh_perpetual_access_from_db(my_package.package_id)
