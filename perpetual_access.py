# coding: utf-8

from cached_property import cached_property
from datetime import datetime

from app import db
from package_input import PackageInput
from scenario import refresh_perpetual_access_from_db


class PerpetualAccess(db.Model):
    __tablename__ = "jump_perpetual_access"
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    issn_l = db.Column(db.Text, primary_key=True)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)

    def to_dict(self):
        return {
            "package_id": self.package_id,
            "issn_l": self.issn_l,
            "start_date": self.start_date and self.start_date.isoformat(),
            "end_date": self.end_date and self.end_date.isoformat(),
        }


class PerpetualAccessInput(db.Model, PackageInput):
    __tablename__ = "jump_perpetual_access_input"
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    issn = db.Column(db.Text, primary_key=True)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)

    def import_view_name(self):
        return "jump_perpetual_access_view"

    def destination_table(self):
        return PerpetualAccess.__tablename__

    def file_type_label(self):
        return u"perpetual-access"

    def issn_columns(self):
        return ["issn"]

    def csv_columns(self):
        return {
            "start_date": {
                "normalize": lambda date, warn_if_blank=False: self.normalize_date(date, default=datetime(1970, 1, 1), warn_if_blank=warn_if_blank),
                "name_snippets": [u"start", u"begin"],
                "required": True
            },
            "end_date": {
                "normalize": lambda date, warn_if_blank=False: self.normalize_date(date, default=datetime(1970, 12, 31), warn_if_blank=warn_if_blank),
                "name_snippets": [u"end"],
                "required": True
            },
            "issn": {
                "normalize": self.normalize_issn,
                "name_snippets": [u"issn"],
                "excluded_name_snippets": [u"online", u"e-", u"eissn"],
                "required": True,
                "warn_if_blank": True,
            }
        }

    def clear_caches(self, my_package):
        super(PerpetualAccessInput, cls).clear_caches(my_package)
        refresh_perpetual_access_from_db(my_package.package_id)
