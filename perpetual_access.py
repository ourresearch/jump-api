# coding: utf-8

from cached_property import cached_property
from datetime import datetime
from collections import OrderedDict

from app import db
from package_input import PackageInput
from scenario import refresh_perpetual_access_from_db


class PerpetualAccess(db.Model):
    __tablename__ = "jump_perpetual_access"
    issn_l = db.Column(db.Text, primary_key=True)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)

    @cached_property
    def journal_metadata(self):
        from journalsdb import get_journal_metadata
        return get_journal_metadata(self.issn_l)

    @cached_property
    def issns(self):
        return self.journal_metadata.issns

    @cached_property
    def display_issns(self):
        return self.journal_metadata.display_issns

    @cached_property
    def display_issn_l(self):
        return self.journal_metadata.display_issn_l

    @cached_property
    def title(self):
        return self.journal_metadata.title

    @cached_property
    def publisher(self):
        return self.journal_metadata.publisher

    @cached_property
    def display_start_date(self):
        if not self.start_date:
            return None
        return self.start_date.isoformat()[0:10]

    @cached_property
    def display_end_date(self):
        if not self.end_date:
            return None
        return self.end_date.isoformat()[0:10]

    def to_dict(self):
        return OrderedDict([
            ("issn_l_prefixed", self.display_issn_l),
            ("issn_l", self.issn_l),
            ("issns", self.display_issns),
            ("title", self.title),
            ("publisher", self.publisher),
            ("start_date", self.display_start_date),
            ("end_date", self.display_end_date),
        ])


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
        super(PerpetualAccessInput, self).clear_caches(my_package)
        refresh_perpetual_access_from_db(my_package.package_id)
