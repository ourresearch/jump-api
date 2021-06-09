# coding: utf-8

from cached_property import cached_property
from collections import OrderedDict

from app import db
from package_input import PackageInput


class JournalPrice(db.Model):
    __tablename__ = "jump_journal_prices"

    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    issn_l = db.Column(db.Text, primary_key=True)
    subject = db.Column(db.Text)
    price = db.Column(db.Numeric)
    year = db.Column(db.Numeric)

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

    def to_dict(self):
        return OrderedDict([
            ("issn_l_prefixed", self.display_issn_l),
            ("issn_l", self.issn_l),
            ("issns", self.display_issns),
            ("title", self.title),
            ("publisher", self.publisher),
            ("price", self.price)
        ])


class JournalPriceInput(db.Model, PackageInput):
    __tablename__ = "jump_journal_prices_input"
    issn = db.Column(db.Text, primary_key=True)
    price = db.Column(db.Numeric)
    year = db.Column(db.Numeric)
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)

    def import_view_name(self):
        return "jump_journal_prices_view"

    def destination_table(self):
        return JournalPrice.__tablename__

    def issn_columns(self):
        return ["issn"]

    def csv_columns(self):
        return {
            "issn": {
                "normalize": self.normalize_issn,
                "name_snippets": [u"issn"],
                "required": True,
                "warn_if_blank": True,
            },
            "price": {
                "normalize": self.normalize_price,
                "name_snippets": [u"price", u"usd", u"cost"],
                "warn_if_blank": True,
            },
            "year": {
                "normalize": self.normalize_year,
                "name_snippets": [u"year"],
                "required": False,
            }
        }

    def file_type_label(self):
        return u"price"

    def clear_caches(self, my_package):
        super(JournalPriceInput, self).clear_caches(my_package)

    def validate_publisher(self):
        return True
