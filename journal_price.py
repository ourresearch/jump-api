# coding: utf-8

from cached_property import cached_property
from app import db
from package_input import PackageInput
from scenario import refresh_cached_prices_from_db


class JournalPrice(db.Model):
    __tablename__ = "jump_journal_prices"

    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    publisher = db.Column(db.Text)
    title = db.Column(db.Text)
    issn_l = db.Column(db.Text, primary_key=True)
    subject = db.Column(db.Text)
    price = db.Column(db.Numeric)
    year = db.Column(db.Numeric)

    def to_dict(self):
        return {
            "package_id": self.package_id,
            "publisher": self.publisher,
            "title": self.title,
            "issn_l": self.issn_l,
            "subject": self.subject,
            "price": self.price,
            "year": self.year,
        }


class JournalPriceInput(db.Model, PackageInput):
    __tablename__ = "jump_journal_prices_input"

    publisher = db.Column(db.Text)
    issn = db.Column(db.Text, primary_key=True)
    subject = db.Column(db.Text)
    price = db.Column(db.Numeric)
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    year = db.Column(db.Numeric)

    def import_view_name(self):
        return "jump_journal_prices_view"

    def destination_table(self):
        return JournalPrice.__tablename__

    def issn_columns(self):
        return ["issn"]

    def csv_columns(self):
        return {
            "publisher": {
                "normalize": self.strip_text,
                "name_snippets": [u"publisher"],
                "required": False,
            },
            "issn": {
                "normalize": self.normalize_issn,
                "name_snippets": [u"issn"],
                "required": True,
                "warn_if_blank": True,
            },
            "subject": {
                "normalize": self.strip_text,
                "name_snippets": [u"subj"],
                "required": False,
            },
            "price": {
                "normalize": self.normalize_price,
                "name_snippets": [u"price", u"usd", u"cost"],
                "warn_if_blank": True,
            },
            "year": {
                "normalize": self.normalize_year,
                "name_snippets": [u"year", u"date"],
                "required": False,
            }
        }

    def file_type_label(self):
        return u"price"

    def clear_caches(self, my_package):
        super(JournalPriceInput, self).clear_caches(my_package)
        refresh_cached_prices_from_db(my_package.package_id, my_package.publisher)

    def validate_publisher(self):
        return True
