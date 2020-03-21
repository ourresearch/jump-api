from app import db
from package_input import PackageInput


class JournalPrice(db.Model):
    __tablename__ = 'jump_journal_prices'

    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    publisher = db.Column(db.Text)
    title = db.Column(db.Text)
    issn_l = db.Column(db.Text, primary_key=True)
    subject = db.Column(db.Text)
    usa_usd = db.Column(db.Numeric)
    year = db.Column(db.Numeric)

    def to_dict(self):
        return {
            'package_id': self.package_id,
            'publisher': self.publisher,
            'title': self.title,
            'issn_l': self.issn_l,
            'subject': self.subject,
            'usa_usd': self.usa_usd,
            'year': self.year,
        }


class JournalPriceInput(db.Model, PackageInput):
    __tablename__ = 'jump_journal_prices_input'

    publisher = db.Column(db.Text)
    issn = db.Column(db.Text, primary_key=True)
    subject = db.Column(db.Text)
    usa_usd = db.Column(db.Numeric)
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    year = db.Column(db.Numeric)

    @classmethod
    def import_view_name(cls):
        return 'jump_journal_prices_view'

    @classmethod
    def destination_table(cls):
        return JournalPrice.__tablename__

    @classmethod
    def csv_columns(cls):
        return {
            'publisher': {
                'normalize': lambda x: x,
                'name_snippets': [u'publisher'],
                'required': False,
            },
            'issn': {
                'normalize': cls.normalize_issn,
                'name_snippets': [u'issn'],
            },
            'subject': {
                'normalize': lambda x: x,
                'name_snippets': [u'subj'],
                'required': False,
            },
            'usa_usd': {
                'normalize': cls.normalize_price,
                'name_snippets': [u'price', u'usd', u'cost'],
            },
            'year': {
                'normalize': cls.normalize_year,
                'name_snippets': [u'year', u'date'],
                'required': False,
            }
        }
