from app import db
from package_input import PackageInput


class Counter(db.Model):
    __tablename__ = 'jump_counter'
    issn_l = db.Column(db.Text, primary_key=True)
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    organization = db.Column(db.Text)
    publisher = db.Column(db.Text)
    issn = db.Column(db.Text, primary_key=True)
    journal_name = db.Column(db.Text)
    total = db.Column(db.Numeric)

    def to_dict(self):
        return {
            'issn_l': self.issn_l,
            'package_id': self.package_id,
            'organization': self.organization,
            'publisher': self.publisher,
            'issn': self.issn,
            'journal_name': self.journal_name,
            'total': self.total,
        }


class CounterInput(db.Model, PackageInput):
    __tablename__ = 'jump_counter_input'
    organization = db.Column(db.Text)
    publisher = db.Column(db.Text)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)
    issn = db.Column(db.Text, primary_key=True)
    journal_name = db.Column(db.Text)
    total = db.Column(db.Numeric)
    age_0y = db.Column(db.Numeric)
    age_1y = db.Column(db.Numeric)
    age_2y = db.Column(db.Numeric)
    age_3y = db.Column(db.Numeric)
    age_4y = db.Column(db.Numeric)
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)

    @classmethod
    def import_view_name(cls):
        return 'jump_counter_view_flat'

    @classmethod
    def destination_table(cls):
        return Counter.__tablename__

    @classmethod
    def csv_columns(cls):
        return {
            'publisher': {
                'normalize': lambda x: x,
                'name_snippets': [u'publisher'],
            },
            'print_issn': {
                'normalize': cls.normalize_issn,
                'name_snippets': [u'print issn', 'print_issn', 'issn'],
            },
            'online_issn': {
                'normalize': cls.normalize_issn,
                'name_snippets': [u'online issn'],
                'required': False,
            },
            'total': {
                'normalize': cls.normalize_int,
                'name_snippets': [u'total'],
            },
            'journal_name': {
                'normalize': lambda x: x,
                'name_snippets': [u'title', 'journal', 'journal_name'],
                'exact_name': True,
            },
        }

    @classmethod
    def ignore_row(cls, row):
        if u'all journals' in row.get('journal_name', u'').lower() and row.get('print_issn', None) is None:
            return True

        return False

    @classmethod
    def translate_row(cls, row):
        rows = []
        if row['print_issn']:
            rows.append({
                    'publisher': row['publisher'],
                    'issn': row['print_issn'],
                    'total': row['total'],
                    'journal_name': row['journal_name'],
            })
        else:
            if row["journal_name"] != u"Total":
                print u"warning: no print_issn in this row: {}".format(row)


        # don't add these, or else they add duplicates
        # if row['online_issn']:
        #     rows.append({
        #         'publisher': row['publisher'],
        #         'issn': row['online_issn'],
        #         'total': row['total'],
        #         'journal_name': row['journal_name'],
        #     })

        return rows
