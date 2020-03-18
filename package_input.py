import csv
import re

import dateutil.parser

from app import db
from excel import convert_spreadsheet_to_csv
from util import safe_commit
from re import sub


class PackageInput:
    @staticmethod
    def normalize_date(date_str):
        if date_str:
            try:
                return dateutil.parser.parse(date_str).isoformat()
            except Exception:
                raise ValueError(u"unrecognized datetime format")
        else:
            return None

    @staticmethod
    def normalize_year(year):
        if year:
            try:
                return dateutil.parser.parse(year).year
            except Exception:
                raise ValueError(u"unrecognized year format")
        else:
            return None

    @staticmethod
    def normalize_price(price):
        if price:
            try:
                decimal = u',' if re.search(ur'\.\d{3}', price) or re.search(ur',\d{2}$', price) else ur'\.'
                sub_pattern = ur'[^\d{}]'.format(decimal)
                price = sub(sub_pattern, '', price)
                price = sub(',', '.', price)
                return round(float(price))
            except Exception:
                raise ValueError(u"unrecognized price format")
        else:
            return None

    @staticmethod
    def normalize_issn(issn):
        if issn and re.search('[0-9X]{4}-[0-9X]{4}', issn.strip().upper()):
            return issn.strip().upper()
        else:
            raise ValueError(u'invalid ISSN format')

    @classmethod
    def csv_columns(cls):
        raise NotImplementedError()

    @classmethod
    def normalize_column(cls, column_name, column_value):
        for canonical_name, spec in cls.csv_columns().items():
            for snippet in spec['name_snippets']:
                if snippet in column_name:
                    return {canonical_name: spec['normalize'](column_value)}

        raise ValueError(u'unexpected column {}, possible values are {}'.format(
            column_name, ', '.join(cls.csv_columns().keys())
        ))

    @classmethod
    def load(cls, package_id, file_name):
        if file_name.endswith(u'.xls') or file_name.endswith(u'.xlsx'):
            file_name = convert_spreadsheet_to_csv(file_name)

        with open(file_name, 'r') as csv_file:
            dialect = csv.Sniffer().sniff(csv_file.read(1024))

            csv_file.seek(0)
            reader = csv.DictReader(csv_file, dialect=dialect)

            rows = []
            row_no = 1
            for row in reader:
                normalized_row = {}

                for column_name in row.keys():
                    try:
                        normalized_row.update(cls.normalize_column(column_name, row[column_name]))
                    except Exception as e:
                        return False, u'Error reading row {}: {} for {}'.format(
                            row_no, e.message, unicode(row[column_name], 'utf-8')
                        )

                row_keys = sorted(normalized_row.keys())
                expected_keys = sorted(cls.csv_columns().keys())

                if set(row_keys).symmetric_difference(set(expected_keys)):
                    return False, u'Missing expected columns. Expected {} but got {}.'.format(
                        ', '.join(expected_keys),
                        ', '.join(normalized_row)
                    )

                normalized_row.update({'package_id': package_id})
                rows.append(normalized_row)
                row_no += 1

        if package_id == 'BwfVyRm9':
            try:
                db.session.query(cls).filter(cls.package_id == package_id).delete()
                db.session.bulk_save_objects([cls(**row) for row in rows])
                safe_commit(db)
            except Exception as e:
                return False, e.message

            return True, u'Inserted {} {} rows for package {}.'.format(len(rows), cls.__name__, package_id)
        else:
            return True, u'Simulated inserting {} {} rows for package {}.'.format(len(rows), cls.__name__, package_id)
