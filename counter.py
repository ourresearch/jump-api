import re

from app import db, logger
from package_input import PackageInput

class Counter(db.Model):
    __tablename__ = 'jump_counter'
    issn_l = db.Column(db.Text, primary_key=True)
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    journal_name = db.Column(db.Text)
    total = db.Column(db.Numeric)

    def to_dict(self):
        return {
            'issn_l': self.issn_l,
            'package_id': self.package_id,
            'journal_name': self.journal_name,
            'total': self.total,
        }


class CounterInput(db.Model, PackageInput):
    __tablename__ = 'jump_counter_input'
    report_version = db.Column(db.Text)
    report_name = db.Column(db.Text)
    report_year = db.Column(db.Numeric)
    metric_type = db.Column(db.Text)
    yop = db.Column(db.Numeric)
    access_type = db.Column(db.Text)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)
    issn = db.Column(db.Text, primary_key=True)
    journal_name = db.Column(db.Text)
    total = db.Column(db.Numeric)
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)

    @classmethod
    def import_view_name(cls):
        return 'jump_counter_view'

    @classmethod
    def destination_table(cls):
        return Counter.__tablename__

    @classmethod
    def csv_columns(cls):
        return {
            'print_issn': {
                'normalize': cls.normalize_issn,
                'name_snippets': [u'print issn', u'print_issn', u'issn'],
                'excluded_name_snippets': [u'online', u'e-', u'eissn'],
                'warn_if_blank': True,
            },
            'online_issn': {
                'normalize': cls.normalize_issn,
                'name_snippets': [u'online issn', u'online_issn', u'eissn'],
                'exact_name': True,
                'required': False,
                'warn_if_blank': True,
            },
            'total': {
                'normalize': cls.normalize_int,
                'name_snippets': [u'total'],
                'warn_if_blank': True,
                'required': True,
            },
            'journal_name': {
                'normalize': cls.strip_text,
                'name_snippets': [u'title', u'journal', u'journal_name'],
                'exact_name': True,
                'required': False,
            },
            'metric_type': {
                'normalize': cls.strip_text,
                'name_snippets': [u'metric_type'],
                'exact_name': True,
                'required': False,
            },
            'yop': {
                'normalize': cls.normalize_int,
                'name_snippets': [u'yop'],
                'exact_name': True,
                'required': False,
            },
            'access_type': {
                'normalize': cls.strip_text,
                'name_snippets': [u'access_type'],
                'exact_name': True,
                'required': False,
            },
        }

    @classmethod
    def ignore_row(cls, row):
        journal_name = (row.get('journal_name', u'') or u'').lower()
        if (not journal_name or u'all journals' in journal_name) and row.get('print_issn', None) is None:
            return True

        return False

    @classmethod
    def file_type_label(cls):
        return u'counter'

    @classmethod
    def issn_columns(cls):
        return ['print_issn', 'online_issn']

    @classmethod
    def apply_header(cls, normalized_rows, header_rows):
        # get the counter version and file format
        version_labels = {
            'Journal Report 1 (R4)': {
                'report_version': '4',
                'report_name': 'JR1'
            },
            'TR_J1': {
                'report_version': '5',
                'report_name': 'TRJ1',
            },
            'TR_J2': {
                'report_version': '5',
                'report_name': 'TRJ2',
            },
            'TR_J3': {
                'report_version': '5',
                'report_name': 'TRJ3',
            },
            'TR_J4': {
                'report_version': '5',
                'report_name': 'TRJ4',
            },
        }

        report_version = None
        report_name = None

        normalized_header_text = u''.join([re.sub(ur'\s*', u'', u''.join(row)).lower() for row in header_rows])
        for label, values in version_labels.items():
            normalized_label = re.sub(ur'\s*', '', label).lower()
            if normalized_label in normalized_header_text:
                report_version = values['report_version']
                report_name = values['report_name']
                break

        possible_versions = [v['report_version'] for v in version_labels.values()]
        if report_version not in possible_versions:
            logger.warn(u"Got {} as report version, expected one of {}.".format(report_version, possible_versions))

        possible_names = [v['report_name'] for v in version_labels.values()]
        if report_name not in possible_names:
            logger.warn(u"Got {} as report name, expected one of {}.".format(report_name, possible_names))

        print u"Detected report {} {}".format(report_version, report_name)

        # get the year
        # get the header rows that look like months
        # Mar/18, 2017-12-01 00:00:00
        header_years = []
        for cell in header_rows[-1]:
            matches = re.findall(ur'\b(\d{4})\b', cell)
            if len(matches) == 1:
                header_years.append(int(matches[0]))
            else:
                matches = re.findall(ur'\b(\d{2})\b', cell)
                if len(matches) == 1:
                    header_years.append(2000 + int(matches[0]))

        # sort them and take the one in the middle to be the year
        report_year = sorted(header_years)[len(header_years)/2] if header_years else None

        if report_year is None:
            logger.warn(u"Couldn't guess a year from column headers: {}".format(header_rows[-1]))

        for row in normalized_rows:
            row['report_year'] = report_year
            row['report_version'] = report_version
            row['report_name'] = report_name

        # check for COUNTER 5
        # cop5_error = u"Sorry, we don't support COUNTER 5 yet. Please upload a COUNTER 4 JR_1 file."
        #
        # for header_row in header_rows:
        #     row_text = u'|'.join([cell.strip() for cell in header_row]).lower()
        #     if u'report_id|tr_j1' in row_text or u'release|5' in row_text:
        #         raise RuntimeError(cop5_error)

        return normalized_rows
