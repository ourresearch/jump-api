import datetime
import unittest

from nose.tools import assert_equals, assert_raises_regexp

# TODO: separate file input table classes from ingestion logic to remove import cycles
import package
from package_input import PackageInput, ParseWarning
from util import write_to_tempfile


class TestPackageInput(unittest.TestCase):
    def setUp(self):
        self.maxDiff = 5000

    def test_normalize_issn(self):
        assert_equals(PackageInput.normalize_issn('1234-5678'), '1234-5678')
        assert_equals(PackageInput.normalize_issn('1234-567X'), '1234-567X')
        assert_equals(PackageInput.normalize_issn('1234-567x'), '1234-567X')
        assert_equals(PackageInput.normalize_issn('\n1234-567X\t'), '1234-567X')
        assert_equals(PackageInput.normalize_issn('\n1234- 567X\t'), '1234-567X')
        assert_equals(PackageInput.normalize_issn('\n1234-\n567x\t'), '1234-567X')

        invalid_issns = [
            {'issn': '1234-56789',              'warning': ParseWarning.bad_issn},
            {'issn': '1234-56XX',               'warning': ParseWarning.bad_issn},
            {'issn': 'print: \nfs34-\n123x\t',  'warning': ParseWarning.bad_issn},
            {'issn': '\nfs34-\n5123x\t',        'warning': ParseWarning.bad_issn},
            {'issn': 'RT34-\n123x\t',           'warning': ParseWarning.bundle_issn},
            {'issn': 'FS34-\n123y\t',           'warning': ParseWarning.bad_issn},
        ]

        for i in invalid_issns:
            assert_equals(PackageInput.normalize_issn(i['issn']), i['warning'])

        assert_equals(PackageInput.normalize_issn(''), None)
        assert_equals(PackageInput.normalize_issn('', warn_if_blank=True), ParseWarning.bad_issn)

    def test_normalize_date(self):
        assert_equals(PackageInput.normalize_date('1955-11-05'), datetime.datetime(1955, 11, 5).isoformat())
        assert_equals(PackageInput.normalize_date('October 26 1985'), datetime.datetime(1985, 10, 26).isoformat())
        assert_equals(PackageInput.normalize_date('21 Oct 2015'), datetime.datetime(2015, 10, 21).isoformat())

        assert_equals(PackageInput.normalize_date('the other day'), ParseWarning.bad_date)
        assert_equals(PackageInput.normalize_date('spring'), ParseWarning.bad_date)

        assert_equals(PackageInput.normalize_date(''), None)
        assert_equals(PackageInput.normalize_date('', warn_if_blank=True), ParseWarning.bad_date)

    def test_normalize_year(self):
        assert_equals(PackageInput.normalize_year('1955-11-05'), 1955)
        assert_equals(PackageInput.normalize_year('October 1985'), 1985)
        assert_equals(PackageInput.normalize_year('2015'), 2015)

        assert_equals(PackageInput.normalize_year('the other day'), ParseWarning.bad_year)
        assert_equals(PackageInput.normalize_year('spring'), ParseWarning.bad_year)

        assert_equals(PackageInput.normalize_year(''), None)
        assert_equals(PackageInput.normalize_year('', warn_if_blank=True), ParseWarning.bad_year)

    def test_normalize_int(self):
        assert_equals(PackageInput.normalize_int('9000   '), 9000)
        assert_equals(PackageInput.normalize_int('  1917'), 1917)
        assert_equals(PackageInput.normalize_int('many'), ParseWarning.bad_int)
        assert_equals(PackageInput.normalize_int('lots'), ParseWarning.bad_int)

        assert_equals(PackageInput.normalize_int(''), None)
        assert_equals(PackageInput.normalize_int('', warn_if_blank=True), ParseWarning.bad_int)

    def test_normalize_price(self):
        assert_equals(PackageInput.normalize_price('$1,000,000.49'), 1000000)
        assert_equals(PackageInput.normalize_price('$1,000,000'), 1000000)
        assert_equals(PackageInput.normalize_price('$1000000'), 1000000)
        assert_equals(PackageInput.normalize_price('$1.000,49 '), 1000)
        assert_equals(PackageInput.normalize_price('1.000'), 1000)
        assert_equals(PackageInput.normalize_price('   $100,51'), 101)

        assert_equals(PackageInput.normalize_price('$1.1.1'), ParseWarning.bad_usd_price)
        assert_equals(PackageInput.normalize_price('$$$'), ParseWarning.bad_usd_price)

        assert_equals(PackageInput.normalize_price(''), None)
        assert_equals(PackageInput.normalize_price('', warn_if_blank=True), ParseWarning.bad_usd_price)

    def test_normalize_rows(self):
        test_file = write_to_tempfile("""
            int,issn,price
            5,1234-5668,$500
            10, 9876-5432, 123.45
            15, 4331-997X, 1000000
        """.strip())

        rows, warnings = TestInputFormat.normalize_rows(test_file)

        self.assertItemsEqual([
            {'int': 5, 'issn': u'1234-5668', 'price': 500},
            {'int': 10, 'issn': u'9876-5432', 'price': 123},
            {'int': 15, 'issn': u'4331-997X', 'price': 1000000},
        ], rows)

        self.assertItemsEqual([], warnings)

    def test_required_field(self):
        test_file = write_to_tempfile("""
            INT,Price!
            5,50
            6,100.00
        """.strip())

        assert_raises_regexp(
            RuntimeError,
            ur'Missing required columns\. Expected \[int, issn\] but found int \(from input column INT\), price \(from input column Price!\)\.',
            lambda: TestInputFormat.normalize_rows(test_file)
        )

        test_file = write_to_tempfile("""
            int,issn
            5,1234-5678
            6,9999-999X
        """.strip())

        rows, warnings = TestInputFormat.normalize_rows(test_file)

        self.assertItemsEqual([
            {'int': 5, 'issn': u'1234-5678'},
            {'int': 6, 'issn': u'9999-999X'},
        ], rows)

        self.assertItemsEqual([], warnings)

    def test_warn_invalid_fields(self):
        test_file = write_to_tempfile("""
            int,price,issn
            5,$100.00,1234-5678
            6,a few bucks,4444-444X
            7,555,
            8,500,FS66-6666
            9,,3333-3333
        """.strip())

        rows, warnings = TestInputFormat.normalize_rows(test_file)

        self.assertItemsEqual([
            {'int': 5, 'price': 100, 'issn': u'1234-5678'},
            {'int': 9, 'price': None, 'issn': '3333-3333'},
        ], rows)

        self.assertItemsEqual([
            {'message': 'Unrecognized USD format.', 'raw_value': u'a few bucks', 'row_no': 2, 'column_name': 'price','label': 'bad_usd_price', 'file': 'test_input'},
            {'message': 'Invalid ISSN format.', 'raw_value': u'', 'row_no': 3, 'column_name': 'issn', 'label': 'bad_issn', 'file': 'test_input'},
            {'message': 'ISSN represents a bundle of journals, not a single journal.', 'raw_value': u'FS66-6666', 'row_no': 4, 'column_name': 'issn', 'label': 'bundle_issn', 'file': 'test_input'},
        ], warnings)

    def test_excluded_name_snippet(self):
        class PickyInputFormat(TestInputFormat):
            @classmethod
            def csv_columns(cls):
                return {
                    'picky_column': {
                        'normalize': cls.normalize_issn,
                        'name_snippets': [u'column'],
                        'excluded_name_snippets': [u'excluded'],
                        'required': True,
                        'warn_if_blank': True,
                    },
                }

        self.assertIsNone(PickyInputFormat.normalize_column_name('excluded column'))
        self.assertEqual(PickyInputFormat.normalize_column_name('column'), 'picky_column')


class TestInputFormat(PackageInput):
    @classmethod
    def import_view_name(cls):
        raise NotImplementedError()

    @classmethod
    def destination_table(cls):
        raise NotImplementedError()

    @classmethod
    def file_type_label(cls):
        return 'test_input'

    @classmethod
    def csv_columns(cls):
        return {
            'int': {
                'normalize': cls.normalize_int,
                'name_snippets': [u'int'],
                'required': True
            },
            'issn': {
                'normalize': cls.normalize_issn,
                'name_snippets': [u'issn'],
                'required': True,
                'warn_if_blank': True,
            },
            'price': {
                'normalize': cls.normalize_price,
                'name_snippets': [u'price'],
                'required': False
            },
        }
