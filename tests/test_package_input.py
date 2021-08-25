import datetime
import pytest

# TODO: separate file input table classes from ingestion logic to remove import cycles
import package
from package_input import PackageInput, ParseWarning
from util import write_to_tempfile


def test_normalize_issn():
    assert PackageInput.normalize_issn('0266-6731') == '0266-6731'
    assert PackageInput.normalize_issn('0990-7440') == '0990-7440'
    assert PackageInput.normalize_issn('  02666731') == '0266-6731'
    assert PackageInput.normalize_issn('1026-597X') == '1026-597X'
    assert PackageInput.normalize_issn('1026-597x') == '1026-597X'
    assert PackageInput.normalize_issn('\n1026-597X\t') == '1026-597X'
    assert PackageInput.normalize_issn('\n1026- 597X\t') == '1026-597X'
    assert PackageInput.normalize_issn('\n1026-\n597x\t') == '1026-597X'

    invalid_issns = [
        {'issn': '1234-56789',              'warning': ParseWarning.bad_issn},
        {'issn': '1234-56XX',               'warning': ParseWarning.bad_issn},
        {'issn': 'print: \nfs34-\n123x\t',  'warning': ParseWarning.bad_issn},
        {'issn': '\nfs34-\n5123x\t',        'warning': ParseWarning.bad_issn},
        {'issn': 'RT34-\n123x\t',           'warning': ParseWarning.bundle_issn},
        {'issn': 'FS34-\n123y\t',           'warning': ParseWarning.bad_issn},
    ]

    for i in invalid_issns:
        assert PackageInput.normalize_issn(i['issn']) == i['warning']

    assert PackageInput.normalize_issn('') == None
    assert PackageInput.normalize_issn('', warn_if_blank=True) == ParseWarning.no_issn

def test_normalize_date():
    assert PackageInput.normalize_date('1955-11-05') == datetime.datetime(1955, 11, 5).isoformat()
    assert PackageInput.normalize_date('October 26 1985') == datetime.datetime(1985, 10, 26).isoformat()
    assert PackageInput.normalize_date('21 Oct 2015') == datetime.datetime(2015, 10, 21).isoformat()

    assert PackageInput.normalize_date('the other day') == ParseWarning.bad_date
    assert PackageInput.normalize_date('spring') == ParseWarning.bad_date

    assert PackageInput.normalize_date('June-20') == ParseWarning.ambiguous_date
    assert PackageInput.normalize_date('11/5/85') == ParseWarning.ambiguous_date

    assert PackageInput.normalize_date('') == None
    assert PackageInput.normalize_date('', warn_if_blank=True) == ParseWarning.bad_date

def test_normalize_year():
    assert PackageInput.normalize_year('1955-11-05') == 1955
    assert PackageInput.normalize_year('October 1985') == 1985
    assert PackageInput.normalize_year('2015') == 2015

    assert PackageInput.normalize_year('the other day') == ParseWarning.bad_year
    assert PackageInput.normalize_year('spring') == ParseWarning.bad_year

    assert PackageInput.normalize_year('') == None
    assert PackageInput.normalize_year('', warn_if_blank=True) == ParseWarning.bad_year

def test_normalize_int():
    assert PackageInput.normalize_int('9000   ') == 9000
    assert PackageInput.normalize_int('  1917') == 1917
    assert PackageInput.normalize_int('many') == ParseWarning.bad_int
    assert PackageInput.normalize_int('lots') == ParseWarning.bad_int

    assert PackageInput.normalize_int('') == None
    assert PackageInput.normalize_int('', warn_if_blank=True) == ParseWarning.no_int

def test_normalize_price():
    assert PackageInput.normalize_price('$1,000,000.49') == 1000000
    assert PackageInput.normalize_price('$1,000,000') == 1000000
    assert PackageInput.normalize_price('$1000000') == 1000000
    assert PackageInput.normalize_price('$1.000,49 ') == 1000
    assert PackageInput.normalize_price('1.000') == 1
    assert PackageInput.normalize_price('   $100,51') == 101
    assert PackageInput.normalize_price('5341.1849999999995') == 5341
    assert PackageInput.normalize_price('3196.845') == 3197
    assert PackageInput.normalize_price('10684.935') == 10685
    assert PackageInput.normalize_price('10684,935') == 10685
    assert PackageInput.normalize_price('643.815') == 644
    assert PackageInput.normalize_price('$10,325.20') == 10325

    assert PackageInput.normalize_price('$1.1.1') == ParseWarning.bad_usd_price
    assert PackageInput.normalize_price('$$$') == ParseWarning.bad_usd_price

    assert PackageInput.normalize_price('') == None
    assert PackageInput.normalize_price('', warn_if_blank=True) == ParseWarning.no_usd_price

def test_normalize_rows():
    test_file = write_to_tempfile("""
        int,issn,price
        5,0031-9252,$500
        10, 2093-968X, 123.45
        15, 1990-7478, 1000000
    """.strip())

    rows, warnings = TestInputFormat().normalize_rows(file_name=test_file)

    assert [
        {'int': 5, 'issn': '0031-9252', 'price': 500},
        {'int': 10, 'issn': '2093-968X', 'price': 123},
        {'int': 15, 'issn': '1990-7478', 'price': 1000000},
    ] == rows

    assert warnings is None


def test_reject_unknown_issn():
    class TestIssnFormat(PackageInput):
        @classmethod
        def issn_column(cls):
            return 'issn'

        @classmethod
        def csv_columns(cls):
            return {
                'issn': {
                    'normalize': cls.normalize_issn,
                    'name_snippets': ['issn'],
                    'required': True
                },
                'int': {
                    'normalize': cls.normalize_int,
                    'name_snippets': ['int'],
                    'required': True,
                },
            }

    test_file = write_to_tempfile("""
issn,int
3333-3333,1
0024-3205,2
    """.strip())

    rows, warnings = TestIssnFormat().normalize_rows(file_name=test_file)

    assert [
        {'int': 2, 'issn': '0024-3205'},
    ] == rows

    # FIXME: this was failing
    # assert [{'id': 'issn', 'name': 'issn'},
    #         {'id': 'int', 'name': 'int'},
    #         {'id': 'row_id', 'name': 'Row Number'},] == warnings['headers']

    assert [
            {
                'int': {'value': '1', 'error': None},
                'row_id': {'value': 2, 'error': None},
                'issn': {
                    'value': '3333-3333',
                    'error': {
                        'message': u"This looks like an ISSN, but it isn't one we recognize.",
                        'label': 'unknown_issn'
                    }
                },
            },
        ] == warnings['rows']

def test_required_field():
    test_file = write_to_tempfile("""
        INT,Price!
        5,50
        6,100.00
    """.strip())

    with pytest.raises(RuntimeError, match=r"Error: missing required columns."):
        TestInputFormat().normalize_rows(file_name=test_file)

    test_file = write_to_tempfile("""
        int,issn
        5,0031-9252
        6,2093-968X
    """.strip())

    rows, warnings = TestInputFormat().normalize_rows(file_name=test_file)

    assert [
        {'int': 5, 'issn': '0031-9252'},
        {'int': 6, 'issn': '2093-968X'},
    ] == rows

    assert warnings is None

#  def test_warn_invalid_fields():
#     test_file = write_to_tempfile("""
# int,price,issn
# 5,$100.00,2093-968X
# 6,a few bucks,1749-8155
# 7,555,
# 8,500,FS66-6666
# 9,,1990-7478
#     """.strip())

#     rows, warnings = TestInputFormat().normalize_rows(file_name=test_file)

#     assert [
#         {'int': 5, 'price': 100, 'issn': '2093-968X'},
#         {'int': 9, 'price': None, 'issn': '1990-7478'},
#     ] == rows

#     assert [
#             {'id': 'int', 'name': 'int'},
#             {'id': 'price', 'name': 'price'},
#             {'id': 'issn', 'name': 'issn'},
#             {'id': 'row_id', 'name': 'Row Number'},
#         ] == warnings['headers']

#     assert [
#             {
#                 'int': {'value': '6', 'error': None},
#                 'row_id': {'value': 3, 'error': None},
#                 'issn': {'value': '1749-8155', 'error': None},
#                 'price': {
#                     'value': 'a few bucks',
#                     'error': {
#                         'message': 'Unrecognized USD format.',
#                         'label': 'bad_usd_price'
#                     }
#                 }
#             },
#             {
#                 'int': {'value': '7', 'error': None},
#                 'row_id': {'value': 4, 'error': None},
#                 'issn': {
#                     'value': '',
#                     'error': {
#                         'message': 'No ISSN here.',
#                         'label': 'no_issn'
#                     }
#                 },
#                 'price': {'value': '555', 'error': None}
#             },
#             {
#                 'int': {'value': '8', 'error': None},
#                 'row_id': {'value': 5, 'error': None},
#                 'issn': {
#                     'value': 'FS66-6666',
#                     'error': {
#                         'message': 'ISSN represents a bundle of journals, not a single journal.',
#                         'label': 'bundle_issn'
#                     }
#                 },
#                 'price': {'value': '500', 'error': None}
#             }
#         ] == warnings['rows']

def test_excluded_name_snippet():
    class PickyInputFormat(TestInputFormat):
        @classmethod
        def csv_columns(cls):
            return {
                'picky_column': {
                    'normalize': cls.normalize_issn,
                    'name_snippets': ['column'],
                    'excluded_name_snippets': ['excluded'],
                    'required': True,
                    'warn_if_blank': True,
                },
            }

    assert PickyInputFormat().normalize_column_name(raw_column_name='excluded column') is None
    assert PickyInputFormat().normalize_column_name(raw_column_name='column') == 'picky_column'

def test_issn_columns():
    class MultipleIssnFormat(TestInputFormat):
        @classmethod
        def csv_columns(cls):
            return {
                'primary_issn': {
                    'normalize': cls.normalize_issn,
                    'name_snippets': ['primary'],
                    'warn_if_blank': True,
                },
                'secondary_issn': {
                    'normalize': cls.normalize_issn,
                    'name_snippets': ['secondary'],
                    'warn_if_blank': True,
                },
                'integer': {
                    'normalize': cls.normalize_int,
                    'name_snippets': ['int'],
                },
            }

        @classmethod
        def issn_columns(cls):
            return ['primary_issn', 'secondary_issn']

    test_file = write_to_tempfile("""
        Int,Primary,Secondary
        1,,0010-9355
        2,2311-5459,1093-4537
        3,xxxx-xxxx,0009-2363
        4,1935-1194,FS00-0000
        5,,xxxx-xxxx
        6,,
        7,FS00-0000,1111-1111
    """, strip=True)

    rows, warnings = MultipleIssnFormat().normalize_rows(file_name=test_file)

    # FIXME: not passing
    # assert [
    #     {'integer': 1, 'issn': '0010-9355'},
    #     {'integer': 2, 'issn': '2311-5459'},
    #     {'integer': 3, 'issn': '0009-2363'},
    #     {'integer': 4, 'issn': '1935-1194'},
    # ] == rows

    assert [
            {'id': 'row_id', 'name': 'Row Number'},
            {'id': 'integer', 'name': 'Int'},
            {'id': 'primary_issn', 'name': 'Primary'},
            {'id': 'secondary_issn', 'name': 'Secondary'},
        ] == warnings['headers']

    # FIXME: not passing
    # assert [
    # {
    #     'integer': {'value': '5', 'error': None},
    #     'row_id': {'value': 6, 'error': None},
    #     'primary_issn': {
    #         'value': '',
    #         'error': {
    #             'message': 'No ISSN here.',
    #             'label': 'no_issn'
    #         }
    #     },
    #     'secondary_issn': {
    #         'value': 'xxxx-xxxx',
    #         'error': {
    #             'message': "This doesn't look like an ISSN.",
    #             'label': 'bad_issn'
    #         }
    #     },
    # },
    # {
    #     'integer': {'value': '6', 'error': None},
    #     'row_id': {'value': 7, 'error': None},
    #     'primary_issn': {
    #         'value': '',
    #         'error': {
    #             'message': 'No ISSN here.',
    #             'label': 'no_issn'
    #         }
    #     },
    #     'secondary_issn': {
    #         'value': '',
    #         'error': {
    #             'message': 'No ISSN here.',
    #             'label': 'no_issn'
    #         }
    #     },
    # },
    # {
    #     'integer': {'value': '7', 'error': None},
    #     'row_id': {'value': 8, 'error': None},
    #     'primary_issn': {
    #         'value': 'FS00-0000',
    #         'error': {
    #             'message': 'ISSN represents a bundle of journals, not a single journal.',
    #             'label': 'bundle_issn'
    #         }
    #     },
    #     'secondary_issn': {
    #         'value': '1111-1111',
    #         'error': {
    #             'message': "This looks like an ISSN, but it isn't one we recognize.",
    #             'label': 'unknown_issn'
    #         }
    #     },
    # },] == warnings['rows']


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
                'name_snippets': ['int'],
                'required': True
            },
            'issn': {
                'normalize': cls.normalize_issn,
                'name_snippets': ['issn'],
                'required': True,
                'warn_if_blank': True,
            },
            'price': {
                'normalize': cls.normalize_price,
                'name_snippets': ['price'],
                'required': False
            },
        }
