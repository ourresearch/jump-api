import datetime
import unittest

from nose.tools import assert_equals

# TODO: separate file input table classes from ingestion logic to remove import cycles
import package
from package_input import ParseWarning
from perpetual_access import PerpetualAccessInput


class TestPerpetualAccess(unittest.TestCase):
    def setUp(self):
        self.maxDiff = 5000

    def test_normalize_start_date(self):
        normalize = PerpetualAccessInput.csv_columns()['start_date']['normalize']
        assert_equals(normalize('2015-10-21'), datetime.datetime(2015, 10, 21).isoformat())
        assert_equals(normalize('October 21st 2015'), datetime.datetime(2015, 10, 21).isoformat())

        assert_equals(normalize('2015'), datetime.datetime(2015, 1, 1).isoformat())
        assert_equals(normalize('1993-01'), datetime.datetime(1993, 1, 1).isoformat())

        assert_equals(normalize('a few days ago'), ParseWarning.bad_date)
        assert_equals(normalize(None), None)

    def test_normalize_end_date(self):
        normalize = PerpetualAccessInput.csv_columns()['end_date']['normalize']
        assert_equals(normalize('2015-10-21'), datetime.datetime(2015, 10, 21).isoformat())
        assert_equals(normalize('October 21st 2015'), datetime.datetime(2015, 10, 21).isoformat())

        assert_equals(normalize('2015'), datetime.datetime(2015, 12, 31).isoformat())
        assert_equals(normalize('1993-02'), datetime.datetime(1993, 2, 28).isoformat())

        assert_equals(normalize('a few days ago'), ParseWarning.bad_date)
        assert_equals(normalize(None), None)

    def test_imports_perpetual_access(self):
        rows, warnings = PerpetualAccessInput.normalize_rows('test/test_files/perpetual_access/perpetual_access.csv')

        self.assertItemsEqual(
            [
                {'issn': '0012-4508', 'start_date': None, 'end_date': datetime.datetime(2020, 1, 1).isoformat()},
                {'issn': '0013-4694', 'start_date': datetime.datetime(1900, 1, 1).isoformat(), 'end_date': None},
                {'issn': '0749-5978', 'start_date': datetime.datetime(1955, 11, 05).isoformat(), 'end_date': datetime.datetime(2017, 10, 26).isoformat()},
            ],
            rows
        )

        self.assertItemsEqual([], warnings)
