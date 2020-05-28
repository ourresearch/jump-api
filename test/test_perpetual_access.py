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

        assert_equals(normalize('a few days ago'), ParseWarning.bad_date)
        assert_equals(normalize(None), None)

    def test_normalize_end_date(self):
        normalize = PerpetualAccessInput.csv_columns()['end_date']['normalize']
        assert_equals(normalize('2015-10-21'), datetime.datetime(2015, 10, 21).isoformat())
        assert_equals(normalize('October 21st 2015'), datetime.datetime(2015, 10, 21).isoformat())

        assert_equals(normalize('2015'), datetime.datetime(2015, 12, 31).isoformat())

        assert_equals(normalize('a few days ago'), ParseWarning.bad_date)
        assert_equals(normalize(None), None)
