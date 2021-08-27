# coding: utf-8

import datetime
import pytest
import package
from package_input import ParseWarning
from perpetual_access import PerpetualAccessInput

def test_normalize_start_date():
    normalize = PerpetualAccessInput().csv_columns()['start_date']['normalize']
    assert normalize('2015-10-21') == datetime.datetime(2015, 10, 21).isoformat()
    assert normalize('October 21st 2015') == datetime.datetime(2015, 10, 21).isoformat()

    assert normalize('2015') == datetime.datetime(2015, 1, 1).isoformat()
    assert normalize('1993-01') == datetime.datetime(1993, 1, 1).isoformat()

    assert normalize('a few days ago') == ParseWarning.bad_date
    assert normalize(None) == None

def test_normalize_end_date():
    normalize = PerpetualAccessInput().csv_columns()['end_date']['normalize']
    assert normalize('2015-10-21') == datetime.datetime(2015, 10, 21).isoformat()
    assert normalize('October 21st 2015') == datetime.datetime(2015, 10, 21).isoformat()

    assert normalize('2015') == datetime.datetime(2015, 12, 31).isoformat()
    assert normalize('1993-02') == datetime.datetime(1993, 2, 28).isoformat()

    assert normalize('a few days ago') == ParseWarning.bad_date
    assert normalize(None) == None

def test_imports_perpetual_access():
    rows, warnings = PerpetualAccessInput().normalize_rows(file_name='tests/test_files/perpetual_access/perpetual_access.csv')

    assert[
        {'issn': '0012-4508', 'start_date': None, 'end_date': datetime.datetime(2020, 1, 1).isoformat()},
        {'issn': '0013-4694', 'start_date': datetime.datetime(1900, 1, 1).isoformat(), 'end_date': None},
        {'issn': '0749-5978', 'start_date': datetime.datetime(1955, 11, 5).isoformat(), 'end_date': datetime.datetime(2017, 10, 26).isoformat()},
    ] == rows

    assert warnings is None
