import datetime
# import unittest
import pytest

# from nose.tools import assert_equals, assert_raises_regexp

# TODO: separate file input table classes from ingestion logic to remove import cycles
import package
from package_input import PackageInput, ParseWarning
from util import write_to_tempfile


# def setUp(self):
#     self.maxDiff = 5000

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
