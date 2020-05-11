import unittest

from nose.tools import assert_equals
from nose.tools import assert_raises_regexp

# TODO: separate file input table classes from ingestion logic to remove import cycles
import package
from package_input import PackageInput


class TestPackageInput(unittest.TestCase):
    def test_normalizes_issn(self):
        assert_equals(PackageInput.normalize_issn('1234-5678'), '1234-5678')
        assert_equals(PackageInput.normalize_issn('1234-567X'), '1234-567X')
        assert_equals(PackageInput.normalize_issn('1234-567x'), '1234-567X')
        assert_equals(PackageInput.normalize_issn('\n1234-567X\t'), '1234-567X')
        assert_equals(PackageInput.normalize_issn('\n1234- 567X\t'), '1234-567X')
        assert_equals(PackageInput.normalize_issn('\n1234-\n567x\t'), '1234-567X')
        assert_equals(PackageInput.normalize_issn('\nfs34-\n567x\t'), 'FS34-567X')

    def test_rejects_invalid_issns(self):
        invalid_issns = [
            '1234-56789',
            '1234-56XX',
            'print: \nfs34-\n123x\t',
            '\nfs34-\n5123x\t',
            'RT34-\n123x\t',
            'FS34-\n123y\t',
        ]

        for i in invalid_issns:
            assert_raises_regexp(ValueError, i, lambda: PackageInput.normalize_issn(i))
