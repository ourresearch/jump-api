# coding: utf-8

import unittest

# TODO: separate file input table classes from ingestion logic to remove import cycles
import package
from journal_price import JournalPriceInput

class TestCounterInput(unittest.TestCase):
    def setUp(self):
        self.maxDiff = 5000

    def test_imports_double_quoted_csv_with_commas_in_prices(self):
        rows, warnings = JournalPriceInput.normalize_rows('test/test_files/journal_price/double_quote_comma.csv')
        self.assertItemsEqual([
            {'issn': '0009-2614', 'price': 10198},
            {'issn': '0006-2952', 'price': 10278},
            {'issn': '0376-7388', 'price': 10325},
        ], rows)
