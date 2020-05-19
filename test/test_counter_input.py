# coding: utf-8

import unittest

# TODO: separate file input table classes from ingestion logic to remove import cycles
import package
from counter import CounterInput


class TestCounterInput(unittest.TestCase):
    def test_imports_counter_4_samples(self):
        self.maxDiff = 5000

        file_rows = {
            'counter4_jr1_2018_00.csv': [
                {'publisher': u'Elsevier', 'report_name': 'JR1', 'report_version': '4', 'report_year': 2018, 'issn': u'0379-4172', 'total': 0,   'journal_name': u'Acta Genetica Sinica', },
                {'publisher': u'Elsevier', 'report_name': 'JR1', 'report_version': '4', 'report_year': 2018, 'issn': u'1877-3435', 'total': 326, 'journal_name': u'Current Opinion in Environmental Sustainability', },
                {'publisher': u'Elsevier', 'report_name': 'JR1', 'report_version': '4', 'report_year': 2018, 'issn': u'2213-8463', 'total': 17,  'journal_name': u'Manufacturing Letters', },
                {'publisher': u'Elsevier', 'report_name': 'JR1', 'report_version': '4', 'report_year': 2018, 'issn': u'1328-0163', 'total': 0,   'journal_name': u'The Asia Pacific Heart Journal', },
                {'publisher': u'Elsevier', 'report_name': 'JR1', 'report_version': '4', 'report_year': 2018, 'issn': u'0923-2524', 'total': 0,   'journal_name': u'Urgences Médicales', },
                {'publisher': u'Elsevier', 'report_name': 'JR1', 'report_version': '4', 'report_year': 2018, 'issn': u'0944-2006', 'total': 231, 'journal_name': u'Zoology', },
            ],
            'counter4_jr1_2018_01.xlsx': [
                {'publisher': u'Elsevier', 'report_name': 'JR1', 'report_version': '4', 'report_year': 2018, 'issn': u'0956-7151', 'total': 1,   'journal_name': u'Acta Metallurgica et Materialia', },
                {'publisher': u'Elsevier', 'report_name': 'JR1', 'report_version': '4', 'report_year': 2018, 'issn': u'1017-995X', 'total': 24,  'journal_name': u'Acta Orthopaedica et Traumatologica Turcica', },
                {'publisher': u'Elsevier', 'report_name': 'JR1', 'report_version': '4', 'report_year': 2018, 'issn': u'0091-3057', 'total': 122, 'journal_name': u'Pharmacology, Biochemistry and Behavior', },
                {'publisher': u'Elsevier', 'report_name': 'JR1', 'report_version': '4', 'report_year': 2018, 'issn': u'0944-2006', 'total': 16,  'journal_name': u'Zoology', },
            ]
        }

        for file_name, expected_rows in file_rows.items():
            rows = CounterInput.normalize_rows('test/test_files/counter/{}'.format(file_name))
            self.assertItemsEqual(expected_rows, rows)