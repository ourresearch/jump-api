# coding: utf-8

import pytest

# TODO: separate file input table classes from ingestion logic to remove import cycles
import package
from counter import CounterInput

file_rows = {
    'counter4_jr1_2018_00.csv': [
        {'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0379-4172', 'total': 0,   'journal_name': 'Acta Genetica Sinica', },
        {'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '1877-3435', 'total': 326, 'journal_name': 'Current Opinion in Environmental Sustainability', },
        {'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '2213-8463', 'total': 17,  'journal_name': 'Manufacturing Letters', },
        {'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '1328-0163', 'total': 0,   'journal_name': 'The Asia Pacific Heart Journal', },
        {'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0923-2524', 'total': 0,   'journal_name': 'Urgences Medicales', },
        {'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0944-2006', 'total': 231, 'journal_name': 'Zoology', },
        # {'publisher': 'Elsevier', 'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0379-4172', 'total': 0,   'journal_name': 'Acta Genetica Sinica', },
        # {'publisher': 'Elsevier', 'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '1877-3435', 'total': 326, 'journal_name': 'Current Opinion in Environmental Sustainability', },
        # {'publisher': 'Elsevier', 'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '2213-8463', 'total': 17,  'journal_name': 'Manufacturing Letters', },
        # {'publisher': 'Elsevier', 'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '1328-0163', 'total': 0,   'journal_name': 'The Asia Pacific Heart Journal', },
        # {'publisher': 'Elsevier', 'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0923-2524', 'total': 0,   'journal_name': 'Urgences Médicales', },
        # {'publisher': 'Elsevier', 'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0944-2006', 'total': 231, 'journal_name': 'Zoology', },
    ],
    'counter4_jr1_2018_01.xlsx': [
        {'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0956-7151', 'total': 1,   'journal_name': 'Acta Metallurgica et Materialia', },
        {'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '1017-995X', 'total': 24,  'journal_name': 'Acta Orthopaedica et Traumatologica Turcica', },
        {'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0091-3057', 'total': 122, 'journal_name': 'Pharmacology, Biochemistry and Behavior', },
        {'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0944-2006', 'total': 16,  'journal_name': 'Zoology', },
        # {'publisher': 'Elsevier', 'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0956-7151', 'total': 1,   'journal_name': 'Acta Metallurgica et Materialia', },
        # {'publisher': 'Elsevier', 'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '1017-995X', 'total': 24,  'journal_name': 'Acta Orthopaedica et Traumatologica Turcica', },
        # {'publisher': 'Elsevier', 'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0091-3057', 'total': 122, 'journal_name': 'Pharmacology, Biochemistry and Behavior', },
        # {'publisher': 'Elsevier', 'report_name': 'jr1', 'report_version': '4', 'report_year': 2018, 'issn': '0944-2006', 'total': 16,  'journal_name': 'Zoology', },
    ]
}


# class TestCounterInput(unittest.TestCase):
#     def setUp():
#         self.maxDiff = 5000

def test_imports_counter_4_samples():
    for file_name, expected_rows in file_rows.items():
        rows, warnings = CounterInput().normalize_rows(file_name='tests/test_files/counter/{}'.format(file_name))
        assert expected_rows == rows

# def test_imports_utf_16le():
#     rows, warnings = CounterInput().normalize_rows(file_name='tests/test_files/counter/counter4_jr1_2018_00_utf_16le.csv')
#     assert file_rows['counter4_jr1_2018_00.csv'] == rows

# def test_imports_utf_16be():
#     rows, warnings = CounterInput().normalize_rows(file_name='tests/test_files/counter/counter4_jr1_2018_00_utf_16be.csv')
#     assert file_rows['counter4_jr1_2018_00.csv'] == rows

# def test_imports_utf_16le_bom():
#     rows, warnings = CounterInput().normalize_rows(file_name='tests/test_files/counter/counter4_jr1_2018_00_utf_16le_bom.csv')
#     assert file_rows['counter4_jr1_2018_00.csv'] == rows

# def test_imports_utf_16be_bom():
#     rows, warnings = CounterInput().normalize_rows(file_name='tests/test_files/counter/counter4_jr1_2018_00_utf_16be_bom.csv')
#     assert file_rows['counter4_jr1_2018_00.csv'] == rows

# def test_imports_utf_32le():
#     rows, warnings = CounterInput().normalize_rows(file_name='tests/test_files/counter/counter4_jr1_2018_00_utf_32le.csv')
#     assert file_rows['counter4_jr1_2018_00.csv'] == rows

# def test_imports_utf_32be():
#     rows, warnings = CounterInput().normalize_rows(file_name='tests/test_files/counter/counter4_jr1_2018_00_utf_32be.csv')
#     assert file_rows['counter4_jr1_2018_00.csv'] == rows

# def test_imports_utf_32le_bom():
#     rows, warnings = CounterInput().normalize_rows(file_name='tests/test_files/counter/counter4_jr1_2018_00_utf_32le_bom.csv')
#     assert file_rows['counter4_jr1_2018_00.csv'] == rows

# def test_imports_utf_132be_bom():
#     rows, warnings = CounterInput().normalize_rows(file_name='tests/test_files/counter/counter4_jr1_2018_00_utf_32be_bom.csv')
#     assert file_rows['counter4_jr1_2018_00.csv'] == rows

# def test_imports_windows_1252():
#     rows, warnings = CounterInput().normalize_rows(file_name='tests/test_files/counter/counter4_jr1_2018_00_windows_1252.csv')
#     assert file_rows['counter4_jr1_2018_00.csv'] == rows

# def test_rejects_counter5():
#     with pytest.raises(RuntimeError, match=r".*COUNTER 5.*"):
#         CounterInput().normalize_rows(file_name='tests/test_files/counter/counter5_tr_j1_2019_00.csv')
