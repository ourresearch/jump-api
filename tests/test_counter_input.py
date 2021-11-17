import pytest
from counter import CounterInput
from app import get_db_cursor

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


def test_imports_counter_4_samples():
    for file_name, expected_rows in file_rows.items():
        rows, warnings = CounterInput().normalize_rows(file_name='tests/test_files/counter/{}'.format(file_name))
        assert expected_rows == rows

def test_set_to_delete():
    package_id = 'package-55BdKPno2uX5'
    report_name = "jr1"
    res = CounterInput().set_to_delete(package_id, report_name)
    assert res == "Queued to delete"
    with get_db_cursor() as cursor:
        command = "select * from jump_raw_file_upload_object where package_id=%(package_id)s and file=%(file)s"
        values = {'package_id': package_id, 'file': report_name}
        print(cursor.mogrify(command, values))
        cursor.execute(command, values)
        rows = cursor.fetchall()
    assert len(rows) == 0

# def test_delete():
#     # add counter files
#     # FIXME, add this step - There's no way to programatically upload a COUNTER file
#     #   Upload to S3 is done on the front end - we can only run /publisher/<package_id>/sign-s3 route here
#     #   - but maybe I can run /publisher/<package_id>/sign-s3 and then get back the url and upload from here?
# 
#     # try to delete counter files for which they exist
#     package_id = 'package-55BdKPno2uX5'
#     res = CounterInput().delete(package_id = package_id)
#     assert isinstance(res, str)
#     assert 'Deleted CounterInput' in res
#     assert package_id in res

#     # try to delete counter files when they don't exist
#     res2 = CounterInput().delete(package_id = package_id)
#     assert isinstance(res2, str)
#     assert 'Deleted CounterInput' in res
#     assert package_id in res