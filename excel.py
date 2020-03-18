import csv
import tempfile
import zipfile

import openpyxl
import pyexcel

from app import logger


def convert_xls_to_xlsx(xls_file):
    xlsx_file_name = tempfile.mkstemp(suffix='.xlsx')[1]
    pyexcel.save_book_as(file_name=xls_file, dest_file_name=xlsx_file_name)
    return xlsx_file_name


def convert_spreadsheet_to_csv(spreadsheet):
    if spreadsheet.endswith('.xls'):
        spreadsheet = convert_xls_to_xlsx(spreadsheet)

    try:
        workbook = openpyxl.load_workbook(open(spreadsheet, "rb"), read_only=True)
    except (KeyError, zipfile.BadZipfile) as e:
        logger.info(u'{} could not be opened as a spreadsheet: {}, {}'.format(spreadsheet, type(e), e.message))
        return False

    rows = []
    column_names = None

    for sheet_name in list(workbook.sheetnames):
        sheet = workbook[sheet_name]

        sheet_column_names = {}
        for i, column in enumerate(list(sheet.iter_rows(min_row=1, max_row=1))[0]):
            sheet_column_names[i] = column.value.lower().strip()

        if column_names is None:
            column_names = sheet_column_names.values()
        elif set(sheet_column_names.values()).difference(set(column_names)):
            raise ValueError(u'all worksheets must contain the same columns')

        for row_cells in sheet.iter_rows(min_row=2):
            row = {}
            for column in range(0, len(sheet_column_names)):
                row[sheet_column_names[column]] = row_cells[column].value or None
            rows.append(row)

    csv_file_name = tempfile.mkstemp()[1]

    with open(csv_file_name, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, column_names)

        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return csv_file_name