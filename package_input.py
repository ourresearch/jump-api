import json
import os
import re
import tempfile
from re import sub

import boto3
import dateutil.parser
import shortuuid
import unicodecsv as csv
from enum import Enum
from sqlalchemy.sql import text

import package
import purge_cache
from app import db, logger
from excel import convert_spreadsheet_to_csv
from package_file_warning import PackageFileWarning
from util import convert_to_utf_8
from util import safe_commit


class PackageInput:
    @staticmethod
    def normalize_date(date_str, warn_if_blank=False, default=None):
        if date_str:
            try:
                return dateutil.parser.parse(date_str, default=default).isoformat()
            except Exception:
                return ParseWarning.bad_date
        else:
            return ParseWarning.bad_date if warn_if_blank else None

    @staticmethod
    def normalize_year(year, warn_if_blank=False):
        if year:
            try:
                return dateutil.parser.parse(year).year
            except Exception:
                return ParseWarning.bad_year
        else:
            return ParseWarning.bad_year if warn_if_blank else None

    @staticmethod
    def normalize_int(value, warn_if_blank=False):
        if value:
            try:
                return int(value)
            except Exception:
                return ParseWarning.bad_int
        else:
            return ParseWarning.bad_int if warn_if_blank else None

    @staticmethod
    def normalize_price(price, warn_if_blank=False):
        if price:
            try:
                decimal = u',' if re.search(ur'\.\d{3}', price) or re.search(ur',\d{2}$', price) else ur'\.'
                sub_pattern = ur'[^\d{}]'.format(decimal)
                price = sub(sub_pattern, '', price)
                price = sub(',', '.', price)
                return int(round(float(price)))
            except Exception:
                return ParseWarning.bad_usd_price
        else:
            return ParseWarning.bad_usd_price if warn_if_blank else None

    @staticmethod
    def normalize_issn(issn, warn_if_blank=False):
        if issn:
            issn = sub(ur'\s', '', issn).upper()
            if re.match(ur'^\d{4}-\d{3}(?:X|\d)$', issn):
                return issn
            elif re.match(ur'^[A-Z0-9]{4}-\d{3}(?:X|\d)$', issn):
                return ParseWarning.bundle_issn
            else:
                return ParseWarning.bad_issn
        else:
            return ParseWarning.bad_issn if warn_if_blank else None

    @staticmethod
    def strip_text(txt, warn_if_blank=False):
        if txt is not None:
            return txt.strip()
        else:
            return ParseWarning.blank_text if warn_if_blank else None


    @classmethod
    def csv_columns(cls):
        raise NotImplementedError()

    @classmethod
    def import_view_name(cls):
        raise NotImplementedError()

    @classmethod
    def destination_table(cls):
        raise NotImplementedError()

    @classmethod
    def file_type_label(cls):
        raise NotImplementedError()

    @classmethod
    def translate_row(cls, row):
        return [row]

    @classmethod
    def ignore_row(cls, row):
        return False

    @classmethod
    def apply_header(cls, normalized_rows, header_rows):
        return normalized_rows

    @classmethod
    def normalize_column_name(cls, raw_column_name):
        for canonical_name, spec in cls.csv_columns().items():
            name_snippets = spec['name_snippets']
            excluded_name_snippets = spec.get('excluded_name_snippets', [])

            for snippet in name_snippets:
                snippet = snippet.lower()
                column_name = raw_column_name.strip().lower()
                exact_name = spec.get('exact_name', False)
                contains_excluded_snippet = False

                for ens in excluded_name_snippets:
                    if ens in column_name:
                        contains_excluded_snippet = True
                        break

                if not contains_excluded_snippet:
                    if (exact_name and snippet == column_name) or (not exact_name and snippet in column_name.lower()):
                        return canonical_name

        return None

    @classmethod
    def normalize_cell(cls, normalized_column_name, raw_column_value):
        spec = cls.csv_columns()[normalized_column_name]
        return spec['normalize'](raw_column_value, spec.get('warn_if_blank', False))


    @classmethod
    def _copy_to_s3(cls, prefix, filename):
        s3 = boto3.client('s3')
        bucket_name = 'jump-redshift-staging'
        object_name = '{}_{}_{}'.format(prefix, cls.__name__, shortuuid.uuid())
        s3.upload_file(filename, bucket_name, object_name)
        return 's3://{}/{}'.format(bucket_name, object_name)

    @classmethod
    def delete(cls, package_id):
        num_deleted = db.session.query(cls).filter(cls.package_id == package_id).delete()
        db.session.execute("delete from {} where package_id = '{}'".format(cls.destination_table(), package_id))

        db.session.query(PackageFileWarning).filter(
            PackageFileWarning.package_id == package_id, PackageFileWarning.file == cls.file_type_label()
        ).delete()

        safe_commit(db)

        my_package = db.session.query(package.Package).filter(package.Package.package_id == package_id).scalar()
        if my_package:
            cls.clear_caches(my_package)


        return u'Deleted {} {} rows for package {}.'.format(num_deleted, cls.__name__, package_id)

    @classmethod
    def clear_caches(cls, my_package):
        my_package.clear_package_counter_breakdown_cache()
        purge_cache.purge_common_package_data_cache(my_package.package_id)

    @classmethod
    def update_dest_table(cls, package_id):
        # unload_cmd = text('''
        #     unload
        #     ('select * from {view} where package_id = \\'{package_id}\\'')
        #     to 's3://jump-redshift-staging/{package_id}_{view}_{uuid}/'
        #     with credentials :creds csv'''.format(
        #         view=cls.import_view_name(),
        #         package_id=package_id,
        #         uuid=shortuuid.uuid(),
        #     )
        # )
        #
        # aws_creds = 'aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}'.format(
        #     aws_key=os.getenv('AWS_ACCESS_KEY_ID'),
        #     aws_secret=os.getenv('AWS_SECRET_ACCESS_KEY')
        # )
        #
        # db.session.execute(unload_cmd.bindparams(creds=aws_creds))

        db.session.execute("delete from {} where package_id = '{}'".format(cls.destination_table(), package_id))

        db.session.execute(
            "insert into {} (select * from {} where package_id = '{}')".format(
                cls.destination_table(), cls.import_view_name(), package_id
            )
        )

    @classmethod
    def make_package_file_warning(cls, parse_warning, row_no=None, raw_column_name=None, raw_value=None, additional_msg=None):
        return {
            'file': cls.file_type_label(),
            'row_no': row_no,
            'column_name': raw_column_name,
            'raw_value': raw_value,
            'label': parse_warning.value['label'],
            'message': u'{}{}'.format(
                parse_warning.value['text'],
                u' message: {}'.format(additional_msg) if additional_msg else u''
            )
        }

    @classmethod
    def normalize_rows(cls, file_name):
        # convert to csv if needed
        if file_name.endswith(u'.xls') or file_name.endswith(u'.xlsx'):
            sheet_csv_file_names = convert_spreadsheet_to_csv(file_name, parsed=False)
            if not sheet_csv_file_names:
                raise RuntimeError(u'{} could not be opened as a spreadsheet'.format(file_name))

            if len(sheet_csv_file_names) > 1:
                raise RuntimeError(u'Workbook contains multiple sheets.')

            file_name = sheet_csv_file_names[0]

        # convert to utf-8
        file_name = convert_to_utf_8(file_name)
        logger.info('converted file: {}'.format(file_name))

        with open(file_name, 'r') as csv_file:
            # determine the csv format
            dialect_sample = ''
            for i in range(0, 20):
                next_line = csv_file.readline()
                dialect_sample = dialect_sample + next_line
                if not next_line:
                    break

            dialect = csv.Sniffer().sniff(dialect_sample)
            csv_file.seek(0)

            # turn rows into arrays
            # remember the first row that looks like a header
            max_columns = 0
            header_index = None
            parsed_rows = []
            line_no = 0
            for line in csv.reader(csv_file, dialect=dialect):
                if not any([cell.strip() for cell in line]):
                    continue

                parsed_rows.append(line)

                if len(line) > max_columns and all(line):
                    max_columns = len(line)
                    header_index = line_no
                    logger.info(u'candidate header row: {}'.format(u', '.join(line)))

                line_no += 1

            if header_index is None:
                # give up. can't turn rows into dicts if we don't have a header
                raise RuntimeError(u"Couldn't identify a header row in the file.")

            warnings = []
            normalized_rows = []

            # make sure we have all the required columns
            raw_column_names = parsed_rows[header_index]
            normalized_column_names = [cls.normalize_column_name(cn) for cn in raw_column_names]
            raw_to_normalized_map = dict(zip(raw_column_names, normalized_column_names))
            required_keys = [k for k, v in cls.csv_columns().items() if v.get('required', True)]

            if set(required_keys).difference(set(normalized_column_names)):
                explanation = u'Missing required columns. Expected [{}] but found {}.'.format(
                    ', '.join(sorted(required_keys)),
                    ', '.join([
                        u'{} (from input column {})'.format(raw_to_normalized_map[raw], raw)
                        for raw in sorted(raw_to_normalized_map.keys()) if raw_to_normalized_map[raw]
                    ])
                )
                raise RuntimeError(explanation)

            # combine the header and data rows into dicts
            row_dicts = [dict(zip(parsed_rows[header_index], x)) for x in parsed_rows[header_index+1:]]

            for row_no, row in enumerate(row_dicts):
                normalized_row = {}
                cell_warnings = []

                for raw_column_name in row.keys():
                    try:
                        raw_value = row[raw_column_name]
                        normalized_name = cls.normalize_column_name(raw_column_name)
                        if normalized_name:
                            normalized_value = cls.normalize_cell(normalized_name, raw_value)
                            if isinstance(normalized_value, ParseWarning):
                                parse_warning = normalized_value
                                logger.info('parse warning: {}'.format(parse_warning))
                                cell_warnings.append(cls.make_package_file_warning(
                                    parse_warning,
                                    row_no=row_no + 1,
                                    raw_column_name=raw_column_name,
                                    raw_value=raw_value,
                                ))
                                normalized_row.setdefault(normalized_name, None)
                            else:
                                normalized_row.setdefault(normalized_name, normalized_value)
                    except Exception as e:
                        cell_warnings.append(cls.make_package_file_warning(
                            ParseWarning.unknown,
                            row_no=row_no + 1,
                            raw_column_name=raw_column_name,
                            raw_value=raw_value,
                            additional_msg=e.message
                        ))

                if cls.ignore_row(normalized_row):
                    continue

                warnings.extend(cell_warnings)

                if not cell_warnings:
                    normalized_rows.extend(cls.translate_row(normalized_row))

            cls.apply_header(normalized_rows, parsed_rows[0:header_index+1])

            return normalized_rows, warnings

    @classmethod
    def load(cls, package_id, file_name, commit=False):
        try:
            normalized_rows, warnings = cls.normalize_rows(file_name)
        except (RuntimeError, UnicodeError) as e:
            return {'success': False, 'message': e.message, 'warnings': []}

        if not normalized_rows:
            return {
                'success': False,
                'message': u'No usable rows found in {}'.format(file_name),
                'warnings': []
            }

        for row in normalized_rows:
            row.update({'package_id': package_id})
            logger.info(u'normalized row: {}'.format(json.dumps(row)))

        # save normalized rows
        db.session.query(cls).filter(cls.package_id == package_id).delete()

        sorted_fields = sorted(normalized_rows[0].keys())
        normalized_csv_filename = tempfile.mkstemp()[1]
        with open(normalized_csv_filename, 'w') as normalized_csv_file:
            writer = csv.DictWriter(normalized_csv_file, delimiter=',', encoding='utf-8', fieldnames=sorted_fields)
            for row in normalized_rows:
                writer.writerow(row)

        s3_object = cls._copy_to_s3(package_id, normalized_csv_filename)

        copy_cmd = text('''
            copy {table} ({fields}) from '{s3_object}'
            credentials :creds format as csv
            timeformat 'auto';
        '''.format(
            table=cls.__tablename__,
            fields=', '.join(sorted_fields),
            s3_object=s3_object,
        ))

        aws_creds = 'aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}'.format(
            aws_key=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        db.session.execute(copy_cmd.bindparams(creds=aws_creds))
        cls.update_dest_table(package_id)

        # save warnings

        db.session.query(PackageFileWarning).filter(
            PackageFileWarning.package_id == package_id, PackageFileWarning.file == cls.file_type_label()
        ).delete()

        if warnings:
            warning_rows = [PackageFileWarning(**w).__dict__ for w in warnings]
            for wr in warning_rows:
                wr.update({'package_id': package_id})
                wr.pop('_sa_instance_state', None)

            warning_columns = sorted(warning_rows[0].keys())
            warning_csv_filename = tempfile.mkstemp()[1]
            with open(warning_csv_filename, 'w') as warning_csv_file:
                writer = csv.DictWriter(warning_csv_file, delimiter=',', encoding='utf-8', fieldnames=warning_columns)
                for wr in warning_rows:
                    writer.writerow(wr)

            s3_object = cls._copy_to_s3(u'{}_warnings'.format(package_id), warning_csv_filename)

            copy_cmd = text('''
                copy jump_package_file_import_warning ({fields}) from '{s3_object}'
                credentials :creds format as csv
                timeformat 'auto';
            '''.format(
                fields=', '.join(warning_columns),
                s3_object=s3_object,
            ))

            logger.info(copy_cmd)

            db.session.execute(copy_cmd.bindparams(creds=aws_creds))

        my_package = db.session.query(package.Package).filter(package.Package.package_id == package_id).scalar()

        if commit:
            safe_commit(db)
            if my_package:
                cls.clear_caches(my_package)

        return {
            'success': True,
            'message': u'Inserted {} {} rows for package {}.'.format(len(normalized_rows), cls.__name__, package_id),
            'warnings': warnings
        }


class ParseWarning(Enum):
    bad_issn = {
        'label': 'bad_issn',
        'text': 'Invalid ISSN format.'
    }
    bundle_issn = {
        'label': 'bundle_issn',
        'text': 'ISSN represents a bundle of journals, not a single journal.'
    }
    bad_date = {
        'label': 'bad_date',
        'text': 'Unrecognized date format.'
    }
    bad_year = {
        'label': 'bad_year',
        'text': 'Unrecognized date or year.'
    }
    bad_int = {
        'label': 'bad_int',
        'text': 'Unrecognized integer format.'
    }
    bad_usd_price = {
        'label': 'bad_usd_price',
        'text': 'Unrecognized USD format.'
    }
    blank_text = {
        'label': 'blank_text',
        'text': 'Expected text here.'
    }
    unknown = {
        'label': 'unknown_error',
        'text': 'There was an unexpected error parsing this cell. Try to correct the cell value or contact support.'
    }
    row_error = {
        'label': 'row_error',
        'text': 'Error for this row. See cell warnings for details.'
    }
    no_rows = {
        'label': 'no_rows',
        'text': "No usable rows could be extracted."
    }
