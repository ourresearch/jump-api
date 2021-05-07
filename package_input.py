# coding: utf-8

from cached_property import cached_property
import json
import os
import re
import tempfile
import calendar
from re import sub

import babel.numbers
import dateutil.parser
import shortuuid
import unicodecsv as csv
from enum import Enum
from sqlalchemy.sql import text
from kids.cache import cache

import package
from app import db, logger
from app import get_db_cursor
from app import reset_cache
from consortium import Consortium
from app import s3_client
from excel import convert_spreadsheet_to_csv
from package_file_error_rows import PackageFileErrorRow
from raw_file_upload_object import RawFileUploadObject
from util import convert_to_utf_8
from util import safe_commit



class PackageInput:
    @staticmethod
    def normalize_date(date_str, warn_if_blank=False, default=None):
        if date_str:
            try:
                parsed_date = dateutil.parser.parse(date_str, default=default)
                if unicode(parsed_date.year) not in date_str:
                    return ParseWarning.ambiguous_date
                return parsed_date.isoformat()
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
            return ParseWarning.no_int if warn_if_blank else None

    @staticmethod
    def normalize_price(price, warn_if_blank=False):
        if price:
            try:
                if u"." in price and u"," in price and price.find(u".") < price.find(u","):
                    locale = u"de"
                elif price.count(u",") == 1 and (re.search(ur"\d{4,},", price) or not re.search(ur",\d{3}[$.]", price)):
                    locale = u"de"
                else:
                    locale = u"en"

                price = sub(ur"[^\d.,]", "", price)
                parsed_price = babel.numbers.parse_decimal(price, locale=locale)
                return int(round(parsed_price))
            except babel.numbers.NumberFormatError:
                return ParseWarning.bad_usd_price
        else:
            return ParseWarning.no_usd_price if warn_if_blank else None

    @staticmethod
    def normalize_issn(issn, warn_if_blank=False):
        from scenario import get_ricks_journal_flat
        if issn:
            issn = sub(ur"\s", "", issn).upper()
            if re.match(ur"^\d{4}-?\d{3}(?:X|\d)$", issn):
                issn = issn.replace(u"-", "")
                issn = issn[0:4] + u"-" + issn[4:8]
                if issn in get_ricks_journal_flat():
                    return issn
                else:
                    return ParseWarning.unknown_issn
            elif re.match(ur"^[A-Z0-9]{4}-\d{3}(?:X|\d)$", issn):
                return ParseWarning.bundle_issn
            else:
                return ParseWarning.bad_issn
        else:
            return ParseWarning.no_issn if warn_if_blank else None

    @staticmethod
    def strip_text(txt, warn_if_blank=False):
        if txt is not None:
            return txt.strip()
        else:
            return ParseWarning.blank_text if warn_if_blank else None


    def csv_columns(self):
        raise NotImplementedError()

    def import_view_name(self):
        raise NotImplementedError()

    def destination_table(self):
        raise NotImplementedError()

    def file_type_label(self):
        raise NotImplementedError()

    def translate_row(self, row):
        return row

    def ignore_row(self, row):
        return False

    def apply_header(self, normalized_rows, header_rows):
        return normalized_rows

    @cache
    def normalize_column_name(self, raw_column_name):
        for canonical_name, spec in self.csv_columns().items():
            name_snippets = spec["name_snippets"]
            excluded_name_snippets = spec.get("excluded_name_snippets", [])

            for snippet in name_snippets:
                snippet = snippet.lower()
                column_name = raw_column_name.strip().lower()
                exact_name = spec.get("exact_name", False)
                contains_excluded_snippet = False

                for ens in excluded_name_snippets:
                    if ens in column_name:
                        contains_excluded_snippet = True
                        break

                if not contains_excluded_snippet:
                    if (exact_name and snippet == column_name) or (not exact_name and snippet in column_name.lower()):
                        return canonical_name

        return None

    @cache
    def normalize_cell(self, normalized_column_name, raw_column_value):
        spec = self.csv_columns()[normalized_column_name]
        return spec["normalize"](raw_column_value, spec.get("warn_if_blank", False))


    def _copy_staging_csv_to_s3(self, filename, package_id):
        bucket_name = "jump-redshift-staging"
        object_name = "{}_{}_{}".format(package_id, self.__class__.__name__, shortuuid.uuid())
        s3_client.upload_file(filename, bucket_name, object_name)
        return "s3://{}/{}".format(bucket_name, object_name)

    def _raw_s3_bucket(self):
        return u"unsub-file-uploads"

    def _copy_raw_to_s3(self, filename, package_id, num_rows=None, error=None):
        if u"." in filename:
            suffix = u".{}".format(filename.split(u".")[-1])
        else:
            suffix = u""

        object_name = "{}_{}{}".format(package_id, self.file_type_label(), suffix)
        bucket_name = self._raw_s3_bucket()

        s3_client.upload_file(filename, bucket_name, object_name)

        with get_db_cursor() as cursor:
            print("delete from jump_raw_file_upload_object where package_id = '{}' and file = '{}'".format(
                package_id, self.file_type_label()))

            cursor.execute("delete from jump_raw_file_upload_object where package_id = '{}' and file = '{}'".format(
                package_id, self.file_type_label()))

        db.session.add(RawFileUploadObject(
            package_id=package_id,
            file=self.file_type_label(),
            bucket_name=bucket_name,
            object_name=object_name,
            num_rows=num_rows,
            error=error
        ))

        return "s3://{}/{}".format(bucket_name, object_name)

    # def get_raw_upload_object(self, package_id):
    #     object_details = RawFileUploadObject.query.filter(
    #         RawFileUploadObject.package_id == package_id, RawFileUploadObject.file == self.file_type_label()
    #     ).scalar()
    #
    #     if not object_details:
    #         return None
    #
    #     try:
    #         raw_object = s3_client.get_object(Bucket=object_details.bucket_name, Key=object_details.object_name)
    #
    #         headers = {
    #             "Content-Length": raw_object["ContentLength"],
    #             "Content-Disposition": "attachment; filename='{}'".format(object_details.object_name)
    #         }
    #
    #         return {
    #             "body": raw_object["Body"],
    #             "content_type": raw_object["ContentType"],
    #             "headers": headers
    #         }
    #     except s3_client.exceptions.NoSuchKey:
    #         return None

    def delete(self, package_id, dummy=None):

        with get_db_cursor() as cursor:
            print("delete from {} where package_id = '{}'".format(self.__tablename__, package_id))
            print("delete from {} where package_id = '{}'".format(self.destination_table(), package_id))
            print("delete from jump_file_import_error_rows where package_id = '{}' and file = '{}'".format(
                package_id, self.file_type_label()))
            print("delete from jump_raw_file_upload_object where package_id = '{}' and file = '{}'".format(
                package_id, self.file_type_label()))

            cursor.execute("delete from {} where package_id = '{}'".format(self.__tablename__, package_id))
            cursor.execute("delete from {} where package_id = '{}'".format(self.destination_table(), package_id))
            cursor.execute("delete from jump_file_import_error_rows where package_id = '{}' and file = '{}'".format(
                package_id, self.file_type_label()))
            cursor.execute("delete from jump_raw_file_upload_object where package_id = '{}' and file = '{}'".format(
                package_id, self.file_type_label()))

        my_package = db.session.query(package.Package).filter(package.Package.package_id == package_id).scalar()
        if my_package:
            self.clear_caches(my_package)

        return u"Deleted {} rows for package {}.".format(self.__class__.__name__, package_id)


    def clear_caches(self, my_package):
        # print "clearing cache"
        if my_package.is_owned_by_consortium:
            print u"clearing consortium cache for my_package.is_owned_by_consortium: {}".format(my_package)
            for consortium_scenario_id in my_package.consortia_scenario_ids_who_own_this_package:

                # this will actually recalculate all member institutions instead of just this one
                # which will take longer, but don't worry about it for now

                my_consortium = Consortium(consortium_scenario_id)
                email = u"heather+{}@ourresearch.org".format(my_package.package_id)
                my_consortium.queue_for_recompute(email)
                reset_cache("consortium", "consortium_get_computed_data", consortium_scenario_id)
        else:
            print u"NO NEED TO cache clear consortium_get_computed_data for my_package {}".format(my_package)

        # my_package.clear_package_counter_breakdown_cache() # not used anymore

    def update_dest_table(self, package_id):
        # unload_cmd = text("""
        #     unload
        #     ('select * from {view} where package_id = \\'{package_id}\\'')
        #     to 's3://jump-redshift-staging/{package_id}_{view}_{uuid}/'
        #     with credentials :creds csv""".format(
        #         view=self.import_view_name(),
        #         package_id=package_id,
        #         uuid=shortuuid.uuid(),
        #     )
        # )
        #
        # aws_creds = "aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}".format(
        #     aws_key=os.getenv("AWS_ACCESS_KEY_ID"),
        #     aws_secret=os.getenv("AWS_SECRET_ACCESS_KEY")
        # )
        #

        with get_db_cursor() as cursor:
            print("delete from {} where package_id = '{}'".format(self.destination_table(), package_id))
            print("insert into {} (select * from {} where package_id = '{}')".format(
                    self.destination_table(), self.import_view_name(), package_id))

            cursor.execute("delete from {} where package_id = '{}'".format(self.destination_table(), package_id))
            cursor.execute("insert into {} (select * from {} where package_id = '{}')".format(
                    self.destination_table(), self.import_view_name(), package_id))

    def make_package_file_warning(self, parse_warning, additional_msg=None):
        return {
            "label": parse_warning.value["label"],
            "message": u"{}{}".format(
                parse_warning.value["text"],
                u" {}".format(additional_msg) if additional_msg else u""
            )
        }

    def save_errors(self, package_id, errors):
        if errors is None:
            return

        error_str = json.dumps(errors)
        chunk_size = 64 * 1024 - 1
        error_chunks = [error_str[i:i + chunk_size] for i in range(0, len(error_str), chunk_size)]

        rows = []
        for i, chunk in enumerate(error_chunks):
            rows.append(
                PackageFileErrorRow(package_id=package_id, file=self.file_type_label(), sequence=i, errors=chunk)
            )

        for row in rows:
            db.session.add(row)

    def load_errors(self, package_id):
        rows = PackageFileErrorRow.query.filter(
            PackageFileErrorRow.package_id == package_id,
            PackageFileErrorRow.file == self.file_type_label()
        ).order_by(PackageFileErrorRow.sequence).all()

        if not rows:
            return None
        else:
            try:
                errors = json.loads(u"".join([row.errors for row in rows]))
            except ValueError:
                print u"ValueError in load_errors with ", package_id
                return None

            if errors["rows"]:
                return errors
            else:
                return None

    def issn_columns(self):
        return []

    def validate_publisher(self):
        return False

    def normalize_rows(self, file_name, file_package=None):
        from scenario import get_ricks_journal_flat

        # convert to csv if needed
        if file_name.endswith(u".xls") or file_name.endswith(u".xlsx"):
            sheet_csv_file_names = convert_spreadsheet_to_csv(file_name, parsed=False)
            if not sheet_csv_file_names:
                raise RuntimeError(u"Could not be opened as a spreadsheet.")

            if len(sheet_csv_file_names) > 1:
                raise RuntimeError(u"Workbook contains multiple sheets.")

            file_name = sheet_csv_file_names[0]

        # convert to utf-8
        # very slow, try not doing this for now
        # file_name = convert_to_utf_8(file_name)
        # logger.info("converted file: {}".format(file_name))

        with open(file_name, "rb") as csv_file:
            # determine the csv format
            dialect_sample = ""
            for i in range(0, 20):
                next_line = csv_file.readline()
                # ignore header row when determining dialect
                # data rows should be more consistent with each other
                if i > 0:
                    dialect_sample = dialect_sample + next_line
                if not next_line:
                    break

            reader_params = {}
            try:
                dialect = csv.Sniffer().sniff(dialect_sample)
                # logger.info(u"sniffed csv dialect:\n{}".format(json.dumps(vars(dialect), indent=2)))
            except csv.Error:
                dialect = None
                if file_name.endswith(u".tsv"):
                    reader_params["delimiter"] = "\t"

            csv_file.seek(0)

            # turn rows into arrays
            # remember the first row that looks like a header
            max_columns = 0
            header_index = None
            parsed_rows = []
            line_no = 0  # the index in parsed_rows where this row will land
            absolute_line_no = 0  # the actual file row we're parsing
            parsed_to_absolute_line_no = {}

            # print "getting chardet_result"
            # import chardet
            # with open(file_name, 'rb') as rawdata:
            #     chardet_result = chardet.detect(rawdata.read(1000000))
            # print chardet_result

            for line in csv.reader(csv_file, dialect=dialect, encoding="ISO-8859-1", **reader_params):
                absolute_line_no += 1
                if not any([cell.strip() for cell in line]):
                    continue

                parsed_rows.append(line)
                parsed_to_absolute_line_no[line_no] = absolute_line_no

                populated_columns = len([cell for cell in line if cell.strip()])
                if populated_columns > max_columns:
                    max_columns = populated_columns
                    header_index = line_no
                    logger.info(u"candidate header row: {}".format(u", ".join(line)))

                line_no += 1

            if header_index is None:
                # give up. can't turn rows into dicts if we don't have a header
                raise RuntimeError(u"Couldn't identify a header row in the file.")

            error_rows = {
                "rows": [],
                "headers": [{"id": "row_id", "name": "Row Number"}]
            }

            normalized_rows = []

            # make sure we have all the required columns
            raw_column_names = parsed_rows[header_index]
            normalized_column_names = [self.normalize_column_name(cn) for cn in raw_column_names]
            raw_to_normalized_map = dict(zip(raw_column_names, normalized_column_names))
            normalized_to_raw_map = {}
            for k, v in raw_to_normalized_map.items():
                normalized_to_raw_map[v] = k

            required_keys = [k for k, v in self.csv_columns().items() if v.get("required", True)]


            # combine the header and data rows into dicts
            row_dicts = [dict(zip(parsed_rows[header_index], x)) for x in parsed_rows[header_index+1:]]


            # if ("total" in required_keys) and ("total" not in normalized_column_names) and ("jan" in normalized_column_names):
            #     for row in row_dicts:
            #
            #         row["total"] = 0
            #         for month_idx in range(1, 13):
            #             month_name = calendar.month_abbr[month_idx].lower()
            #             new_value = self.normalize_cell(month_name, row[normalized_to_raw_map[month_name]])
            #             row["total"] += new_value
            #
            #     normalized_column_names += ["total"]

            if set(required_keys).difference(set(normalized_column_names)):
                print u"Error: missing required columns. Required: {}, Have {} As {}.".format(required_keys, raw_column_names, normalized_column_names)
                raise RuntimeError(u"Missing required columns.")

            for row_no, row in enumerate(row_dicts):
                absolute_row_no = parsed_to_absolute_line_no[row_no] + header_index + 1
                normalized_row = {}
                cell_errors = {}

                for raw_column_name in row.keys():
                    raw_value = row[raw_column_name]
                    normalized_name = self.normalize_column_name(raw_column_name)
                    if normalized_name:
                        try:
                            normalized_value = self.normalize_cell(normalized_name, raw_value)
                            if isinstance(normalized_value, ParseWarning):
                                parse_warning = normalized_value
                                # logger.info("parse warning: {} for data {},  {}".format(parse_warning, raw_column_name, row))
                                cell_errors[normalized_name] = self.make_package_file_warning(parse_warning)
                                normalized_row.setdefault(normalized_name, None)
                            else:
                                normalized_row.setdefault(normalized_name, normalized_value)
                        except Exception as e:
                            cell_errors[normalized_name] = self.make_package_file_warning(
                                ParseWarning.unknown, additional_msg=u"message: {}".format(e.message)
                            )

                if self.ignore_row(normalized_row):
                    continue

                normalized_row = self.translate_row(normalized_row)

                # keep the first issn in this row
                for issn_col in self.issn_columns():
                    if normalized_row.get(issn_col, None):
                        row_issn = normalized_row[issn_col]
                        [cell_errors.pop(c, None) for c in self.issn_columns()]  # delete errors for all issn columns
                        [normalized_row.pop(c, None) for c in self.issn_columns()] # delete issn columns
                        normalized_row["issn"] = row_issn
                        break

                if not cell_errors:
                    normalized_rows.append(normalized_row)
                else:
                    error_row = {
                        "row_id": {
                            "value": absolute_row_no,
                            "error": None
                        }
                    }

                    for normalized_name in normalized_to_raw_map.keys():
                        if normalized_name:
                            raw_name = normalized_to_raw_map[normalized_name]

                            error_row[normalized_name] = {
                                "value": row[raw_name],
                                "error": cell_errors.get(normalized_name, None)
                            }

                    error_rows["rows"].append(error_row)

            for normalized, raw in normalized_to_raw_map.items():
                if normalized:
                    error_rows["headers"].append({"id": normalized, "name": raw})

            self.apply_header(normalized_rows, parsed_rows[0:header_index+1])

            if not error_rows["rows"]:
                error_rows = None

            return normalized_rows, error_rows



    def load(self, package_id, file_name, commit=False):
        my_package = db.session.query(package.Package).filter(package.Package.package_id == package_id).scalar()

        try:
            normalized_rows, error_rows = self.normalize_rows(file_name, file_package=my_package)
        except (UnicodeError, csv.Error) as e:
            print u"normalize_rows error {}".format(e)
            self._copy_raw_to_s3(file_name, package_id, num_rows=None, error="error_reading_file")
            return {"success": False, "message": "error_reading_file", "warnings": []}
        except RuntimeError as e:
            print u"Runtime Error processing file {}".format(e.message)
            self._copy_raw_to_s3(file_name, package_id, num_rows=None, error="runtime_error")
            return {"success": False, "message": e.message, "warnings": []}

        # save normalized rows

        aws_creds = "aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}".format(
            aws_key=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret=os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        if normalized_rows:
            for row in normalized_rows:
                row.update({"package_id": package_id})
                # logger.info(u"normalized row: {}".format(json.dumps(row)))

            # delete what we've got
            from counter import CounterInput

            if isinstance(self, CounterInput):
                report_name = normalized_rows[1]["report_name"]
                report_version = normalized_rows[1]["report_version"]
                # make sure to delete counter 4 if loading counter 5, or vice versa
                if report_version == "4":
                    self.delete(package_id, "trj2")
                    self.delete(package_id, "trj3")
                    self.delete(package_id, "trj4")
                elif report_version == "5":
                    self.delete(package_id, "jr1")

                # and now delete the thing you are currently loading
                self.delete(package_id, report_name)

                # then set this for use further in the function
                self.set_file_type_label(report_name)
            else:
                self.delete(package_id)

            sorted_fields = sorted(normalized_rows[0].keys())
            normalized_csv_filename = tempfile.mkstemp()[1]
            num_rows = 0
            with open(normalized_csv_filename, "w") as normalized_csv_file:
                writer = csv.DictWriter(normalized_csv_file, delimiter=",", encoding="utf-8", fieldnames=sorted_fields)
                for row in normalized_rows:
                    num_rows += 1
                    writer.writerow(row)

            s3_object = self._copy_staging_csv_to_s3(normalized_csv_filename, package_id)

            copy_cmd = text("""
                copy {table} ({fields}) from '{s3_object}'
                credentials :creds format as csv
                timeformat 'auto';
            """.format(
                table=self.__tablename__,
                fields=", ".join(sorted_fields),
                s3_object=s3_object,
            ))

            print(copy_cmd.bindparams(creds=aws_creds))
            safe_commit(db)
            db.session.execute(copy_cmd.bindparams(creds=aws_creds))
            safe_commit(db)
            self.update_dest_table(package_id)
            self._copy_raw_to_s3(file_name, package_id, num_rows, error=None)
        else:
            self._copy_raw_to_s3(file_name, package_id, num_rows=0, error="no_useable_rows")

        # delete the current errors, save new errors
        # self.save_errors(package_id, error_rows)
        # db.session.flush()

        if commit:
            db.session.flush()  # see if this fixes Serializable isolation violation
            db.session.commit()
            if my_package:
                self.clear_caches(my_package)

        if normalized_rows:
            return {
                "success": True,
                "message": u"Inserted {} {} rows for package {}.".format(len(normalized_rows), self.__class__.__name__, package_id),
                "warnings": error_rows
            }
        else:
            return {
                "success": False,
                "message": u"No usable rows found.",
                "warnings": error_rows
            }


class ParseWarning(Enum):
    bad_issn = {
        "label": "bad_issn",
        "text": "This doesn't look like an ISSN."
    }
    unknown_issn = {
        "label": "unknown_issn",
        "text": "This looks like an ISSN, but it isn't one we recognize."
    }
    bundle_issn = {
        "label": "bundle_issn",
        "text": "ISSN represents a bundle of journals, not a single journal."
    }
    no_issn = {
        "label": "no_issn",
        "text": "No ISSN here."
    }
    bad_date = {
        "label": "bad_date",
        "text": "Unrecognized date format."
    }
    ambiguous_date = {
        "label": "ambiguous_date",
        "text": "Date must contain a 4-digit year."
    }
    bad_year = {
        "label": "bad_year",
        "text": "Unrecognized date or year."
    }
    bad_int = {
        "label": "bad_int",
        "text": "Unrecognized integer format."
    }
    no_int = {
        "label": "no_int",
        "text": "Expected an integer here."
    }
    bad_usd_price = {
        "label": "bad_usd_price",
        "text": "Unrecognized USD format."
    }
    no_usd_price = {
        "label": "no_usd_price",
        "text": "A price in USD is required here."
    }
    blank_text = {
        "label": "blank_text",
        "text": "Expected text here."
    }
    unknown = {
        "label": "unknown_error",
        "text": "There was an unexpected error parsing this cell. Try to correct the cell value or contact support."
    }
    row_error = {
        "label": "row_error",
        "text": "Error for this row. See cell warnings for details."
    }
    no_rows = {
        "label": "no_rows",
        "text": "No usable rows could be extracted."
    }
