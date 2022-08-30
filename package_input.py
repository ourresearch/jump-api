# coding: utf-8

import json
import os
import re
import tempfile
from re import sub
import requests

import babel.numbers
import dateutil.parser
import shortuuid
import csv
from enum import Enum
from sqlalchemy.sql import text
from psycopg2 import sql
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
from util import safe_commit



class PackageInput:
    @staticmethod
    def normalize_date(date_str, warn_if_blank=False, default=None):
        if date_str:
            try:
                parsed_date = dateutil.parser.parse(date_str, default=default)
                if str(parsed_date.year) not in date_str:
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
                if "." in price and "," in price and price.find(".") < price.find(","):
                    locale = "de"
                elif price.count(",") == 1 and (re.search(r"\d{4,},", price) or not re.search(r",\d{3}[$.]", price)):
                    locale = "de"
                else:
                    locale = "en"

                price = sub(r"[^\d.,]", "", price)
                parsed_price = babel.numbers.parse_decimal(price, locale=locale)
                return int(round(parsed_price))
            except babel.numbers.NumberFormatError:
                return ParseWarning.bad_usd_price
        else:
            return ParseWarning.no_usd_price if warn_if_blank else None

    @staticmethod
    def normalize_issn(issn, warn_if_blank=False):
        from openalex import oa_issns
        if issn:
            issn = issn.replace("issn:", "")
            issn = sub(r"\s", "", issn).upper()
            if re.match(r"^\d{4}-?\d{3}(?:X|\d)$", issn):
                issn = issn.replace("-", "")
                issn = issn[0:4] + "-" + issn[4:8]
                if issn not in oa_issns:
                    print(f"Missing journal in normalize_issn {issn} from OpenAlex: https://api.openalex.org/venues/issn:{issn}")
                    return ParseWarning.unknown_issn
                return issn
            elif re.match(r"^[A-Z0-9]{4}-\d{3}(?:X|\d)$", issn):
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

    def update_subscriptions(self):
        pass

    @cache
    def normalize_column_name(self, raw_column_name):
        for canonical_name, spec in list(self.csv_columns().items()):
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
        upload_bucket = "unsub-file-uploads-testing" if os.getenv("TESTING_DB") else "unsub-file-uploads"
        return upload_bucket

    def _copy_raw_to_s3(self, filename, package_id, num_rows=None, error=None, error_details=None):
        if "." in filename:
            suffix = ".{}".format(filename.split(".")[-1])
        else:
            suffix = ""

        object_name = "{}_{}{}".format(package_id, self.file_type_label(), suffix)
        bucket_name = self._raw_s3_bucket()

        s3_client.upload_file(filename, bucket_name, object_name)

        with get_db_cursor() as cursor:
            command = "delete from jump_raw_file_upload_object where package_id=%s and file=%s"
            cursor.execute(command, (package_id, self.file_type_label(),))

        if error and not error_details:
            error_details_dict = {
                "no_useable_rows": "No usable rows found.",
                "error_reading_file": "Error reading this file. Try opening this file, save in .xlsx format, and upload that."}
            error_details = error_details_dict.get("error", "Error processing file. Please email this file to support@unsub.org so the Unsub team can look into the problem.")

        new_object = RawFileUploadObject(
            package_id=package_id,
            file=self.file_type_label(),
            bucket_name=bucket_name,
            object_name=object_name,
            num_rows=num_rows,
            error=error,
            error_details=error_details
        )

        db.session.add(new_object)
        safe_commit(db)

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

    def set_to_delete(self, package_id, report_name=None):
        with get_db_cursor() as cursor:
            command = "update jump_raw_file_upload_object set to_delete_date=sysdate where package_id=%s and file=%s"
            cursor.execute(command, (package_id, self.file_type_label(),))
        return "Queued to delete"


    # report_name is used by CounterInput override
    def delete(self, package_id, report_name=None):
        sql_delete = "delete from {} where package_id=%s"
        with get_db_cursor() as cursor:
            sql1 = sql.SQL(sql_delete).format(sql.Identifier(self.__tablename__))
            cursor.execute(sql1, (package_id,))
            sql2 = sql.SQL(sql_delete).format(sql.Identifier(self.destination_table()))
            cursor.execute(sql2, (package_id,))
            cursor.execute("delete from jump_file_import_error_rows where package_id=%s and file=%s",
                (package_id, self.file_type_label(),))
            cursor.execute("delete from jump_raw_file_upload_object where package_id=%s and file=%s",
                (package_id, self.file_type_label(),))

        my_package = db.session.query(package.Package).filter(package.Package.package_id == package_id).scalar()
        if my_package:
            self.clear_caches(my_package)

        message = "Deleted {} rows for package {}.".format(self.__class__.__name__, package_id)
        print(message)
        return message


    def clear_caches(self, my_package):
        # print "clearing cache"
        if my_package.is_owned_by_consortium:
            print("clearing consortium cache for my_package.is_owned_by_consortium: {}".format(my_package))
            for consortium_scenario_id in my_package.consortia_scenario_ids_who_own_this_package:

                # this will actually recalculate all member institutions instead of just this one
                # which will take longer, but don't worry about it for now

                my_consortium = Consortium(consortium_scenario_id)
                email = "scott+{}@ourresearch.org".format(my_package.package_id)
                my_consortium.queue_for_recompute(email)
                reset_cache("consortium", "consortium_get_computed_data", consortium_scenario_id)

        # my_package.clear_package_counter_breakdown_cache() # not used anymore

    def update_dest_table(self, package_id):
        with get_db_cursor() as cursor:
            qry1 = sql.SQL("delete from {} where package_id=%s").format(sql.Identifier(self.destination_table()))
            cursor.execute(qry1, (package_id,))

            qry2 = sql.SQL("insert into {} (select * from {} where package_id=%s)").format(
                sql.Identifier(self.destination_table()), sql.Identifier(self.import_view_name()))
            cursor.execute(qry2, (package_id,))

    def make_package_file_warning(self, parse_warning, additional_msg=None):
        return {
            "label": parse_warning.value["label"],
            "message": "{}{}".format(
                parse_warning.value["text"],
                " {}".format(additional_msg) if additional_msg else ""
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
                errors = json.loads("".join([row.errors for row in rows]))
            except ValueError:
                print("ValueError in load_errors with ", package_id)
                return None

            if errors["rows"]:
                return errors
            else:
                return None

    def issn_columns(self):
        return []

    def validate_publisher(self):
        return False

    def is_single_column_file(self, csv_file):
        import re
        line = csv_file.readline().rstrip()
        return len(re.split(',|;|\\s', line)) == 1

    def normalize_rows(self, file_name, file_package=None):
        # convert to csv if needed
        if file_name.endswith(".xls") or file_name.endswith(".xlsx"):
            sheet_csv_file_names = convert_spreadsheet_to_csv(file_name, parsed=False)
            if not sheet_csv_file_names:
                raise RuntimeError("Error: Could not be opened as a spreadsheet.")

            if len(sheet_csv_file_names) > 1:
                raise RuntimeError("Error: Workbook contains multiple sheets.")

            file_name = sheet_csv_file_names[0]

        # convert to utf-8
        # very slow, try not doing this for now
        # file_name = convert_to_utf_8(file_name)
        # logger.info("converted file: {}".format(file_name))

        with open(file_name, "r", encoding="utf-8-sig") as csv_file:
            reader_params = {}

            if self.is_single_column_file(csv_file):
                dialect = None
                if file_name.endswith(".tsv"):
                    reader_params["delimiter"] = "\t"
            else:
                # determine the csv format
                dialect_sample = ""
                for i in range(0, 20):
                    next_line = csv_file.readline()
                    # ignore header row when determining dialect
                    # data rows should be more consistent with each other
                    if i > 0:
                        dialect_sample = str(dialect_sample) + str(next_line)
                    if not next_line:
                        break

                try:
                    dialect = csv.Sniffer().sniff(dialect_sample)
                    # logger.info(u"sniffed csv dialect:\n{}".format(json.dumps(vars(dialect), indent=2)))
                except csv.Error:
                    dialect = None
                    if file_name.endswith(".tsv"):
                        reader_params["delimiter"] = "\t"

            csv_file.seek(0)

            # turn rows into arrays - remember the first row that looks like a header
            max_columns = 0
            header_index = None
            parsed_rows = []
            line_no = 0  # the index in parsed_rows where this row will land
            absolute_line_no = 0  # the actual file row we're parsing
            parsed_to_absolute_line_no = {}

            for line in csv.reader(csv_file, dialect=dialect, **reader_params):
                absolute_line_no += 1
                if not any([cell.strip() for cell in line]):
                    continue

                parsed_rows.append(line)
                parsed_to_absolute_line_no[line_no] = absolute_line_no

                populated_columns = len([cell for cell in line if cell.strip()])
                if populated_columns > max_columns:
                    max_columns = populated_columns
                    header_index = line_no
                    logger.info("candidate header row: {}".format(", ".join(line)))

                line_no += 1

            if header_index is None:
                # give up. can't turn rows into dicts if we don't have a header
                raise RuntimeError("Error: Couldn't identify a header row in the file.")

            error_rows = {
                "rows": [],
                "headers": [{"id": "row_id", "name": "Row Number"}]
            }

            normalized_rows = []

            # make sure we have all the required columns
            self.raw_column_names = parsed_rows[header_index]
            normalized_column_names = [self.normalize_column_name(cn) for cn in self.raw_column_names]
            raw_to_normalized_map = dict(list(zip(self.raw_column_names, normalized_column_names)))
            normalized_to_raw_map = {}
            for k, v in list(raw_to_normalized_map.items()):
                normalized_to_raw_map[v] = k

            required_keys = [k for k, v in list(self.csv_columns().items()) if v.get("required", True)]

            # combine the header and data rows into dicts
            row_dicts = [dict(list(zip(parsed_rows[header_index], x))) for x in parsed_rows[header_index+1:]]

            if set(required_keys).difference(set(normalized_column_names)):
                raise RuntimeError("Error: missing required columns. Required: {}, Found: {}.".format(required_keys, self.raw_column_names))

            for row_no, row in enumerate(row_dicts):
                absolute_row_no = parsed_to_absolute_line_no[row_no] + header_index + 1
                normalized_row = {}
                cell_errors = {}

                for raw_column_name in list(row.keys()):
                    raw_value = row[raw_column_name]
                    normalized_name = self.normalize_column_name(raw_column_name)
                    if normalized_name:
                        try:
                            normalized_value = self.normalize_cell(normalized_name, raw_value)
                            if normalized_value.__class__.__name__ == "ParseWarning":
                                parse_warning = normalized_value
                                # logger.info("parse warning: {} for data {},  {}".format(parse_warning, raw_column_name, row))
                                cell_errors[normalized_name] = self.make_package_file_warning(parse_warning)
                                normalized_row.setdefault(normalized_name, None)
                            else:
                                normalized_row.setdefault(normalized_name, normalized_value)
                        except Exception as e:
                            cell_errors[normalized_name] = self.make_package_file_warning(
                                ParseWarning.unknown, additional_msg="message: {}".format(str(e))
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

                    for normalized_name in list(normalized_to_raw_map.keys()):
                        if normalized_name:
                            raw_name = normalized_to_raw_map[normalized_name]

                            error_row[normalized_name] = {
                                "value": row[raw_name],
                                "error": cell_errors.get(normalized_name, None)
                            }

                    error_rows["rows"].append(error_row)

            for normalized, raw in list(normalized_to_raw_map.items()):
                if normalized:
                    error_rows["headers"].append({"id": normalized, "name": raw})

            if not normalized_rows and not error_rows['rows']:
                raise RuntimeError("Error: No rows found")

            if normalized_rows:
                self.apply_header(normalized_rows, parsed_rows[0:header_index+1])

            if not error_rows["rows"]:
                error_rows = None

            return normalized_rows, error_rows



    def load(self, package_id, file_name, file_type, commit=False):
        my_package = db.session.query(package.Package).filter(package.Package.package_id == package_id).scalar()

        if "counter" in file_type:
            self.stored_file_type_label = file_type

        try:
            normalized_rows, error_rows = self.normalize_rows(file_name, file_package=my_package)
        except (UnicodeError, UnicodeDecodeError, csv.Error) as e:
            print("normalize_rows error {}".format(e))
            err_mssg = str(e)
            if "decode" in err_mssg:
                err_mssg += " (try fixing encoding issues, possibly saving file in a different format, and/or changing file encoding)"
            self._copy_raw_to_s3(file_name, package_id, num_rows=None, error="error_reading_file", error_details=err_mssg)
            return {"success": False, "message": "error_reading_file", "warnings": []}
        except RuntimeError as e:
            print("Runtime Error processing file: {}".format(str(e)))
            self._copy_raw_to_s3(file_name, package_id, num_rows=None, error="parsing_error", error_details=str(e))
            return {"success": False, "message": str(e), "warnings": []}

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
            with open(normalized_csv_filename, "w", encoding="utf-8") as normalized_csv_file:
                writer = csv.DictWriter(normalized_csv_file, delimiter=",", fieldnames=sorted_fields)
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

            print((copy_cmd.bindparams(creds=aws_creds)))
            safe_commit(db)
            db.session.execute(copy_cmd.bindparams(creds=aws_creds))
            safe_commit(db)
            self.update_dest_table(package_id)
            self._copy_raw_to_s3(file_name, package_id, num_rows, error=None)
            from filter_titles import FilterTitlesInput
            if isinstance(self, FilterTitlesInput):
                self.update_subscriptions(package_id)
        else:
            from collections import OrderedDict
            try:
                z = list(filter(lambda x: [v.get('error') for k, v in x.items()], error_rows['rows']))[0]
                errors_dct = OrderedDict()
                for k,v in z.items():
                    if v.get('error'):
                        errors_dct[k] = v.get('error')

                errors_tup = next(iter(errors_dct.items()))
                error_str = f"First error. Error reading column '{errors_tup[0]}': {errors_tup[1].get('message')}"
            except:
                error_str = None

            self._copy_raw_to_s3(file_name, package_id, num_rows=0, error="no_useable_rows", error_details=error_str)

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
                "message": "Inserted {} {} rows for package {}.".format(len(normalized_rows), self.__class__.__name__, package_id),
                "warnings": error_rows
            }
        else:
            return {
                "success": False,
                "message": "No usable rows found.",
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
