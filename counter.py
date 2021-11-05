# coding: utf-8

from cached_property import cached_property
import re
import calendar
from kids.cache import cache

from util import safe_commit
from app import db
from app import logger
from app import get_db_cursor
from package_input import PackageInput
from psycopg2.extensions import AsIs

class Counter(db.Model):
    __tablename__ = "jump_counter"
    issn_l = db.Column(db.Text, primary_key=True)
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    journal_name = db.Column(db.Text)
    total = db.Column(db.Numeric(asdecimal=False))
    report_version = db.Column(db.Text, primary_key=True)
    report_name = db.Column(db.Text, primary_key=True)
    report_year = db.Column(db.Numeric(asdecimal=False))
    metric_type = db.Column(db.Text, primary_key=True)
    access_type = db.Column(db.Text, primary_key=True)
    yop = db.Column(db.Numeric(asdecimal=False), primary_key=True)

    def to_dict(self):
        return {
            "issn_l": self.issn_l,
            "package_id": self.package_id,
            "journal_name": self.journal_name,
            "total": self.total,
            "report_version": self.report_version,
            "report_name": self.report_name,
            "report_year": self.report_year,
            "metric_type": self.metric_type,
            "access_type": self.access_type,
            "yop": self.yop,
        }


class CounterInput(db.Model, PackageInput):
    __tablename__ = "jump_counter_input"
    report_version = db.Column(db.Text, primary_key=True)
    report_name = db.Column(db.Text, primary_key=True)
    report_year = db.Column(db.Numeric(asdecimal=False))
    metric_type = db.Column(db.Text, primary_key=True)
    yop = db.Column(db.Numeric(asdecimal=False), primary_key=True)
    access_type = db.Column(db.Text, primary_key=True)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)
    issn = db.Column(db.Text, primary_key=True)
    journal_name = db.Column(db.Text)
    total = db.Column(db.Numeric(asdecimal=False))
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)

    def calculate_file_type_label(self, report_name):
        if not report_name or (report_name == "jr1"):
            return "counter"
        else:
            return "counter-{}".format(report_name.lower())

    def set_file_type_label(self, report_name):
        self.stored_file_type_label = self.calculate_file_type_label(report_name)

    def file_type_label(self):
        if hasattr(self, "stored_file_type_label"):
            return self.stored_file_type_label
        return "counter"

    def import_view_name(self):
        return "jump_counter_view"

    def destination_table(self):
        return Counter.__tablename__

    @cache
    def csv_columns(self):
        columns = {
            "print_issn": {
                "normalize": self.normalize_issn,
                "name_snippets": ["print issn", "print_issn", "issn"],
                "excluded_name_snippets": ["online", "e-", "eissn"],
                "warn_if_blank": True,
            },
            "online_issn": {
                "normalize": self.normalize_issn,
                "name_snippets": ["online issn", "online_issn", "eissn"],
                "exact_name": True,
                "required": False,
                "warn_if_blank": True,
            },
            "total": {
                "normalize": self.normalize_int,
                "name_snippets": ["total"],
                "warn_if_blank": True,
                "required": True,
            },
            "journal_name": {
                "normalize": self.strip_text,
                "name_snippets": ["title", "journal", "journal_name"],
                "exact_name": True,
                "required": False,
            },
            "metric_type": {
                "normalize": self.strip_text,
                "name_snippets": ["metric_type"],
                "exact_name": True,
                "required": False,
            },
            "yop": {
                "normalize": self.normalize_int,
                "name_snippets": ["yop"],
                "exact_name": True,
                "required": False,
            },
            "access_type": {
                "normalize": self.strip_text,
                "name_snippets": ["access_type"],
                "exact_name": True,
                "required": False,
            },
        }
        # for month_idx in range(1, 13):
        #     month_column = {
        #         "normalize": self.normalize_int,
        #         "name_snippets": [calendar.month_abbr[month_idx].lower(), u"-{:02d}".format(month_idx)],
        #         "warn_if_blank": False,
        #         "required": False,
        #     }
        #     columns[calendar.month_abbr[month_idx].lower()] = month_column

        return columns

    def ignore_row(self, row):
        journal_name = (row.get("journal_name", "") or "").lower()
        if (not journal_name or "all journals" in journal_name) and row.get("print_issn", None) is None:
            return True

        return False

    def issn_columns(self):
        return ["print_issn", "online_issn"]

    def apply_header(self, normalized_rows, header_rows):
        # get the counter version and file format
        version_labels = {
            "Journal Report 1 (R4)": {
                "report_version": "4",
                "report_name": "jr1"
            },
            "TR_J1": {
                "report_version": "5",
                "report_name": "trj1",
            },
            "TR_J2": {
                "report_version": "5",
                "report_name": "trj2",
            },
            "TR_J3": {
                "report_version": "5",
                "report_name": "trj3",
            },
            "TR_J4": {
                "report_version": "5",
                "report_name": "trj4",
            },
        }

        assigned_label = None

        normalized_header_text = "".join([re.sub(r"\s*", "", "".join(row)).lower() for row in header_rows])
        for label in version_labels:
            normalized_label = re.sub(r"\s*", "", label).lower()
            if normalized_label in normalized_header_text:
                assigned_label = label

        if not assigned_label:
            first_row = normalized_rows[0]
            if "metric_type" not in first_row:
                assigned_label = "Journal Report 1 (R4)"
            elif "yop" in first_row:
                assigned_label = "TR_J4"
            elif first_row["metric_type"] == "No_License":
                assigned_label = "TR_J2"
            elif "OA_Gold" in [row.get("access_type", "") for row in normalized_rows]:
                assigned_label = "TR_J3"

        if assigned_label:
            print("Recognized the file type as {}".format(version_labels[assigned_label]))
            report_version = version_labels[assigned_label]["report_version"]
            report_name = version_labels[assigned_label]["report_name"]
        else:
            print("Warning: Didn't recognize the counter file type")
            report_version = "4"
            report_name = "jr1"

        # get the year
        # get the header rows that look like months
        # Mar/18, 2017-12-01 00:00:00
        header_years = []
        for cell in header_rows[-1]:
            matches = re.findall(r"\b(\d{4})\b", cell)
            if len(matches) == 1:
                header_years.append(int(matches[0]))
            else:
                matches = re.findall(r"\b(\d{2})\b", cell)
                if len(matches) == 1:
                    header_years.append(2000 + int(matches[0]))

        # sort them and take the one in the middle to be the year
        report_year = sorted(header_years)[int(len(header_years)/2)] if header_years else None

        if report_year is None:
            logger.warn("Couldn't guess a year from column headers: {}".format(header_rows[-1]))


        for row in normalized_rows:
            row["report_year"] = report_year
            row["report_version"] = report_version
            row["report_name"] = report_name

        return normalized_rows

    def set_to_delete(self, package_id, report_name=None):
        if report_name:
            report_name = report_name.lower()

        if report_name == None:
            report_name = "jr1"

        with get_db_cursor() as cursor:
            command = "update jump_raw_file_upload_object set to_delete_date=sysdate where package_id=%(package_id)s and file=%(file)s"
            values = {'package_id': package_id, 'file': self.calculate_file_type_label(report_name)}
            print(cursor.mogrify(command, values))
            cursor.execute(command, values)

        return "Queued to delete"

    def sql_delete(self, cursor, sql_str, tablename, package_id, report_or_file):
        values = {'table': AsIs(tablename), 'id': package_id, 'rname_or_file': report_or_file}
        cursor.execute(sql_str, values)

    def delete(self, package_id, report_name=None):
        # DELETE to /publisher/<publisher_id>/counter/trj2  (or trj3, trj4)
        # DELETE to /publisher/<publisher_id>/counter/jr1 will keep deleting everything
        # DELETE to /publisher/<publisher_id>/counter will keep deleting everything

        if report_name:
            report_name = report_name.lower()

        if report_name == None:
            report_name = "jr1"

        # delete select files if counter 5, else delete all counter data of all report names, including null
        command_delete_or = "DELETE FROM %(table)s where package_id=%(id)s and ((report_name is null) or (report_name=%(rname_or_file)s))"
        command_delete = "DELETE FROM %(table)s where package_id=%(id)s and file=%(rname_or_file)s"
        command_delete_report = "DELETE FROM %(table)s where package_id=%(id)s and report_name=%(rname_or_file)s"
        if report_name == "jr1":
            with get_db_cursor() as cursor:
                self.sql_delete(cursor, command_delete_or, self.__tablename__, package_id, report_name)
                self.sql_delete(cursor, command_delete_or, self.destination_table(), package_id, report_name)
                self.sql_delete(cursor, command_delete, 'jump_raw_file_upload_object', package_id, self.calculate_file_type_label(report_name))
                self.sql_delete(cursor, command_delete, 'jump_file_import_error_rows', package_id, self.calculate_file_type_label(report_name))
        else:
            with get_db_cursor() as cursor:
                self.sql_delete(cursor, command_delete_report, self.__tablename__, package_id, report_name)
                self.sql_delete(cursor, command_delete_report, self.destination_table(), package_id, report_name)
                self.sql_delete(cursor, command_delete, 'jump_raw_file_upload_object', package_id, self.calculate_file_type_label(report_name))

        from package import Package
        my_package = db.session.query(Package).filter(Package.package_id == package_id).scalar()
        if my_package:
            self.clear_caches(my_package)

        message = "Deleted {} rows for package {}.".format(self.__class__.__name__, package_id)
        print(message)
        return message
