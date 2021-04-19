# coding: utf-8

from cached_property import cached_property
import re
import calendar

from util import safe_commit
from app import db, logger
from package_input import PackageInput

class Counter(db.Model):
    __tablename__ = "jump_counter"
    issn_l = db.Column(db.Text, primary_key=True)
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)
    journal_name = db.Column(db.Text)
    total = db.Column(db.Numeric)

    def to_dict(self):
        return {
            "issn_l": self.issn_l,
            "package_id": self.package_id,
            "journal_name": self.journal_name,
            "total": self.total,
        }


class CounterInput(db.Model, PackageInput):
    __tablename__ = "jump_counter_input"
    report_version = db.Column(db.Text)
    report_name = db.Column(db.Text)
    report_year = db.Column(db.Numeric)
    metric_type = db.Column(db.Text)
    yop = db.Column(db.Numeric)
    access_type = db.Column(db.Text)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)
    issn = db.Column(db.Text, primary_key=True)
    journal_name = db.Column(db.Text)
    total = db.Column(db.Numeric)
    package_id = db.Column(db.Text, db.ForeignKey("jump_account_package.package_id"), primary_key=True)

    def calculate_file_type_label(report_name):
        if not report_name or (report_name == "jr1"):
            return u"counter"
        else:
            return u"counter-{}".format(report_name.lower())

    def set_file_type_label(self, report_name):
        self.stored_file_type_label = self.calculate_file_type_label(report_name)

    def file_type_label(self):
        if hasattr(self, "stored_file_type_label"):
            return self.stored_file_type_label
        return u"counter"

    def import_view_name(self):
        return "jump_counter_view"

    def destination_table(self):
        return Counter.__tablename__

    def csv_columns(self):
        columns = {
            "print_issn": {
                "normalize": self.normalize_issn,
                "name_snippets": [u"print issn", u"print_issn", u"issn"],
                "excluded_name_snippets": [u"online", u"e-", u"eissn"],
                "warn_if_blank": True,
            },
            "online_issn": {
                "normalize": self.normalize_issn,
                "name_snippets": [u"online issn", u"online_issn", u"eissn"],
                "exact_name": True,
                "required": False,
                "warn_if_blank": True,
            },
            "total": {
                "normalize": self.normalize_int,
                "name_snippets": [u"total"],
                "warn_if_blank": True,
                "required": True,
            },
            "journal_name": {
                "normalize": self.strip_text,
                "name_snippets": [u"title", u"journal", u"journal_name"],
                "exact_name": True,
                "required": False,
            },
            "metric_type": {
                "normalize": self.strip_text,
                "name_snippets": [u"metric_type"],
                "exact_name": True,
                "required": False,
            },
            "yop": {
                "normalize": self.normalize_int,
                "name_snippets": [u"yop"],
                "exact_name": True,
                "required": False,
            },
            "access_type": {
                "normalize": self.strip_text,
                "name_snippets": [u"access_type"],
                "exact_name": True,
                "required": False,
            },
        }
        for month_idx in range(1, 13):
            month_column = {
                "normalize": self.normalize_int,
                "name_snippets": [calendar.month_abbr[month_idx].lower(), u"-{:02d}".format(month_idx)],
                "warn_if_blank": False,
                "required": False,
            }
            columns[calendar.month_abbr[month_idx].lower()] = month_column

        return columns

    def ignore_row(self, row):
        journal_name = (row.get("journal_name", u"") or u"").lower()
        if (not journal_name or u"all journals" in journal_name) and row.get("print_issn", None) is None:
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

        normalized_header_text = u"".join([re.sub(ur"\s*", u"", u"".join(row)).lower() for row in header_rows])
        for label in version_labels:
            normalized_label = re.sub(ur"\s*", "", label).lower()
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
            elif "OA_Gold" in [row["access_type"] for row in normalized_rows]:
                assigned_label = "TR_J3"

        if assigned_label:
            print u"Recognized the file type as {}".format(version_labels[assigned_label])
            report_version = version_labels[assigned_label]["report_version"]
            report_name = version_labels[assigned_label]["report_name"]
        else:
            print u"Warning: Didn't recognize the counter file type"
            report_version = None
            report_name = None


        # check for COUNTER 5
        # cop5_error = u"Sorry, we don"t support COUNTER 5 yet. Please upload a COUNTER 4 JR_1 file."
        #
        # for header_row in header_rows:
        #     row_text = u"|".join([cell.strip() for cell in header_row]).lower()
        #     if u"report_id|tr_j1" in row_text or u"release|5" in row_text:
        #         raise RuntimeError(cop5_error)


        # get the year
        # get the header rows that look like months
        # Mar/18, 2017-12-01 00:00:00
        header_years = []
        for cell in header_rows[-1]:
            matches = re.findall(ur"\b(\d{4})\b", cell)
            if len(matches) == 1:
                header_years.append(int(matches[0]))
            else:
                matches = re.findall(ur"\b(\d{2})\b", cell)
                if len(matches) == 1:
                    header_years.append(2000 + int(matches[0]))

        # sort them and take the one in the middle to be the year
        report_year = sorted(header_years)[len(header_years)/2] if header_years else None

        if report_year is None:
            logger.warn(u"Couldn't guess a year from column headers: {}".format(header_rows[-1]))


        for row in normalized_rows:
            row["report_year"] = report_year
            row["report_version"] = report_version
            row["report_name"] = report_name

        return normalized_rows


    def delete(self, package_id, report_name=None):
        # DELETE to /publisher/<publisher_id>/counter/trj2  (or trj3, trj4)
        # DELETE to /publisher/<publisher_id>/counter/jr1 will keep deleting everything
        # DELETE to /publisher/<publisher_id>/counter will keep deleting everything

        if report_name:
            report_name = report_name.lower()

        if report_name == None:
            report_name = "jr1"

        # delete select files if counter 5, else delete all counter data of all report names, including null
        if report_name == "jr1":
            db.session.execute("delete from {} where package_id = '{}' and ((report_name is null) or (report_name = '{}'))".format(
                self.__tablename__, package_id, report_name))

            db.session.execute("delete from {} where package_id = '{}' and ((report_name is null) or (report_name = '{}'))".format(
                self.destination_table(), package_id, report_name))

            db.session.execute("delete from jump_raw_file_upload_object where package_id = '{}' and file = '{}'".format(
                package_id, self.calculate_file_type_label(report_name)))

            db.session.execute("delete from jump_file_import_error_rows where package_id = '{}' and file = '{}'".format(
                package_id, self.calculate_file_type_label(report_name)))

        else:
            db.session.execute("delete from {} where package_id = '{}' and report_name = '{}'".format(
                self.__tablename__, package_id, report_name))

            db.session.execute("delete from {} where package_id = '{}' and report_name = '{}'".format(
                self.destination_table(), package_id, report_name))

            db.session.execute("delete from jump_raw_file_upload_object where package_id = '{}' and file = '{}'".format(
                package_id, self.calculate_file_type_label(report_name), report_name))

            if report_name == "trj2":
                db.session.execute("delete from jump_file_import_error_rows where package_id = '{}' and file = '{}'".format(
                    package_id, self.calculate_file_type_label(report_name), report_name))

        safe_commit(db)

        from package import Package
        my_package = db.session.query(Package).filter(Package.package_id == package_id).scalar()
        if my_package:
            self.clear_caches(my_package)


        return u"Deleted {} rows for package {}.".format(self.__class__.__name__, package_id)