"""Tests for COUNTER 5.1 report parsing support.

Standalone tests that verify apply_header correctly identifies COUNTER 5.1
reports. Does not require DB connection.

Run with: python tests/test_counter51.py
"""

import re
import sys
import os


def apply_header_standalone(normalized_rows, header_rows):
    """Standalone copy of CounterInput.apply_header logic for testing without DB.

    This mirrors the logic in counter.py so we can test version detection
    without needing Flask/SQLAlchemy/DB dependencies.
    """
    version_labels = {
        "Journal Report 1 (R4)": {"report_version": "4", "report_name": "jr1"},
        "TR_J1": {"report_version": "5", "report_name": "trj1"},
        "TR_J2": {"report_version": "5", "report_name": "trj2"},
        "TR_J3": {"report_version": "5", "report_name": "trj3"},
        "TR_J4": {"report_version": "5", "report_name": "trj4"},
    }

    assigned_label = None
    normalized_header_text = "".join(
        [re.sub(r"\s*", "", "".join(row)).lower() for row in header_rows]
    )
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
        elif set([row.get("access_type", "") for row in normalized_rows]) & {
            "OA_Gold", "Open", "Free_To_Read"
        }:
            assigned_label = "TR_J3"

    if assigned_label:
        report_version = version_labels[assigned_label]["report_version"]
        report_name = version_labels[assigned_label]["report_name"]
    else:
        report_version = "4"
        report_name = "jr1"

    header_years = []
    for cell in header_rows[-1]:
        matches = re.findall(r"\b(\d{4})\b", cell)
        if len(matches) == 1:
            header_years.append(int(matches[0]))
        else:
            matches = re.findall(r"\b(\d{2})\b", cell)
            if len(matches) == 1:
                header_years.append(2000 + int(matches[0]))

    report_year = (
        sorted(header_years)[int(len(header_years) / 2)] if header_years else None
    )

    for row in normalized_rows:
        row["report_year"] = report_year
        row["report_version"] = report_version
        row["report_name"] = report_name

    return normalized_rows


def test_counter51_trj3_detected_from_header():
    """COUNTER 5.1 TR_J3 should be detected from Report_ID in header."""
    header_rows = [
        ["Report_Name", "Journal Usage by Access Type"],
        ["Report_ID", "TR_J3"],
        ["Release", "5.1"],
        ["Institution_Name", "Test Library"],
        ["Title", "Print_ISSN", "Access_Type", "Metric_Type",
         "Reporting_Period_Total", "Jan-25", "Feb-25"],
    ]
    normalized_rows = [
        {"issn": "0098-7484", "access_type": "Controlled",
         "metric_type": "Total_Item_Requests", "total": 100},
        {"issn": "0098-7484", "access_type": "Free_To_Read",
         "metric_type": "Total_Item_Requests", "total": 50},
        {"issn": "0098-7484", "access_type": "Open",
         "metric_type": "Total_Item_Requests", "total": 20},
    ]

    result = apply_header_standalone(normalized_rows, header_rows)
    assert result[0]["report_version"] == "5"
    assert result[0]["report_name"] == "trj3"
    assert result[0]["report_year"] == 2025
    for row in result:
        assert row["report_version"] == "5"
        assert row["report_name"] == "trj3"
    print("  PASS: test_counter51_trj3_detected_from_header")


def test_counter51_trj3_fallback_with_open():
    """5.1 TR_J3 should be detected via fallback when 'Open' is in access_type."""
    header_rows = [
        ["", ""],
        ["Title", "Print_ISSN", "Access_Type", "Metric_Type",
         "Reporting_Period_Total", "Jan-25"],
    ]
    normalized_rows = [
        {"issn": "0098-7484", "access_type": "Open",
         "metric_type": "Total_Item_Requests", "total": 20},
    ]

    result = apply_header_standalone(normalized_rows, header_rows)
    assert result[0]["report_version"] == "5"
    assert result[0]["report_name"] == "trj3"
    print("  PASS: test_counter51_trj3_fallback_with_open")


def test_counter51_trj3_fallback_with_free_to_read():
    """5.1 TR_J3 should be detected via fallback when 'Free_To_Read' is in access_type."""
    header_rows = [
        ["", ""],
        ["Title", "Print_ISSN", "Access_Type", "Metric_Type",
         "Reporting_Period_Total", "Jan-25"],
    ]
    normalized_rows = [
        {"issn": "0098-7484", "access_type": "Free_To_Read",
         "metric_type": "Total_Item_Requests", "total": 50},
    ]

    result = apply_header_standalone(normalized_rows, header_rows)
    assert result[0]["report_version"] == "5"
    assert result[0]["report_name"] == "trj3"
    print("  PASS: test_counter51_trj3_fallback_with_free_to_read")


def test_counter50_trj3_fallback_still_works():
    """5.0 TR_J3 fallback with 'OA_Gold' should still work (regression)."""
    header_rows = [
        ["", ""],
        ["Title", "Print_ISSN", "Access_Type", "Metric_Type",
         "Reporting_Period_Total", "Jan-19"],
    ]
    normalized_rows = [
        {"issn": "0098-7484", "access_type": "OA_Gold",
         "metric_type": "Total_Item_Requests", "total": 100},
    ]

    result = apply_header_standalone(normalized_rows, header_rows)
    assert result[0]["report_version"] == "5"
    assert result[0]["report_name"] == "trj3"
    print("  PASS: test_counter50_trj3_fallback_still_works")


def test_counter51_preserves_access_type_values():
    """5.1 rows should preserve their distinct access_type values."""
    header_rows = [
        ["Report_ID", "TR_J3"],
        ["Release", "5.1"],
        ["Title", "Print_ISSN", "Access_Type", "Metric_Type",
         "Reporting_Period_Total", "Jan-25"],
    ]
    normalized_rows = [
        {"issn": "0098-7484", "access_type": "Controlled",
         "metric_type": "Unique_Item_Requests", "total": 80},
        {"issn": "0098-7484", "access_type": "Free_To_Read",
         "metric_type": "Unique_Item_Requests", "total": 40},
        {"issn": "0098-7484", "access_type": "Open",
         "metric_type": "Unique_Item_Requests", "total": 15},
    ]

    result = apply_header_standalone(normalized_rows, header_rows)
    access_types = [row["access_type"] for row in result]
    assert "Controlled" in access_types
    assert "Free_To_Read" in access_types
    assert "Open" in access_types
    total = sum(row["total"] for row in result)
    assert total == 135
    print("  PASS: test_counter51_preserves_access_type_values")


def test_counter51_trj2_detected():
    """COUNTER 5.1 TR_J2 should be detected from header."""
    header_rows = [
        ["Report_ID", "TR_J2"],
        ["Release", "5.1"],
        ["Title", "Print_ISSN", "Metric_Type", "Reporting_Period_Total", "Jan-25"],
    ]
    normalized_rows = [
        {"issn": "0098-7484", "metric_type": "No_License", "total": 5},
    ]

    result = apply_header_standalone(normalized_rows, header_rows)
    assert result[0]["report_version"] == "5"
    assert result[0]["report_name"] == "trj2"
    print("  PASS: test_counter51_trj2_detected")


def test_counter51_trj4_detected():
    """COUNTER 5.1 TR_J4 should be detected from header."""
    header_rows = [
        ["Report_ID", "TR_J4"],
        ["Release", "5.1"],
        ["Title", "Print_ISSN", "YOP", "Metric_Type",
         "Reporting_Period_Total", "Jan-25"],
    ]
    normalized_rows = [
        {"issn": "0098-7484", "yop": 2020,
         "metric_type": "Total_Item_Requests", "total": 10},
    ]

    result = apply_header_standalone(normalized_rows, header_rows)
    assert result[0]["report_version"] == "5"
    assert result[0]["report_name"] == "trj4"
    print("  PASS: test_counter51_trj4_detected")


if __name__ == "__main__":
    tests = [
        test_counter51_trj3_detected_from_header,
        test_counter51_trj3_fallback_with_open,
        test_counter51_trj3_fallback_with_free_to_read,
        test_counter50_trj3_fallback_still_works,
        test_counter51_preserves_access_type_values,
        test_counter51_trj2_detected,
        test_counter51_trj4_detected,
    ]

    print("Running COUNTER 5.1 tests...")
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR: {test.__name__}: {e}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed out of {len(tests)} tests")
    sys.exit(1 if failed else 0)
