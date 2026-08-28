from unittest import TestCase

from sds_utils.dashboard.backend.data import parse_asset_name
from sds_utils.dashboard.frontend.summary import (
    SUMMARY_DIMENSIONS,
    summarize_status_rows,
    summary_drilldown,
)


class SummaryTests(TestCase):
    def test_summary_dates_use_partition_start_timestamps(self) -> None:
        rows = [
            {
                "asset": "mag_l1d_first",
                "status": "materialized",
                "missing_file": "",
                "partition": "x_2026-01-03T12:00:00Z_to_2026-01-04T00:00:00Z",
            },
            {
                "asset": "mag_l1d_first",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-01T23:00:00Z_to_2026-01-02T00:00:00Z",
            },
        ]

        result = summarize_status_rows(rows, set(SUMMARY_DIMENSIONS))  # type: ignore[arg-type]

        self.assertEqual(result[0]["first_date"], "2026-01-01")
        self.assertEqual(result[0]["last_date"], "2026-01-03")

    def test_asset_name_parser_preserves_descriptor_underscores(self) -> None:
        self.assertEqual(
            parse_asset_name("mag_l1d_gradiometry_offsets_norm"),
            ("mag", "l1d", "gradiometry_offsets_norm"),
        )

    def test_failed_parse_uses_whole_asset_as_descriptor(self) -> None:
        self.assertEqual(parse_asset_name("unstructured"), (None, None, "unstructured"))

    def test_disabled_dimension_is_aggregated(self) -> None:
        rows = [
            {"asset": "mag_l1d_first", "status": "failed", "missing_file": ""},
            {
                "asset": "mag_l1d_second",
                "status": "materialized",
                "missing_file": "",
            },
        ]
        result = summarize_status_rows(rows, {"instrument", "data_level"})  # type: ignore[arg-type]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["failed"], 1)
        self.assertEqual(result[0]["materialized"], 1)
        self.assertEqual(result[0]["descriptor"], "")

    def test_missing_file_can_be_a_grouping_dimension(self) -> None:
        rows = [
            {"asset": "mag_l1d_first", "status": "skipped", "missing_file": "SPICE"},
            {"asset": "mag_l1d_second", "status": "skipped", "missing_file": "ultra_l1a"},
        ]
        result = summarize_status_rows(rows, {"missing_file"})  # type: ignore[arg-type]
        self.assertEqual(
            {row["missing_file"]: row["skipped"] for row in result},
            {"SPICE": 1, "ultra_l1a": 1},
        )

    def test_summary_can_group_by_single_days(self) -> None:
        rows = [
            {
                "asset": "mag_l1d_first",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
            {
                "asset": "mag_l1d_first",
                "status": "materialized",
                "missing_file": "",
                "partition": "x_2026-01-02T00:00:00Z_to_2026-01-03T00:00:00Z",
            },
        ]

        result = summarize_status_rows(rows, {"instrument"}, "day")  # type: ignore[arg-type]

        self.assertEqual(
            [(row["first_date"], row["last_date"]) for row in result],
            [("2026-01-01", "2026-01-01"), ("2026-01-02", "2026-01-02")],
        )

    def test_summary_can_group_by_weeks(self) -> None:
        rows = [
            {
                "asset": "mag_l1d_first",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-04T00:00:00Z_to_2026-01-05T00:00:00Z",
            },
            {
                "asset": "mag_l1d_first",
                "status": "materialized",
                "missing_file": "",
                "partition": "x_2026-01-05T00:00:00Z_to_2026-01-06T00:00:00Z",
            },
        ]

        result = summarize_status_rows(rows, {"instrument"}, "week")  # type: ignore[arg-type]

        self.assertEqual(
            [(row["first_date"], row["last_date"]) for row in result],
            [("2025-12-29", "2026-01-04"), ("2026-01-05", "2026-01-11")],
        )

    def test_summary_can_group_by_multi_day_periods(self) -> None:
        rows = [
            {
                "asset": "mag_l1d_first",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
            {
                "asset": "mag_l1d_first",
                "status": "materialized",
                "missing_file": "",
                "partition": "x_2026-01-02T00:00:00Z_to_2026-01-03T00:00:00Z",
            },
        ]

        result = summarize_status_rows(rows, {"instrument"}, "days", 3)  # type: ignore[arg-type]

        self.assertEqual(len(result), 1)
        self.assertEqual(
            (result[0]["first_date"], result[0]["last_date"]),
            ("2026-01-01", "2026-01-03"),
        )

    def test_drilldown_matches_enabled_dimensions_and_date_bucket(self) -> None:
        rows = [
            {
                "asset": "mag_l1d_first",
                "instrument": "mag",
                "data_level": "l1d",
                "descriptor": "first",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-02T00:00:00Z_to_2026-01-03T00:00:00Z",
            },
            {
                "asset": "mag_l1d_first",
                "instrument": "mag",
                "data_level": "l1d",
                "descriptor": "first",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-09T00:00:00Z_to_2026-01-10T00:00:00Z",
            },
            {
                "asset": "mag_l1d_second",
                "instrument": "mag",
                "data_level": "l1d",
                "descriptor": "second",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-02T00:00:00Z_to_2026-01-03T00:00:00Z",
            },
        ]
        summary = summarize_status_rows(
            rows, {"instrument", "descriptor"}, "week"
        )  # type: ignore[arg-type]
        first_group = next(
            row
            for row in summary
            if row["descriptor"] == "first" and row["first_date"] == "2025-12-29"
        )

        drilldown = summary_drilldown(
            first_group, {"instrument", "descriptor"}, "week"
        )

        self.assertTrue(drilldown.matches(rows[0]))  # type: ignore[arg-type]
        self.assertFalse(drilldown.matches(rows[1]))  # type: ignore[arg-type]
        self.assertFalse(drilldown.matches(rows[2]))  # type: ignore[arg-type]

    def test_all_dates_drilldown_does_not_add_a_date_constraint(self) -> None:
        rows = [
            {
                "asset": "mag_l1d_first",
                "instrument": "mag",
                "data_level": "l1d",
                "descriptor": "first",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-02T00:00:00Z_to_2026-01-03T00:00:00Z",
            }
        ]
        summary = summarize_status_rows(rows, {"instrument"})  # type: ignore[arg-type]

        drilldown = summary_drilldown(summary[0], {"instrument"}, "all")

        self.assertTrue(drilldown.matches(rows[0]))  # type: ignore[arg-type]

    def test_date_drilldown_keeps_unpartitioned_group_separate(self) -> None:
        rows = [
            {
                "asset": "mag_l1d_first",
                "instrument": "mag",
                "data_level": "l1d",
                "descriptor": "first",
                "status": "not-found",
                "missing_file": "",
                "partition": "",
            },
            {
                "asset": "mag_l1d_first",
                "instrument": "mag",
                "data_level": "l1d",
                "descriptor": "first",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-02T00:00:00Z_to_2026-01-03T00:00:00Z",
            },
        ]
        summary = summarize_status_rows(rows, {"instrument"}, "day")  # type: ignore[arg-type]
        unpartitioned_group = next(row for row in summary if not row["first_date"])

        drilldown = summary_drilldown(
            unpartitioned_group, {"instrument"}, "day"
        )

        self.assertTrue(drilldown.matches(rows[0]))  # type: ignore[arg-type]
        self.assertFalse(drilldown.matches(rows[1]))  # type: ignore[arg-type]
