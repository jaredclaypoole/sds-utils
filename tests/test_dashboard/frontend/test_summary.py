from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock
from types import SimpleNamespace

from sds_utils.dashboard.backend.data import parse_asset_name
from sds_utils.dashboard.frontend.elems import (
    AssetsStatusSnapshotTable,
    AssetsStatusView,
    SnapshotDataLevelFilter,
    SnapshotInstrumentFilter,
    SnapshotPartitionTypeFilter,
    SortRule,
)
from sds_utils.dashboard.frontend.summary import (
    SUMMARY_DIMENSIONS,
    snapshot_status_rows,
    summarize_status_rows,
    summary_drilldown,
)


class SummaryTests(TestCase):
    def test_snapshot_padding_uses_maximum_for_each_instrument_and_status(self) -> None:
        table = AssetsStatusSnapshotTable()
        table.source_rows = [
            {
                "asset": f"codice_l1a_first_{index}",
                "status": "materialized",
                "missing_file": "",
                "partition": "x_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            }
            for index in range(10)
        ] + [
            {
                "asset": "codice_l1a_second",
                "status": "materialized",
                "missing_file": "",
                "partition": "x_2026-01-02T00:00:00Z_to_2026-01-03T00:00:00Z",
            },
            {
                "asset": "codice_l1a_third",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-02T00:00:00Z_to_2026-01-03T00:00:00Z",
            },
        ]
        table.date_aggregation = "day"
        table.aggregation_days = 2
        table.table = MagicMock()

        table._apply()

        codice_parts = table.table.rows[0]["codice"]
        self.assertEqual(codice_parts[0]["text"], "10")
        self.assertEqual(table.table.rows[1]["codice"][0]["text"], " 1")
        self.assertEqual(table.table.rows[1]["codice"][2]["text"], "1")

    def test_snapshot_uses_data_level_columns_for_one_instrument(self) -> None:
        table = AssetsStatusSnapshotTable("mag")
        table.source_rows = [
            {
                "asset": "mag_l1a_first",
                "status": "materialized",
                "missing_file": "",
                "partition": "x_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
            {
                "asset": "mag_l2_second",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
            {
                "asset": "codice_l3_ignored",
                "status": "skipped",
                "missing_file": "",
                "partition": "x_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
        ]
        table.table = MagicMock()

        table._apply()

        self.assertEqual(table.groups, ["l1a", "l2"])
        self.assertEqual(
            [column["name"] for column in table.table.columns],
            ["first_date", "last_date", "l1a", "l2"],
        )
        self.assertEqual(table.table.rows[0]["l1a"][0]["text"], "1")
        self.assertEqual(table.table.rows[0]["l2"][2]["text"], "1")

    def test_snapshot_respects_and_displays_date_sort_directions(self) -> None:
        table = AssetsStatusSnapshotTable("mag")
        table.source_rows = [
            {
                "asset": "mag_l1a_first",
                "status": "materialized",
                "missing_file": "",
                "partition": f"x_2026-01-{day:02d}T00:00:00Z_to_2026-01-{day + 1:02d}T00:00:00Z",
            }
            for day in (1, 2)
        ]
        table.date_aggregation = "day"
        table.table = MagicMock()

        table.set_sorting(
            [SortRule("first_date", descending=True), SortRule("last_date")]
        )

        self.assertEqual(
            [row["first_date"] for row in table.table.rows],
            ["2026-01-02", "2026-01-01"],
        )
        directions = {
            column["name"]: column["sort_direction"]
            for column in table.table.columns
        }
        self.assertEqual(directions["first_date"], "desc")
        self.assertEqual(directions["last_date"], "asc")

    def test_snapshot_filters_selected_partition_types(self) -> None:
        table = AssetsStatusSnapshotTable("mag")
        table.source_rows = [
            {
                "asset": "mag_l1a_first",
                "status": "materialized",
                "missing_file": "",
                "partition": "daily_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
            {
                "asset": "mag_l1a_second",
                "status": "failed",
                "missing_file": "",
                "partition": "repoint42_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
            {
                "asset": "mag_l1a_third",
                "status": "skipped",
                "missing_file": "",
                "partition": "idex10day_2026-01-01T00:00:00Z_to_2026-01-11T00:00:00Z",
            },
        ]
        table.table = MagicMock()

        table.set_partition_types({"daily", "idex10day"})

        status_parts = table.table.rows[0]["l1a"]
        self.assertEqual(status_parts[0]["text"], "1")
        self.assertEqual(status_parts[2]["text"], "0")
        self.assertEqual(status_parts[3]["text"], "1")

    def test_snapshot_filters_selected_data_levels(self) -> None:
        table = AssetsStatusSnapshotTable("mag")
        table.source_rows = [
            {
                "asset": "mag_l1a_first",
                "status": "materialized",
                "missing_file": "",
                "partition": "daily_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
                "data_level": "l1a",
            },
            {
                "asset": "mag_ancillary_second",
                "status": "failed",
                "missing_file": "",
                "partition": "daily_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
                "data_level": "ancillary",
            },
        ]
        table.table = MagicMock()

        table.set_data_levels({"ancillary"})

        self.assertEqual(table.groups, ["ancillary"])
        self.assertEqual(table.table.rows[0]["ancillary"][2]["text"], "1")

    def test_data_level_filter_categories_special_prefixes_and_other(self) -> None:
        self.assertEqual(SnapshotDataLevelFilter._category("l1a"), "l1")
        self.assertEqual(SnapshotDataLevelFilter._category("L3"), "l3")
        self.assertEqual(SnapshotDataLevelFilter._category("ancillary"), "other")

    def test_snapshot_instrument_filter_categories(self) -> None:
        for instrument in ("lo", "hi", "ultra"):
            self.assertEqual(SnapshotInstrumentFilter._category(instrument), "ena")
        for instrument in ("codice", "hit", "idex", "glows", "mag", "swapi", "swe"):
            self.assertEqual(
                SnapshotInstrumentFilter._category(instrument), "in_situ"
            )
        self.assertEqual(
            SnapshotInstrumentFilter._category("spacecraft"), "other"
        )

    def test_all_instrument_snapshot_filter_hides_rows_and_columns(self) -> None:
        table = AssetsStatusSnapshotTable("all")
        table.source_rows = [
            {
                "asset": "mag_l1a_first",
                "instrument": "mag",
                "data_level": "l1a",
                "status": "materialized",
                "missing_file": "",
                "partition": "daily_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
            {
                "asset": "lo_l1a_second",
                "instrument": "lo",
                "data_level": "l1a",
                "status": "failed",
                "missing_file": "",
                "partition": "daily_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
        ]
        table.table = MagicMock()

        table.set_instruments({"lo"})

        self.assertEqual(table.groups, ["lo"])
        self.assertEqual(table.table.rows[0]["lo"][2]["text"], "1")

    def test_snapshot_instrument_aggregation_modes(self) -> None:
        table = AssetsStatusSnapshotTable("all")
        table.source_rows = [
            {
                "asset": "mag_l1a_daily",
                "instrument": "mag",
                "data_level": "l1a",
                "status": "materialized",
                "missing_file": "",
                "partition": "daily_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
            {
                "asset": "mag_l1a_repoint",
                "instrument": "mag",
                "data_level": "l1a",
                "status": "failed",
                "missing_file": "",
                "partition": "repoint2_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
            {
                "asset": "mag_l1a_aggregate",
                "instrument": "mag",
                "data_level": "l1a",
                "status": "skipped",
                "missing_file": "",
                "partition": "ten_day_2026-01-01T00:00:00Z_to_2026-01-11T00:00:00Z",
            },
        ]
        table.table = MagicMock()
        table.set_instruments({"mag"})

        table.set_instrument_aggregation("separate")
        self.assertEqual(table.groups, ["mag-short", "mag-agg"])
        self.assertEqual(table.table.rows[0]["mag-short"][0]["text"], "1")
        self.assertEqual(table.table.rows[0]["mag-short"][2]["text"], "1")
        self.assertEqual(table.table.rows[0]["mag-agg"][3]["text"], "1")

        table.set_instrument_aggregation("daily")
        self.assertEqual(table.table.rows[0]["mag"][0]["text"], "1")
        self.assertEqual(table.table.rows[0]["mag"][2]["text"], "0")
        self.assertEqual(table.table.rows[0]["mag"][3]["text"], "0")

        table.set_instrument_aggregation("agg")
        self.assertEqual(table.table.rows[0]["mag"][3]["text"], "1")

    def test_separate_mode_hides_empty_short_and_agg_columns(self) -> None:
        table = AssetsStatusSnapshotTable("all")
        table.source_rows = [
            {
                "asset": "mag_l1a_daily",
                "instrument": "mag",
                "data_level": "l1a",
                "status": "materialized",
                "missing_file": "",
                "partition": "daily_2026-01-01T00:00:00Z_to_2026-01-02T00:00:00Z",
            },
            {
                "asset": "lo_l1a_aggregate",
                "instrument": "lo",
                "data_level": "l1a",
                "status": "failed",
                "missing_file": "",
                "partition": "ten_day_2026-01-01T00:00:00Z_to_2026-01-11T00:00:00Z",
            },
        ]
        table.table = MagicMock()
        table.set_instruments({"mag", "lo"})

        table.set_instrument_aggregation("separate")

        self.assertEqual(table.groups, ["mag", "lo"])
        self.assertEqual(table.aggregation_for_group("mag"), "short")
        self.assertEqual(table.aggregation_for_group("lo"), "agg")

    def test_snapshot_filter_all_parents_toggle_every_available_child(self) -> None:
        partition_filter = object.__new__(SnapshotPartitionTypeFilter)
        partition_filter._updating = False
        partition_filter.available_types = {"daily", "repoint", "idex10day"}
        partition_filter.pending_types = {"daily"}
        partition_filter._sync_checkboxes = MagicMock()

        partition_filter._all_changed(SimpleNamespace(value=True))
        self.assertEqual(
            partition_filter.pending_types,
            {"daily", "repoint", "idex10day"},
        )
        partition_filter._all_changed(SimpleNamespace(value=False))
        self.assertEqual(partition_filter.pending_types, set())

        level_filter = object.__new__(SnapshotDataLevelFilter)
        level_filter._updating = False
        level_filter.available_levels = {"l1a", "l2", "ancillary"}
        level_filter.pending_levels = {"l1a"}
        level_filter._sync_checkboxes = MagicMock()

        level_filter._all_changed(SimpleNamespace(value=True))
        self.assertEqual(
            level_filter.pending_levels,
            {"l1a", "l2", "ancillary"},
        )
        level_filter._all_changed(SimpleNamespace(value=False))
        self.assertEqual(level_filter.pending_levels, set())

    def test_dismissing_snapshot_filter_menus_commits_pending_values(self) -> None:
        partition_filter = object.__new__(SnapshotPartitionTypeFilter)
        partition_filter.pending_types = {"daily"}
        partition_filter.selected_types = {"daily", "repoint"}
        partition_filter.available_types = {"daily", "repoint"}
        partition_filter.button = MagicMock()
        partition_filter.on_apply = MagicMock()

        partition_filter._menu_hidden()

        self.assertEqual(partition_filter.selected_types, {"daily"})
        partition_filter.on_apply.assert_called_once_with({"daily"})

        level_filter = object.__new__(SnapshotDataLevelFilter)
        level_filter.pending_levels = {"l1a", "ancillary"}
        level_filter.selected_levels = {"l1a"}
        level_filter.available_levels = {"l1a", "ancillary"}
        level_filter.button = MagicMock()
        level_filter.on_apply = MagicMock()

        level_filter._menu_hidden()

        self.assertEqual(level_filter.selected_levels, {"l1a", "ancillary"})
        level_filter.on_apply.assert_called_once_with({"l1a", "ancillary"})

    def test_snapshot_filter_buttons_summarize_selected_child_groups(self) -> None:
        partition_filter = object.__new__(SnapshotPartitionTypeFilter)
        partition_filter.available_types = {
            "daily",
            "repoint",
            "idex10day",
            "weekly",
        }
        partition_filter.selected_types = {"daily", "idex10day"}
        partition_filter.button = MagicMock()

        partition_filter._update_button_label()

        partition_filter.button.set_text.assert_called_once_with(
            "Partition types\nDaily, Agg-"
        )
        partition_filter.selected_types = set(partition_filter.available_types)
        partition_filter._update_button_label()
        self.assertEqual(
            partition_filter.button.set_text.call_args.args[0],
            "Partition types\nAll",
        )

        level_filter = object.__new__(SnapshotDataLevelFilter)
        level_filter.available_levels = {"l0", "l2", "l3a", "l3b", "ancillary"}
        level_filter.selected_levels = {"l0", "l2", "l3a"}
        level_filter.button = MagicMock()

        level_filter._update_button_label()

        level_filter.button.set_text.assert_called_once_with(
            "Data levels\nL0, L2, L3-"
        )
        level_filter.selected_levels = set(level_filter.available_levels)
        level_filter._update_button_label()
        self.assertEqual(
            level_filter.button.set_text.call_args.args[0], "Data levels\nAll"
        )

        instrument_filter = object.__new__(SnapshotInstrumentFilter)
        instrument_filter.available_levels = {"lo", "hi", "mag", "spacecraft"}
        instrument_filter.selected_levels = {"lo", "mag", "spacecraft"}
        instrument_filter.button = MagicMock()

        instrument_filter._update_button_label()

        instrument_filter.button.set_text.assert_called_once_with(
            "Instruments\nENA-, In-Situ, Other"
        )

    def test_snapshot_groups_each_instrument_into_the_same_date_rows(self) -> None:
        rows = [
            {
                "asset": "mag_l1d_first",
                "status": "failed",
                "missing_file": "",
                "partition": "x_2026-01-02T00:00:00Z_to_2026-01-03T00:00:00Z",
            },
            {
                "asset": "codice_l1a_first",
                "status": "materialized",
                "missing_file": "",
                "partition": "x_2026-01-02T00:00:00Z_to_2026-01-03T00:00:00Z",
            },
            {
                "asset": "mag_l1d_second",
                "status": "materialized",
                "missing_file": "",
                "partition": "x_2026-01-03T00:00:00Z_to_2026-01-04T00:00:00Z",
            },
        ]

        result = snapshot_status_rows(rows, ["codice", "mag"], "day")  # type: ignore[arg-type]

        self.assertEqual(
            [(row["first_date"], row["last_date"]) for row in result],
            [("2026-01-02", "2026-01-02"), ("2026-01-03", "2026-01-03")],
        )
        self.assertEqual(result[0]["counts"]["codice"]["materialized"], 1)
        self.assertEqual(result[0]["counts"]["mag"]["failed"], 1)
        self.assertEqual(result[1]["counts"]["codice"]["materialized"], 0)
        self.assertEqual(result[1]["counts"]["mag"]["materialized"], 1)

    def test_snapshot_includes_spacecraft_and_all_six_statuses(self) -> None:
        rows = [
            {
                "asset": "spacecraft_l1a_state",
                "status": "not-found",
                "missing_file": "",
                "partition": "",
            }
        ]

        result = snapshot_status_rows(rows, ["spacecraft"])  # type: ignore[arg-type]

        self.assertEqual(
            result[0]["counts"]["spacecraft"],
            {
                "materialized": 0,
                "materializing": 0,
                "failed": 0,
                "skipped": 0,
                "not-run": 0,
                "not-found": 1,
            },
        )

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


class SnapshotNavigationTests(IsolatedAsyncioTestCase):
    def test_records_per_page_updates_every_asset_table(self) -> None:
        view = object.__new__(AssetsStatusView)
        view.table = MagicMock()
        view.summary_table = MagicMock()
        view.snapshot_table = MagicMock()
        for wrapper in (view.table, view.summary_table, view.snapshot_table):
            wrapper.table.pagination = {"rowsPerPage": 25}

        view._set_records_per_page(100)

        for wrapper in (view.table, view.summary_table, view.snapshot_table):
            self.assertEqual(wrapper.table.pagination["rowsPerPage"], 100)

    async def test_instrument_header_selects_that_instrument(self) -> None:
        view = object.__new__(AssetsStatusView)
        view.instrument = "all"
        view.snapshot_table = MagicMock(groups=["mag"])
        view.snapshot_table.instrument_for_group.return_value = "mag"
        view.toolbar = MagicMock()
        view.set_instrument = AsyncMock()

        await view._on_snapshot_group_click("mag")

        self.assertEqual(view.toolbar.instrument_select.value, "mag")
        view.set_instrument.assert_awaited_once_with("mag")

    async def test_data_level_header_opens_filtered_summary(self) -> None:
        view = object.__new__(AssetsStatusView)
        view.instrument = "mag"
        view.snapshot_table = MagicMock(groups=["l1a"])
        view.toolbar = MagicMock()
        view.snapshot_filter_chip = MagicMock()
        view._update_summary = MagicMock()
        view._apply_view_visibility = MagicMock()
        view._schedule_settings_save = MagicMock()

        await view._on_snapshot_group_click("l1a")

        self.assertEqual(view.snapshot_summary_filter, ("mag", "l1a"))
        self.assertEqual(view.snapshot_return_instrument, "mag")
        self.assertEqual(view.view_mode, "summary")
        self.assertEqual(view.toolbar.view_select.value, "summary")
        view.snapshot_filter_chip.set_text.assert_called_once_with("mag / l1a")
        view._update_summary.assert_called_once_with()
