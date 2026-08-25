import csv
from io import StringIO
from types import SimpleNamespace
from unittest import TestCase

from sds_utils.dashboard.frontend.elems import AssetsStatusSummaryTable, AssetsStatusTable
from sds_utils.dashboard.frontend.exporting import csv_table, plain_text_table


class ExportFormattingTests(TestCase):
    def test_csv_preserves_commas_quotes_and_newlines(self) -> None:
        output = csv_table(
            ["Asset", "Reason"],
            [["imap_l1_data", 'Missing "first",\nthen second']],
        )
        self.assertEqual(
            list(csv.reader(StringIO(output))),
            [["Asset", "Reason"], ["imap_l1_data", 'Missing "first",\nthen second']],
        )

    def test_plain_text_is_bordered_and_wraps_long_values(self) -> None:
        output = plain_text_table(
            ["Status", "Reason"],
            [["skipped", "missing dependency information"]],
            maximum_column_width=12,
        )
        self.assertIn("| Status", output)
        self.assertIn("| skipped", output)
        self.assertIn("dependency", output)
        self.assertGreater(output.count("\n"), 4)

    def test_none_is_exported_as_empty(self) -> None:
        self.assertNotIn("None", plain_text_table(["Value"], [[None]]))


class TableExportSelectionTests(TestCase):
    def test_summary_export_omits_disabled_dimensions(self) -> None:
        table = AssetsStatusSummaryTable(
            on_settings_change=lambda: None,
            on_filter=lambda *_args: None,
            on_clear_filter=lambda *_args: None,
        )
        table.enabled_dimensions = {"instrument"}
        table.table = SimpleNamespace(
            rows=[
                {
                    "row_id": "mag",
                    "instrument": "mag",
                    "data_level": "",
                    "descriptor": "",
                    "missing_file": "",
                    "first_date": "2026-01-01",
                    "last_date": "2026-01-02",
                    "materialized": 2,
                    "materializing": 0,
                    "failed": 1,
                    "skipped": 0,
                    "not_run": 0,
                    "not_found": 0,
                }
            ]
        )

        headers, rows = table.export_data()

        self.assertEqual(headers[0], "Instrument")
        self.assertNotIn("Data level", headers)
        self.assertNotIn("Descriptor", headers)
        self.assertNotIn("Missing file", headers)
        self.assertEqual(rows[0][0], "mag")

    def test_main_export_hides_optional_columns_by_default(self) -> None:
        table = AssetsStatusTable(
            on_metadata_change=lambda *_args: None,
            on_settings_change=lambda: None,
        )
        labels = [column["label"] for column in table._displayed_columns()]
        self.assertNotIn("Tags", labels)
        self.assertNotIn("Notes", labels)

        table.visible_optional_columns = {"tags", "notes"}
        labels = [column["label"] for column in table._displayed_columns()]
        self.assertIn("Tags", labels)
        self.assertIn("Notes", labels)

    def test_main_csv_export_appends_partition_link(self) -> None:
        table = AssetsStatusTable(
            on_metadata_change=lambda *_args: None,
            on_settings_change=lambda: None,
        )
        partition_url = "https://dagster.example/assets/mag?view=partitions&partition=p1"
        table.all_rows = [
            {
                "instrument": "mag",
                "data_level": "l1d",
                "descriptor": "norm",
                "partition": "p1",
                "update_timestamp": "2026-08-11T00:00:00+00:00",
                "status": "materialized",
                "missing_file": "",
                "skip_reason": "",
                "missing_files": "",
                "tags": "",
                "notes": "",
                "partition_url": partition_url,
            }
        ]  # type: ignore[assignment]

        headers, rows = table.export_data(include_partition_link=True)

        self.assertEqual(headers[-1], "Partition link")
        self.assertEqual(rows[0][-1], partition_url)

    def test_main_plain_export_does_not_append_partition_link(self) -> None:
        table = AssetsStatusTable(
            on_metadata_change=lambda *_args: None,
            on_settings_change=lambda: None,
        )
        table.all_rows = []
        headers, _rows = table.export_data()
        self.assertNotIn("Partition link", headers)
