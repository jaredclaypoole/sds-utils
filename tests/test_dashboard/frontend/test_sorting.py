from unittest import TestCase

from sds_utils.dashboard.frontend.elems import SortRule, SortingDialog, _sorted_rows
from sds_utils.dashboard.frontend.models import AppSettingsState


class MultiColumnSortingTests(TestCase):
    def test_dialog_open_preserves_precedence_including_hidden_columns(self) -> None:
        dialog = SortingDialog(
            [("instrument", "Instrument"), ("tags", "Tags"), ("notes", "Notes")],
            lambda rules: None,
            lambda: None,
        ).build()

        dialog.open(
            [SortRule("notes", True), SortRule("tags"), SortRule("instrument")]
        )

        self.assertEqual(
            [(rule.column, rule.descending) for rule in dialog.rules],
            [("notes", True), ("tags", False), ("instrument", False)],
        )

    def test_dialog_can_reset_sorting_to_defaults(self) -> None:
        columns = [
            (rule.column, rule.column.replace("_", " ").title())
            for rule in AppSettingsState().sorting
        ]
        dialog = SortingDialog(columns, lambda rules: None, lambda: None).build()
        dialog.open(list(reversed([SortRule(column) for column, _ in columns])))

        dialog._reset_to_defaults()

        self.assertEqual(
            [(rule.column, rule.descending) for rule in dialog.rules],
            [
                (rule.column, rule.descending)
                for rule in AppSettingsState().sorting
            ],
        )

    def test_uses_precedence_and_independent_directions(self) -> None:
        rows = [
            {"instrument": "mag", "failed": 1},
            {"instrument": "imap", "failed": 2},
            {"instrument": "imap", "failed": 4},
        ]

        sorted_rows = _sorted_rows(
            rows,
            [SortRule("instrument"), SortRule("failed", descending=True)],
        )

        self.assertEqual(
            [(row["instrument"], row["failed"]) for row in sorted_rows],
            [("imap", 4), ("imap", 2), ("mag", 1)],
        )

    def test_empty_values_remain_last_when_descending(self) -> None:
        rows = [{"partition": "b"}, {"partition": ""}, {"partition": "a"}]

        sorted_rows = _sorted_rows(rows, [SortRule("partition", descending=True)])

        self.assertEqual([row["partition"] for row in sorted_rows], ["b", "a", ""])
