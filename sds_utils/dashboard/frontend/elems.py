"""Reusable NiceGUI elements for the Dagster query application."""

import asyncio
import logging
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import cmp_to_key
from time import perf_counter
from types import SimpleNamespace
from typing import Any, Self, cast

from nicegui import run, ui
from nicegui.elements.button import Button
from nicegui.elements.label import Label
from nicegui.elements.select import Select
from nicegui.elements.table import Table
from nicegui.events import GenericEventArguments
from pydantic import ValidationError

from ..backend.data import (
    AssetOption,
    AssetStatusRow,
    DagsterAssetsDataSource,
    parse_asset_name,
)
from ..backend.dependency_graph import (
    DEPENDENCY_GRAPH_INSTRUMENTS,
    DependencyGraph,
    dependency_graph_mermaid,
    load_dependency_graph,
)
from ..backend.partition_time import (
    TimestampFiltering,
    include_partition,
    partition_type,
)
from .asset_cache import CachedDagsterAssetsDataSource
from .exporting import csv_table, plain_text_table
from .filtering import (
    is_valid_regex,
    matches_regex,
    matches_search_expression,
    matches_tag_search_expression,
)
from .metadata_store import AttemptMetadataStore
from .models import (
    AppSettingsState,
    ColumnFilterSettings,
    ColumnSortSettings,
    DependencyGraphInstrument,
    EndMode,
    FilterMode,
    InstrumentName,
    OptionalColumn,
    SortColumn,
    StartMode,
    StatusName,
    SummaryDateAggregation,
    SummaryGroupDimension,
    SummarySortColumn,
    ViewMode,
)
from .settings_store import AppSettingsStore
from .summary import (
    MIN_AGGREGATION_DAYS,
    SUMMARY_DIMENSIONS,
    SUMMARY_STATUSES,
    SummaryDrilldown,
    SummaryRow,
    snapshot_status_rows,
    summarize_status_rows,
    summary_drilldown,
)

logger = logging.getLogger("uvicorn.error.sds_utils.dashboard.frontend.ui")

ALL_INSTRUMENTS = [
    "codice",
    "hi",
    "hit",
    "idex",
    "glows",
    "mag",
    "lo",
    "swapi",
    "swe",
    "ultra",
    "spacecraft",
]
ZERO_COUNTS_GRAY = True


def _left_aligned_columns(
    columns: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Return table definitions with consistently left-aligned cells."""
    return [{**column, "align": "left"} for column in columns]


def _format_status_timestamp(value: datetime) -> str:
    formatted = value.astimezone(UTC).isoformat(sep="_", timespec="seconds")
    return f"{formatted[:-6]} {formatted[-6:]}"


class UIElem(ABC):
    @abstractmethod
    def render(self) -> None:
        """Create the UI element."""

    def build(self) -> Self:
        self.render()
        return self


class PageHeader(UIElem):
    def render(self) -> None:
        with ui.column().classes("gap-1"):
            ui.label("Asset partition status").classes(
                "text-3xl font-semibold text-slate-900"
            )
            ui.label(
                "Inspect materialized, active, failed, skipped, not-run, and "
                "not-found partitions."
            ).classes("text-sm text-slate-500")


class StatusFilterCard(UIElem):
    ACTIVE_CLASSES = {
        "materialized": "border-green-400 bg-green-50 text-green-900",
        "materializing": "border-violet-400 bg-violet-50 text-violet-900",
        "failed": "border-red-400 bg-red-50 text-red-900",
        "skipped": "border-amber-400 bg-amber-50 text-amber-900",
        "not-run": "border-slate-400 bg-slate-100 text-slate-900",
        "not-found": "border-blue-400 bg-blue-50 text-blue-900",
    }
    BASE_CLASSES = (
        "min-w-36 px-4 py-3 shadow-none border-2 cursor-pointer "
        "select-none transition-all duration-150"
    )
    INACTIVE_CLASSES = "border-slate-200 bg-white text-slate-400 opacity-50"

    def __init__(
        self,
        status: str,
        on_toggle: Callable[[str], None],
        *,
        active: bool,
        item_label: str = "partitions",
    ) -> None:
        self.status = status
        self.on_toggle = on_toggle
        self.active = active
        self.item_label = item_label
        self.count_label: Label

    def render(self) -> None:
        self.card = ui.card().classes(replace=self._classes())
        self.card.props("role=button tabindex=0")
        self.card.tooltip(f"Toggle {self.status} {self.item_label}")
        self.card.on("click", self._toggle)
        with self.card:
            ui.label(self.status).classes("text-xs uppercase tracking-wide")
            self.count_label = ui.label("0 / 0").classes("text-2xl font-semibold")

    def set_count(self, shown: int, total: int) -> None:
        self.count_label.set_text(f"{shown:,} / {total:,}")

    def _toggle(self) -> None:
        self.active = not self.active
        self.card.classes(replace=self._classes())
        self.on_toggle(self.status)

    def _classes(self) -> str:
        state_classes = (
            self.ACTIVE_CLASSES[self.status] if self.active else self.INACTIVE_CLASSES
        )
        return f"{self.BASE_CLASSES} {state_classes}"


class StatusSummary(UIElem):
    STATUSES = (
        "materialized",
        "materializing",
        "failed",
        "skipped",
        "not-run",
        "not-found",
    )

    def __init__(
        self,
        on_change: Callable[[set[str]], None],
        visible_statuses: set[str],
    ) -> None:
        self.on_change = on_change
        self.visible_statuses = visible_statuses
        self.cards: dict[str, StatusFilterCard] = {}

    def render(self) -> None:
        with ui.row().classes("w-full gap-3 flex-wrap"):
            for status in self.STATUSES:
                self.cards[status] = StatusFilterCard(
                    status,
                    self._status_toggled,
                    active=status in self.visible_statuses,
                ).build()

    def update(
        self,
        rows: list[AssetStatusRow],
        shown_rows: list[AssetStatusRow],
    ) -> None:
        totals = {status: 0 for status in self.STATUSES}
        shown = {status: 0 for status in self.STATUSES}
        for row in rows:
            totals[row["status"]] += 1
        for row in shown_rows:
            shown[row["status"]] += 1
        for status, card in self.cards.items():
            card.set_count(shown[status], totals[status])

    def _status_toggled(self, _status: str) -> None:
        self.on_change({status for status, card in self.cards.items() if card.active})


FilterValue = str | tuple[str, ...]


def _sorted_export_rows(
    rows: list[Any],
    column: str,
    descending: bool,
) -> list[Any]:
    if not column:
        return list(rows)

    def key(row: Any) -> tuple[bool, object]:
        value = row.get(column)
        normalized = value.casefold() if isinstance(value, str) else value
        return value is None, normalized

    return sorted(rows, key=key, reverse=descending)


@dataclass(frozen=True, slots=True)
class SortRule:
    column: str
    descending: bool = False


def _sorted_rows(rows: list[Any], rules: list[SortRule]) -> list[Any]:
    """Sort mappings by multiple rules while consistently placing empty values last."""

    def compare(left: Any, right: Any) -> int:
        for rule in rules:
            left_value = left.get(rule.column)
            right_value = right.get(rule.column)
            left_empty = left_value is None or left_value == ""
            right_empty = right_value is None or right_value == ""
            if left_empty != right_empty:
                return 1 if left_empty else -1
            if left_empty:
                continue
            if isinstance(left_value, str) and isinstance(right_value, str):
                left_value, right_value = left_value.casefold(), right_value.casefold()
            result = (left_value > right_value) - (left_value < right_value)
            if result:
                return -result if rule.descending else result
        return 0

    return sorted(rows, key=cmp_to_key(compare))


class ColumnMenu(UIElem):
    """Reusable NiceGUI controls for sorting and filtering one table column."""

    def __init__(
        self,
        *,
        on_filter: Callable[[str, str, FilterValue], None],
        on_clear_filter: Callable[[str], None],
        on_toggle_grouping: Callable[[str], None] | None = None,
        status_options: tuple[str, ...] = (),
    ) -> None:
        self.on_filter = on_filter
        self.on_clear_filter = on_clear_filter
        self.on_toggle_grouping = on_toggle_grouping
        self.status_options = status_options
        self.column = ""

    def render(self) -> None:
        with ui.dialog() as self.dialog, ui.card().classes("w-96 max-w-full"):
            self.title = ui.label().classes("text-lg font-semibold")
            self.grouping_button = ui.button(
                "Disable grouping", on_click=self._toggle_grouping
            ).props("flat no-caps color=grey")

            ui.separator()
            self.mode_select = ui.select(
                options={
                    "pattern": "Pattern",
                    "regex": "Regular expression",
                    "one_of": "One of",
                    "empty": "Is empty",
                    "not_empty": "Is not empty",
                },
                value="pattern",
                label="Filter mode",
                on_change=self._mode_changed,
            ).classes("w-full")
            self.text_input = (
                ui.input(
                    label="Pattern",
                    placeholder="^prefix* -excluded suffix$",
                )
                .on("keydown.enter", self._apply_filter)
                .props("outlined clearable")
                .classes("w-full")
            )
            self.pattern_help = ui.label(
                "Use * for anything, ** for anything except _, ^/$ to anchor, "
                "- to exclude, and spaces for AND."
            ).classes("text-xs text-slate-500")
            self.status_select = (
                ui.select(
                    options=list(self.status_options),
                    label="Statuses",
                    multiple=True,
                )
                .props("outlined use-chips")
                .classes("w-full")
            )

            with ui.row().classes("w-full justify-end gap-2"):
                self.clear_filter_button = ui.button(
                    "Clear filter", on_click=self._clear_filter
                ).props("flat no-caps color=grey")
                ui.button("Cancel", on_click=self.dialog.close).props("flat no-caps")
                self.apply_filter_button = ui.button(
                    "Apply", on_click=self._apply_filter
                ).props("unelevated no-caps")

    def open(
        self,
        *,
        column: str,
        label: str,
        current_filter: tuple[str, FilterValue] | None,
        grouping_enabled: bool | None = None,
        filter_enabled: bool = True,
    ) -> None:
        self.column = column
        self.title.set_text(label)
        self.grouping_button.set_visibility(grouping_enabled is not None)
        if grouping_enabled is not None:
            self.grouping_button.set_text(
                "Disable grouping" if grouping_enabled else "Enable grouping"
            )
        mode, value = current_filter or ("pattern", "")
        self.mode_select.options = {
            "pattern": "Pattern",
            "regex": "Regular expression",
            **(
                {"one_of": "One of"}
                if column == "status" and self.status_options
                else {}
            ),
            "empty": "Is empty",
            "not_empty": "Is not empty",
        }
        self.mode_select.set_visibility(filter_enabled)
        self.clear_filter_button.set_visibility(filter_enabled)
        self.apply_filter_button.set_visibility(filter_enabled)
        self.mode_select.update()
        self.mode_select.set_value(mode)
        if column == "status":
            selected = (
                list(value) if isinstance(value, tuple) else ([value] if value else [])
            )
            self.status_select.set_value(selected)
        else:
            self.text_input.set_value(value if isinstance(value, str) else "")
        if filter_enabled:
            self._update_filter_control_visibility()
        else:
            self.text_input.set_visibility(False)
            self.pattern_help.set_visibility(False)
            self.status_select.set_visibility(False)
        self.dialog.open()

    def _toggle_grouping(self) -> None:
        if self.on_toggle_grouping is not None:
            self.on_toggle_grouping(self.column)
        self.dialog.close()

    def _mode_changed(self) -> None:
        self._update_filter_control_visibility()

    def _update_filter_control_visibility(self) -> None:
        text_mode = self.mode_select.value in {"pattern", "regex"}
        self.text_input.set_visibility(text_mode)
        self.pattern_help.set_visibility(self.mode_select.value == "pattern")
        self.status_select.set_visibility(self.mode_select.value == "one_of")

    def _apply_filter(self) -> None:
        mode = str(self.mode_select.value)
        if mode == "one_of":
            value: FilterValue = tuple(self.status_select.value or [])
        elif mode == "empty":
            value = "(empty)"
        elif mode == "not_empty":
            value = "(not empty)"
        else:
            value = str(self.text_input.value or "")
        if mode == "regex" and not is_valid_regex(str(value)):
            ui.notify("Invalid regular expression", type="negative")
            return
        if mode not in {"pattern", "regex", "one_of"} or value:
            self.on_filter(self.column, mode, value)
        else:
            self.on_clear_filter(self.column)
        self.dialog.close()

    def _clear_filter(self) -> None:
        self.on_clear_filter(self.column)
        self.dialog.close()


class FilterableSortableTable(UIElem):
    """Shared column filtering and multi-column sorting for NiceGUI tables."""

    COLUMNS: list[dict[str, object]] = []
    TAG_FILTER_COLUMNS: set[str] = set()
    STATUS_FILTER_OPTIONS: tuple[str, ...] = ()

    def __init__(
        self,
        *,
        on_settings_change: Callable[[], None],
        columns: list[dict[str, object]],
    ) -> None:
        self.on_settings_change = on_settings_change
        self.column_filters: dict[str, tuple[str, FilterValue]] = {}
        self.sorting_rules: list[SortRule] = []
        self.columns = _left_aligned_columns(
            [
                {**column, "sortable": False, "filter_value": "", "sort_direction": ""}
                for column in columns
            ]
        )
        self.table: Table

    def _attach_column_controls(self) -> None:
        self.table.on("header-click", self._on_header_click)
        self.column_menu = ColumnMenu(
            on_filter=self.set_column_filter,
            on_clear_filter=self.clear_column_filter,
            status_options=self.STATUS_FILTER_OPTIONS,
        ).build()

    def set_sorting(self, rules: list[SortRule]) -> None:
        available = {str(column["name"]) for column in self.COLUMNS}
        self.sorting_rules = [rule for rule in rules if rule.column in available]
        self._refresh_column_metadata()
        self._apply_filters()

    def _matches_column_filters(self, row: Mapping[str, object]) -> bool:
        for column, (mode, expected) in self.column_filters.items():
            actual = str(row.get(column, ""))
            if mode == "pattern" and isinstance(expected, str):
                matcher = (
                    matches_tag_search_expression
                    if column in self.TAG_FILTER_COLUMNS
                    else matches_search_expression
                )
                if not matcher(actual, expected):
                    return False
            if mode == "regex" and isinstance(expected, str):
                if not matches_regex(actual, expected):
                    return False
            if mode == "one_of" and actual not in expected:
                return False
            if mode == "empty" and actual:
                return False
            if mode == "not_empty" and not actual:
                return False
        return True

    def _on_header_click(self, event: GenericEventArguments) -> None:
        column = str(event.args["column"])
        definition = next(item for item in self.columns if item["name"] == column)
        self.column_menu.open(
            column=column,
            label=str(definition["label"]),
            current_filter=self.column_filters.get(column),
        )

    def set_column_filter(self, column: str, mode: str, value: FilterValue) -> None:
        self.column_filters[column] = (mode, value)
        self._refresh_column_metadata()
        self._apply_filters()
        self.on_settings_change()

    def clear_column_filter(self, column: str) -> None:
        self.column_filters.pop(column, None)
        self._refresh_column_metadata()
        self._apply_filters()
        self.on_settings_change()

    def _refresh_column_metadata(self) -> None:
        for column in self.columns:
            name = str(column["name"])
            current_filter = self.column_filters.get(name)
            if current_filter is None:
                filter_value = ""
            elif isinstance(current_filter[1], tuple):
                filter_value = ", ".join(current_filter[1])
            else:
                filter_value = current_filter[1]
            column["filter_value"] = filter_value
            rule = next(
                (rule for rule in self.sorting_rules if rule.column == name), None
            )
            column["sort_direction"] = (
                "desc" if rule and rule.descending else ("asc" if rule else "")
            )
        self.table.columns = self._displayed_columns()

    def _displayed_columns(self) -> list[dict[str, object]]:
        return self.columns

    @abstractmethod
    def _apply_filters(self) -> None:
        """Apply table-specific scope filters plus shared column/sort state."""


class AssetsStatusTable(FilterableSortableTable):
    OPTIONAL_COLUMNS = ("tags", "notes")
    TAG_FILTER_COLUMNS = {"tags"}
    STATUS_FILTER_OPTIONS = (
        "materialized",
        "materializing",
        "failed",
        "skipped",
        "not-run",
        "not-found",
    )
    COLUMNS = [
        {
            "name": "instrument",
            "label": "Instrument",
            "field": "instrument",
            "sortable": True,
        },
        {
            "name": "data_level",
            "label": "Data level",
            "field": "data_level",
            "sortable": True,
        },
        {
            "name": "descriptor",
            "label": "Descriptor",
            "field": "descriptor",
            "sortable": True,
        },
        {
            "name": "partition",
            "label": "Partition",
            "field": "partition",
            "sortable": True,
        },
        {
            "name": "update_timestamp",
            "label": "Updated (UTC)",
            "field": "update_timestamp",
            "sortable": True,
        },
        {"name": "status", "label": "Status", "field": "status", "sortable": True},
        {
            "name": "missing_file",
            "label": "Missing file",
            "field": "missing_file",
            "sortable": True,
        },
        {
            "name": "skip_reason",
            "label": "Skip reason",
            "field": "skip_reason",
            "sortable": True,
        },
        {
            "name": "missing_files",
            "label": "Missing files",
            "field": "missing_files",
        },
        {"name": "tags", "label": "Tags", "field": "tags", "sortable": True},
        {"name": "notes", "label": "Notes", "field": "notes", "sortable": True},
    ]

    def __init__(
        self,
        *,
        on_metadata_change: Callable[..., object],
        on_settings_change: Callable[[], None],
    ) -> None:
        super().__init__(
            on_settings_change=on_settings_change,
            columns=self.COLUMNS,
        )
        self.on_metadata_change = on_metadata_change
        self.table: Table
        self.all_rows: list[AssetStatusRow] = []
        self.show_unpartitioned_assets = False
        self.visible_statuses = set(StatusSummary.STATUSES) - {"not-run"}
        self.timestamp_filtering = TimestampFiltering.ACTIVE_ONLY
        self.window_start: datetime | None = None
        self.window_end: datetime | None = None
        self.sort_column = "instrument"
        self.sort_descending = False
        self.visible_optional_columns: set[str] = set()
        self.summary_drilldown: SummaryDrilldown | None = None

    def render(self) -> None:
        ui.add_css(
            ".metadata-input input::placeholder, "
            ".metadata-input textarea::placeholder { color: #cbd5e1; opacity: 1; }"
        )
        self.table = ui.table(
            columns=self._displayed_columns(),
            rows=[],
            row_key="row_id",
            pagination={"rowsPerPage": 25},
        ).classes("w-full shadow-none border rounded-lg")
        self.table.props("flat bordered separator=horizontal wrap-cells")
        self.table.add_slot(
            "header-cell",
            """
            <q-th
              :props="props"
              class="cursor-pointer"
              @click="$parent.$emit('header-click', {column: props.col.name})"
            >
              <div class="row items-center no-wrap q-gutter-xs">
                <span>{{ props.col.label }}</span>
                <q-icon
                  v-if="props.col.filter_value"
                  name="filter_alt"
                  color="primary"
                  size="xs"
                />
                <q-icon
                  v-if="props.col.sort_direction"
                  :name="props.col.sort_direction === 'asc' ? 'arrow_upward' : 'arrow_downward'"
                  color="primary"
                  size="xs"
                />
                <q-icon name="expand_more" size="xs" />
              </div>
            </q-th>
            """,
        )
        self._attach_column_controls()
        self.table.on("metadata-change", self.on_metadata_change)
        self.table.add_slot(
            "body-cell-partition",
            """
            <q-td :props="props">
              <a
                v-if="props.value"
                :href="props.row.partition_url"
                target="_blank"
                rel="noopener noreferrer"
                class="text-blue-700 hover:text-blue-900 hover:underline"
              >{{ props.value }}</a>
            </q-td>
            """,
        )
        self.table.add_slot(
            "body-cell-status",
            """
            <q-td :props="props">
              <a
                :href="props.row.status_url"
                target="_blank"
                rel="noopener noreferrer"
              >
                <q-badge
                  :color="({
                    'materialized': 'positive',
                    'materializing': 'info',
                    'failed': 'negative',
                    'skipped': 'warning',
                    'not-run': 'grey-7',
                    'not-found': 'deep-purple',
                  })[props.value]"
                  :label="props.value"
                  outline
                />
              </a>
            </q-td>
            """,
        )
        self.table.add_slot(
            "body-cell-tags",
            """
            <q-td :props="props">
              <q-input
                :model-value="props.row.tags"
                :disable="!props.row.attempt_id"
                class="metadata-input"
                dense borderless
                :placeholder="props.row.attempt_id ? 'Add a tag' : ''"
                @change="$parent.$emit('metadata-change', {
                  row_id: props.row.row_id,
                  attempt_id: props.row.attempt_id,
                  field: 'tags',
                  value: $event,
                })"
              />
            </q-td>
            """,
        )
        self.table.add_slot(
            "body-cell-notes",
            """
            <q-td :props="props">
              <q-input
                :model-value="props.row.notes"
                :disable="!props.row.attempt_id"
                class="metadata-input"
                dense borderless autogrow
                :placeholder="props.row.attempt_id ? 'Add a note' : ''"
                @change="$parent.$emit('metadata-change', {
                  row_id: props.row.row_id,
                  attempt_id: props.row.attempt_id,
                  field: 'notes',
                  value: $event,
                })"
              />
            </q-td>
            """,
        )

    def set_rows(self, rows: list[AssetStatusRow]) -> None:
        self.all_rows = rows
        self._apply_filters()

    def update_metadata(self, row_id: str, field: str, value: str) -> None:
        for row in self.all_rows:
            if row["row_id"] == row_id:
                if field == "tags":
                    row["tags"] = value
                elif field == "notes":
                    row["notes"] = value
                break
        self._apply_filters()
        self.on_settings_change()

    def refresh(self) -> None:
        """Restore rendered values from the current row models."""
        self._apply_filters()

    def set_visible_statuses(self, statuses: set[str]) -> None:
        self.visible_statuses = statuses
        self._apply_filters()

    def set_show_unpartitioned_assets(self, show: bool) -> None:
        self.show_unpartitioned_assets = show
        self._apply_filters()

    def set_timestamp_filtering(self, mode: TimestampFiltering) -> None:
        self.timestamp_filtering = mode
        self._apply_filters()

    def set_activity_window(self, start: datetime, end: datetime) -> None:
        self.window_start = start
        self.window_end = end
        self._apply_filters()

    def set_visible_optional_columns(self, columns: set[str]) -> None:
        self.visible_optional_columns = columns & set(self.OPTIONAL_COLUMNS)
        self._refresh_column_metadata()
        self.table.update()

    def rows_in_timestamp_scope(self) -> list[AssetStatusRow]:
        return [
            row
            for row in self.all_rows
            if self._matches_unpartitioned_filter(row)
            and self._matches_timestamp_filter(row)
        ]

    def rows_allowed_by_column_filters(self) -> list[AssetStatusRow]:
        """Return scoped rows which would show if their status badge were active."""
        return [
            row
            for row in self.rows_in_timestamp_scope()
            if self._matches_column_filters(row)
            and self._matches_summary_drilldown(row)
        ]

    def visible_rows(self) -> list[AssetStatusRow]:
        """Return rows after all detail-view filters, including status badges."""
        return [
            row
            for row in self.rows_allowed_by_column_filters()
            if row["status"] in self.visible_statuses
        ]

    def set_summary_drilldown(self, drilldown: SummaryDrilldown | None) -> None:
        self.summary_drilldown = drilldown
        self._apply_filters()

    def export_data(
        self,
        *,
        include_partition_link: bool = False,
    ) -> tuple[list[str], list[list[object]]]:
        columns = self._displayed_columns()
        rows = _sorted_rows(self.visible_rows(), self.sorting_rules)
        headers = [str(column["label"]) for column in columns]
        values = [[row[str(column["field"])] for column in columns] for row in rows]
        if include_partition_link:
            headers.append("Partition link")
            for values_row, source_row in zip(values, rows, strict=True):
                values_row.append(source_row["partition_url"])
        return headers, values

    def _apply_filters(self) -> None:
        self.table.rows = _sorted_rows(
            [
                row
                for row in self.all_rows
                if self._matches_unpartitioned_filter(row)
                and self._matches_timestamp_filter(row)
                and row["status"] in self.visible_statuses
                and self._matches_column_filters(row)
                and self._matches_summary_drilldown(row)
            ],
            self.sorting_rules,
        )
        self.table.update()

    def _matches_summary_drilldown(self, row: AssetStatusRow) -> bool:
        return self.summary_drilldown is None or self.summary_drilldown.matches(row)

    def set_sorting(self, rules: list[SortRule]) -> None:
        available = {str(column["name"]) for column in self.COLUMNS}
        self.sorting_rules = [rule for rule in rules if rule.column in available]
        self._refresh_column_metadata()
        self._apply_filters()

    def _matches_unpartitioned_filter(self, row: AssetStatusRow) -> bool:
        return self.show_unpartitioned_assets or bool(row["partition"])

    def _matches_timestamp_filter(self, row: AssetStatusRow) -> bool:
        if self.window_start is None or self.window_end is None:
            return True
        return include_partition(
            row["partition"],
            row["updated_in_window"],
            mode=self.timestamp_filtering,
            window_start=self.window_start,
            window_end=self.window_end,
        )

    def _on_header_click(self, event: GenericEventArguments) -> None:
        super()._on_header_click(event)

    def set_column_filter(self, column: str, mode: str, value: FilterValue) -> None:
        super().set_column_filter(column, mode, value)

    def clear_column_filter(self, column: str) -> None:
        super().clear_column_filter(column)

    def _set_sort(self, column: str, descending: bool) -> None:
        self.sort_column = column
        self.sort_descending = descending
        pagination = dict(self.table.pagination)
        pagination["sortBy"] = column or None
        pagination["descending"] = descending
        self.table.pagination = pagination
        self._refresh_column_metadata()
        self.table.update()
        self.on_settings_change()

    def restore_settings(self, settings: AppSettingsState) -> None:
        self.visible_statuses = set(settings.visible_statuses)
        self.show_unpartitioned_assets = settings.show_unpartitioned_assets
        self.timestamp_filtering = TimestampFiltering(settings.timestamp_filtering)
        self.column_filters = {
            column: (
                item.mode,
                tuple(item.value) if isinstance(item.value, list) else item.value,
            )
            for column, item in settings.column_filters.items()
            if any(definition["name"] == column for definition in self.COLUMNS)
        }
        available_columns = {str(column["name"]) for column in self.COLUMNS}
        self.sort_column = (
            settings.sort_column
            if settings.sort_column in available_columns
            else "instrument"
        )
        self.sort_descending = settings.sort_descending
        self.visible_optional_columns = set(settings.visible_optional_columns)
        pagination = dict(self.table.pagination)
        pagination["sortBy"] = None
        pagination["descending"] = False
        self.table.pagination = pagination
        self._refresh_column_metadata()
        self._apply_filters()

    def _refresh_column_metadata(self) -> None:
        super()._refresh_column_metadata()

    def _displayed_columns(self) -> list[dict[str, object]]:
        return [
            column
            for column in self.columns
            if column["name"] not in self.OPTIONAL_COLUMNS
            or column["name"] in self.visible_optional_columns
        ]


class AssetsStatusSummaryTable(UIElem):
    """Grouped status counts derived from the filtered detail rows."""

    DATE_COLUMNS = {"first_date", "last_date"}
    LINK_COLUMN = {"name": "drilldown", "label": "", "field": "row_id"}
    COLUMNS = [
        {"name": "instrument", "label": "Instrument", "field": "instrument"},
        {"name": "data_level", "label": "Data level", "field": "data_level"},
        {"name": "descriptor", "label": "Descriptor", "field": "descriptor"},
        {"name": "missing_file", "label": "Missing file", "field": "missing_file"},
        {"name": "materialized", "label": "Materialized", "field": "materialized"},
        {"name": "materializing", "label": "Materializing", "field": "materializing"},
        {"name": "failed", "label": "Failed", "field": "failed"},
        {"name": "skipped", "label": "Skipped", "field": "skipped"},
        {"name": "not_run", "label": "Not run", "field": "not_run"},
        {"name": "not_found", "label": "Not found", "field": "not_found"},
        {"name": "first_date", "label": "First date", "field": "first_date"},
        {"name": "last_date", "label": "Last date", "field": "last_date"},
    ]

    def __init__(
        self,
        *,
        on_settings_change: Callable[[], None],
        on_filter: Callable[[str, str, FilterValue], None],
        on_clear_filter: Callable[[str], None],
        on_drilldown: Callable[[SummaryRow], None] | None = None,
    ) -> None:
        self.on_settings_change = on_settings_change
        self.on_filter = on_filter
        self.on_clear_filter = on_clear_filter
        self.on_drilldown = on_drilldown or (lambda _row: None)
        self.source_rows: list[AssetStatusRow] = []
        self.enabled_dimensions = set(SUMMARY_DIMENSIONS)
        self.shared_filters: dict[str, tuple[str, FilterValue]] = {}
        self.date_filters: dict[str, tuple[str, FilterValue]] = {}
        self.sort_column = "instrument"
        self.sort_descending = False
        self.sorting_rules: list[SortRule] = []
        self.date_aggregation: SummaryDateAggregation = "all"
        self.aggregation_days = MIN_AGGREGATION_DAYS
        self.columns = _left_aligned_columns(
            [
                {**self.LINK_COLUMN, "sortable": False},
                *[
                    {
                        **column,
                        "sortable": False,
                        "filter_value": "",
                        "sort_direction": "",
                    }
                    for column in self.COLUMNS
                ],
            ]
        )

    def render(self) -> None:
        self.table = ui.table(
            columns=self.columns,
            rows=[],
            row_key="row_id",
            pagination={"rowsPerPage": 25},
        ).classes("w-full shadow-none border rounded-lg")
        self.table.props("flat bordered separator=horizontal")
        self.table.add_slot(
            "body-cell-drilldown",
            """
            <q-td :props="props" class="q-px-xs">
              <q-btn
                icon="link"
                flat round dense
                color="primary"
                aria-label="Show corresponding detail rows"
                @click="$parent.$emit('summary-drilldown', props.row)"
              >
                <q-tooltip>Show corresponding detail rows</q-tooltip>
              </q-btn>
            </q-td>
            """,
        )
        self.table.add_slot(
            "header-cell",
            """
            <q-th
              :props="props"
              :class="props.col.name === 'drilldown' ? '' : 'cursor-pointer'"
              @click="$parent.$emit('header-click', {column: props.col.name})"
            >
              <div class="row items-center no-wrap q-gutter-xs">
                <span>{{ props.col.label }}</span>
                <q-icon v-if="props.col.filter_value" name="filter_alt" color="primary" size="xs" />
                <q-icon
                  v-if="props.col.grouping_disabled"
                  name="layers_clear"
                  color="grey"
                  size="xs"
                />
                <q-icon
                  v-if="props.col.sort_direction"
                  :name="props.col.sort_direction === 'asc' ? 'arrow_upward' : 'arrow_downward'"
                  color="primary"
                  size="xs"
                />
                <q-icon v-if="props.col.name !== 'drilldown'" name="expand_more" size="xs" />
              </div>
            </q-th>
            """,
        )
        for column, text_color in (
            ("materialized", "text-green-700"),
            ("materializing", "text-violet-700"),
            ("failed", "text-red-600"),
            ("skipped", "text-yellow-600"),
            ("not_run", "text-black"),
            ("not_found", "text-blue-700"),
        ):
            zero_text_color = "text-grey-3" if ZERO_COUNTS_GRAY else text_color
            self.table.add_slot(
                f"body-cell-{column}",
                f"""
                <q-td
                  :props="props"
                  :class="props.value === 0 ? '{zero_text_color}' : '{text_color}'"
                >
                  {{{{ props.value }}}}
                </q-td>
                """,
            )
        self.table.on("header-click", self._on_header_click)
        self.table.on("summary-drilldown", self._on_drilldown)
        self.column_menu = ColumnMenu(
            on_filter=self.on_filter,
            on_clear_filter=self.on_clear_filter,
            on_toggle_grouping=self._toggle_grouping,
        ).build()
        self.date_column_menu = ColumnMenu(
            on_filter=self._set_date_filter,
            on_clear_filter=self._clear_date_filter,
        ).build()

    def set_source_rows(self, rows: list[AssetStatusRow]) -> None:
        self.source_rows = rows
        self._apply()

    def restore_settings(self, settings: AppSettingsState) -> None:
        self.enabled_dimensions = set(settings.summary_group_dimensions)
        self.date_aggregation = settings.summary_date_aggregation
        self.aggregation_days = settings.summary_aggregation_days
        self.date_filters = {
            column: (
                item.mode,
                tuple(item.value) if isinstance(item.value, list) else item.value,
            )
            for column, item in settings.summary_column_filters.items()
            if column in self.DATE_COLUMNS
        }
        self.sort_column = settings.summary_sort_column
        self.sort_descending = settings.summary_sort_descending
        pagination = dict(self.table.pagination)
        pagination["sortBy"] = None
        pagination["descending"] = False
        self.table.pagination = pagination
        self._refresh_column_metadata()
        self._apply()

    def _apply(self) -> None:
        rows = summarize_status_rows(
            self.source_rows,
            self.enabled_dimensions,
            self.date_aggregation,
            self.aggregation_days,
        )
        self.table.rows = _sorted_rows(
            [row for row in rows if self._matches_date_filters(row)],
            self.sorting_rules,
        )
        self.table.update()

    def set_sorting(self, rules: list[SortRule]) -> None:
        available = {str(column["name"]) for column in self.COLUMNS}
        self.sorting_rules = [rule for rule in rules if rule.column in available]
        self._refresh_column_metadata()
        self._apply()

    def set_shared_filters(self, filters: dict[str, tuple[str, FilterValue]]) -> None:
        self.shared_filters = filters
        self._refresh_column_metadata()
        self.table.update()

    def export_data(self) -> tuple[list[str], list[list[object]]]:
        columns = [
            column
            for column in self.columns
            if column["name"] != "drilldown"
            and (
                column["name"] not in SUMMARY_DIMENSIONS
                or column["name"] in self.enabled_dimensions
            )
        ]
        rows = _sorted_rows(list(self.table.rows), self.sorting_rules)
        return (
            [str(column["label"]) for column in columns],
            [[row[str(column["field"])] for column in columns] for row in rows],
        )

    def _on_header_click(self, event: GenericEventArguments) -> None:
        column = str(event.args["column"])
        if column == "drilldown":
            return
        definition = next(item for item in self.columns if item["name"] == column)
        menu = (
            self.date_column_menu if column in self.DATE_COLUMNS else self.column_menu
        )
        menu.open(
            column=column,
            label=str(definition["label"]),
            current_filter=(
                self.date_filters.get(column)
                if column in self.DATE_COLUMNS
                else self.shared_filters.get(column)
            ),
            grouping_enabled=(
                column in self.enabled_dimensions
                if column in SUMMARY_DIMENSIONS
                else None
            ),
            filter_enabled=(
                column in SUMMARY_DIMENSIONS or column in self.DATE_COLUMNS
            ),
        )

    def _on_drilldown(self, event: GenericEventArguments) -> None:
        self.on_drilldown(cast(SummaryRow, event.args))

    def _set_date_filter(self, column: str, mode: str, value: FilterValue) -> None:
        self.date_filters[column] = (mode, value)
        self._refresh_column_metadata()
        self._apply()
        self.on_settings_change()

    def _clear_date_filter(self, column: str) -> None:
        self.date_filters.pop(column, None)
        self._refresh_column_metadata()
        self._apply()
        self.on_settings_change()

    def _matches_date_filters(self, row: Mapping[str, object]) -> bool:
        for column, (mode, expected) in self.date_filters.items():
            actual = str(row[column])
            if mode == "pattern" and isinstance(expected, str):
                if not matches_search_expression(actual, expected):
                    return False
            if mode == "regex" and isinstance(expected, str):
                if not matches_regex(actual, expected):
                    return False
            if mode == "one_of" and actual not in expected:
                return False
            if mode == "empty" and actual:
                return False
            if mode == "not_empty" and not actual:
                return False
        return True

    def _toggle_grouping(self, column: str) -> None:
        if column in self.enabled_dimensions:
            self.enabled_dimensions.remove(column)
        else:
            self.enabled_dimensions.add(column)
        self._refresh_column_metadata()
        self._apply()
        self.on_settings_change()

    def set_date_aggregation(self, value: SummaryDateAggregation) -> None:
        self.date_aggregation = value
        self._apply()
        self.on_settings_change()

    def set_aggregation_days(self, value: int) -> None:
        if value < MIN_AGGREGATION_DAYS:
            return
        self.aggregation_days = value
        if self.date_aggregation == "days":
            self._apply()
            self.on_settings_change()

    def _set_sort(self, column: str, descending: bool) -> None:
        self.sort_column = column
        self.sort_descending = descending
        pagination = dict(self.table.pagination)
        pagination["sortBy"] = column or None
        pagination["descending"] = descending
        self.table.pagination = pagination
        self._refresh_column_metadata()
        self.table.update()
        self.on_settings_change()

    def _refresh_column_metadata(self) -> None:
        for column in self.columns:
            name = str(column["name"])
            current_filter = (
                self.date_filters.get(name)
                if name in self.DATE_COLUMNS
                else self.shared_filters.get(name)
            )
            column["filter_value"] = "" if current_filter is None else current_filter[1]
            column["grouping_disabled"] = (
                name in SUMMARY_DIMENSIONS and name not in self.enabled_dimensions
            )
            rule = next(
                (rule for rule in self.sorting_rules if rule.column == name), None
            )
            column["sort_direction"] = (
                "desc" if rule and rule.descending else ("asc" if rule else "")
            )
        self.table.columns = self.columns


class AssetsStatusSnapshotTable(UIElem):
    """Date-bucketed status counts with one compact column per selected group."""

    STATUS_COLORS = {
        "materialized": "text-green-700",
        "materializing": "text-violet-700",
        "failed": "text-red-600",
        "skipped": "text-yellow-600",
        "not-run": "text-black",
        "not-found": "text-blue-700",
    }

    def __init__(
        self,
        instrument: str = "all",
        *,
        on_group_click: Callable[[str], object] | None = None,
    ) -> None:
        self.source_rows: list[AssetStatusRow] = []
        self.instrument = instrument
        self.on_group_click = on_group_click or (lambda _group: None)
        self.groups: list[str] = list(ALL_INSTRUMENTS) if instrument == "all" else []
        self._installed_slots: set[str] = set()
        self.sorting_rules: list[SortRule] = []
        self.selected_partition_types: set[str] | None = None
        self.selected_data_levels: set[str] | None = None
        self.date_aggregation: SummaryDateAggregation = "all"
        self.aggregation_days = MIN_AGGREGATION_DAYS
        self.columns = self._columns()

    def _columns(self) -> list[dict[str, object]]:
        columns: list[dict[str, object]] = [
            {"name": "first_date", "label": "First date", "field": "first_date"},
            {"name": "last_date", "label": "Last date", "field": "last_date"},
        ]
        columns.extend(
            {"name": group, "label": group, "field": group} for group in self.groups
        )
        return _left_aligned_columns(columns)

    def render(self) -> None:
        self.table = ui.table(
            columns=self.columns,
            rows=[],
            row_key="row_id",
            pagination={"rowsPerPage": 25},
        ).classes("w-full shadow-none border rounded-lg")
        self.table.props("flat bordered dense separator=cell")
        self.table.add_slot(
            "header-cell",
            """
            <q-th
              :props="props"
              :class="!['first_date', 'last_date'].includes(props.col.name) ? 'cursor-pointer text-primary' : ''"
              @click="!['first_date', 'last_date'].includes(props.col.name) && $parent.$emit('snapshot-group-click', props.col.name)"
            >
              <div class="row items-center no-wrap q-gutter-xs">
                <span>{{ props.col.label }}</span>
                <q-icon
                  v-if="props.col.sort_direction"
                  :name="props.col.sort_direction === 'asc' ? 'arrow_upward' : 'arrow_downward'"
                  color="primary"
                  size="xs"
                />
              </div>
            </q-th>
            """,
        )
        self.table.on("snapshot-group-click", self._on_group_click)
        self._install_group_slots()

    def _on_group_click(self, event: GenericEventArguments) -> object:
        return self.on_group_click(str(event.args))

    def _install_group_slots(self) -> None:
        for group in self.groups:
            if group in self._installed_slots:
                continue
            self.table.add_slot(
                f"body-cell-{group}",
                """
                <q-td :props="props" class="font-mono whitespace-pre">
                  <template v-for="(part, index) in props.value" :key="index">
                    <span :class="part.color">{{ part.text }}</span><span
                      v-if="index < props.value.length - 1" class="text-grey-3"
                    >/</span>
                  </template>
                  <q-tooltip>materialized / materializing / failed / skipped / not run / not found</q-tooltip>
                </q-td>
                """,
            )
            self._installed_slots.add(group)

    def restore_settings(self, settings: AppSettingsState) -> None:
        self.date_aggregation = settings.summary_date_aggregation
        self.aggregation_days = settings.summary_aggregation_days
        self._apply()

    def set_source_rows(self, rows: list[AssetStatusRow]) -> None:
        self.source_rows = rows
        self._apply()

    def set_partition_types(self, values: set[str]) -> None:
        self.selected_partition_types = set(values)
        self._apply()

    def set_data_levels(self, values: set[str]) -> None:
        self.selected_data_levels = set(values)
        self._apply()

    def set_instrument(self, instrument: str) -> None:
        self.instrument = instrument
        self._apply()

    def set_sorting(self, rules: list[SortRule]) -> None:
        self.sorting_rules = [
            rule for rule in rules if rule.column in {"first_date", "last_date"}
        ]
        self._refresh_column_metadata()
        self._apply()

    def _refresh_column_metadata(self) -> None:
        for column in self.columns:
            rule = next(
                (rule for rule in self.sorting_rules if rule.column == column["name"]),
                None,
            )
            column["sort_direction"] = (
                "desc" if rule and rule.descending else ("asc" if rule else "")
            )
        self.table.columns = self.columns

    def set_date_aggregation(self, value: SummaryDateAggregation) -> None:
        self.date_aggregation = value
        self._apply()

    def set_aggregation_days(self, value: int) -> None:
        if value < MIN_AGGREGATION_DAYS:
            return
        self.aggregation_days = value
        if self.date_aggregation == "days":
            self._apply()

    def _apply(self) -> None:
        source_rows = (
            self.source_rows
            if self.selected_partition_types is None
            else [
                row
                for row in self.source_rows
                if partition_type(row["partition"]) in self.selected_partition_types
            ]
        )
        if self.selected_data_levels is not None:
            source_rows = [
                row
                for row in source_rows
                if row["data_level"] in self.selected_data_levels
            ]
        if self.instrument == "all":
            groups = list(ALL_INSTRUMENTS)
            group_by = "instrument"
        else:
            groups = sorted(
                {
                    data_level
                    for row in source_rows
                    if (parsed := parse_asset_name(row["asset"]))[0] == self.instrument
                    and (data_level := parsed[1]) is not None
                }
            )
            group_by = "data_level"
        if groups != self.groups:
            self.groups = groups
            self.columns = self._columns()
            self._refresh_column_metadata()
            self._install_group_slots()
        snapshot_rows = snapshot_status_rows(
            source_rows,
            self.groups,
            self.date_aggregation,
            self.aggregation_days,
            group_by=group_by,
        )
        widths = {
            (group, status): max(
                (len(str(row["counts"][group][status])) for row in snapshot_rows),
                default=1,
            )
            for group in self.groups
            for status in SUMMARY_STATUSES
        }
        rendered_rows: list[dict[str, object]] = []
        for row in snapshot_rows:
            rendered: dict[str, object] = {
                "row_id": row["row_id"],
                "first_date": row["first_date"],
                "last_date": row["last_date"],
            }
            for group in self.groups:
                rendered[group] = [
                    {
                        "text": str(row["counts"][group][status]).rjust(
                            widths[(group, status)]
                        ),
                        "color": (
                            "text-grey-3"
                            if ZERO_COUNTS_GRAY and row["counts"][group][status] == 0
                            else self.STATUS_COLORS[status]
                        ),
                    }
                    for status in SUMMARY_STATUSES
                ]
            rendered_rows.append(rendered)
        self.table.rows = (
            _sorted_rows(rendered_rows, self.sorting_rules)
            if self.sorting_rules
            else sorted(
                rendered_rows,
                key=lambda row: (str(row["first_date"]), str(row["last_date"])),
            )
        )
        self.table.update()


class DateTimePickerDialog(UIElem):
    """UTC date-and-time picker used by the range controls."""

    def __init__(
        self,
        *,
        title: str,
        on_apply: Callable[[datetime], None],
        on_cancel: Callable[[], None],
    ) -> None:
        self.dialog_title = title
        self.on_apply = on_apply
        self.on_cancel = on_cancel

    def render(self) -> None:
        with ui.dialog() as self.dialog, ui.card().classes("w-[44rem] max-w-full"):
            ui.label(self.dialog_title).classes("text-lg font-semibold")
            ui.label("Times are in UTC.").classes("text-sm text-slate-500")
            with ui.row().classes("w-full items-start gap-4 flex-wrap"):
                self.date_picker = ui.date(mask="YYYY-MM-DD")
                self.time_picker = ui.time(mask="HH:mm:ss").props("with-seconds")
            with ui.row().classes("w-full justify-end gap-2"):
                ui.button("Cancel", on_click=self._cancel).props("flat no-caps")
                ui.button("Apply", on_click=self._apply).props("unelevated no-caps")

    def open(self, value: datetime) -> None:
        value = value.astimezone(UTC)
        self.date_picker.value = value.strftime("%Y-%m-%d")
        self.time_picker.value = value.strftime("%H:%M:%S")
        self.dialog.open()

    def _apply(self) -> None:
        if not self.date_picker.value or not self.time_picker.value:
            ui.notify("Select both a date and time", type="negative")
            return
        value = datetime.fromisoformat(
            f"{self.date_picker.value}T{self.time_picker.value}"
        ).replace(tzinfo=UTC)
        self.dialog.close()
        self.on_apply(value)

    def _cancel(self) -> None:
        self.dialog.close()
        self.on_cancel()


class DaysBeforeDialog(UIElem):
    """Prompt for a custom number of days before the selected end time."""

    def __init__(
        self,
        *,
        on_apply: Callable[[float], None],
        on_cancel: Callable[[], None],
    ) -> None:
        self.on_apply = on_apply
        self.on_cancel = on_cancel

    def render(self) -> None:
        with ui.dialog() as self.dialog, ui.card().classes("w-96 max-w-full"):
            ui.label("Custom days before").classes("text-lg font-semibold")
            self.days_input = ui.number(
                label="Days before end",
                value=3,
                min=0,
                step=0.25,
            ).props("outlined")
            with ui.row().classes("w-full justify-end gap-2"):
                ui.button("Cancel", on_click=self._cancel).props("flat no-caps")
                ui.button("Apply", on_click=self._apply).props("unelevated no-caps")

    def open(self, value: float) -> None:
        self.days_input.value = value
        self.dialog.open()

    def _apply(self) -> None:
        days = float(self.days_input.value or 0)
        if days <= 0:
            ui.notify("Days before must be greater than zero", type="negative")
            return
        self.dialog.close()
        self.on_apply(days)

    def _cancel(self) -> None:
        self.dialog.close()
        self.on_cancel()


class SortingDialog(UIElem):
    """Edit shared multi-column sorting precedence."""

    # Temporary diagnostic: keep the complete sorting UI below intact while
    # verifying that an independently mounted dialog can render basic content.
    DIAGNOSTIC_HELLO_WORLD = False

    def __init__(
        self,
        columns: list[tuple[str, str]],
        on_apply: Callable[[list[SortRule]], None],
        on_cancel: Callable[[], None],
    ) -> None:
        self.columns = columns
        self.labels = dict(columns)
        self.on_apply = on_apply
        self.on_cancel = on_cancel
        self.rules = [SortRule(column) for column, _label in columns]
        self.dragged_column: str | None = None

    def render(self) -> None:
        with ui.dialog() as self.dialog, ui.card().classes("w-[42rem] max-w-full"):
            if self.DIAGNOSTIC_HELLO_WORLD:
                ui.label("Hello world").classes("text-2xl font-semibold")
                ui.label("Sorting dialog diagnostic content.").classes(
                    "text-sm text-slate-500"
                )
                ui.button("Close", on_click=self.dialog.close).props(
                    "unelevated no-caps"
                )
                return
            ui.label("Sorting").classes("text-lg font-semibold")
            ui.label(
                "Earlier columns take precedence. Drag rows or use the movement buttons."
            ).classes("text-sm text-slate-500")
            ui.button(
                "Reset to defaults",
                icon="restart_alt",
                on_click=self._reset_to_defaults,
            ).props("outline no-caps")
            self.list_container = ui.column().classes(
                "w-full gap-1 max-h-[60vh] overflow-auto"
            )
            self._build_rows()
            with ui.row().classes("w-full justify-end gap-2"):
                ui.button("Cancel", on_click=self._cancel).props("flat no-caps")
                ui.button("Apply", on_click=self._apply).props("unelevated no-caps")

    def open(self, rules: list[SortRule]) -> None:
        available = {column for column, _label in self.columns}
        seen: set[str] = set()
        self.rules = []
        for rule in rules:
            if rule.column in available and rule.column not in seen:
                self.rules.append(rule)
                seen.add(rule.column)
        self.rules.extend(
            SortRule(column) for column, _label in self.columns if column not in seen
        )
        if not self.DIAGNOSTIC_HELLO_WORLD:
            self._sync_rows()
        self.dialog.open()

    def _build_rows(self) -> None:
        self.row_elements: dict[str, Any] = {}
        self.direction_switches: dict[str, Any] = {}
        with self.list_container:
            for rule in self.rules:
                with (
                    ui.row()
                    .classes(
                        "w-full items-center gap-1 border rounded px-2 py-1 bg-white cursor-move"
                    )
                    .props("draggable=true") as row
                ):
                    self.row_elements[rule.column] = row
                    row.on(
                        "dragstart",
                        lambda _event, column=rule.column: self._drag_start(column),
                    )
                    row.on(
                        "dragover",
                        js_handler="(event) => event.preventDefault()",
                    )
                    row.on(
                        "drop",
                        lambda _event, column=rule.column: self._drop_on(column),
                    )
                    ui.icon("drag_indicator").classes("text-slate-400")
                    ui.label(self.labels[rule.column]).classes("grow")
                    self.direction_switches[rule.column] = ui.switch(
                        "Descending",
                        value=rule.descending,
                        on_change=lambda event, column=rule.column: self._set_direction(
                            column, bool(event.value)
                        ),
                    ).props("dense")
                    ui.button(
                        icon="vertical_align_top",
                        on_click=lambda _=None, column=rule.column: self._move_column(
                            column, "top"
                        ),
                    ).props("flat dense round")
                    ui.button(
                        icon="arrow_upward",
                        on_click=lambda _=None, column=rule.column: self._move_column(
                            column, "up"
                        ),
                    ).props("flat dense round")
                    ui.button(
                        icon="arrow_downward",
                        on_click=lambda _=None, column=rule.column: self._move_column(
                            column, "down"
                        ),
                    ).props("flat dense round")
                    ui.button(
                        icon="vertical_align_bottom",
                        on_click=lambda _=None, column=rule.column: self._move_column(
                            column, "bottom"
                        ),
                    ).props("flat dense round")

    def _sync_rows(self) -> None:
        for index, rule in enumerate(self.rules):
            self.row_elements[rule.column].move(self.list_container, index)
            self.direction_switches[rule.column].set_value(rule.descending)

    def _move_column(self, column: str, destination: str) -> None:
        source = next(i for i, rule in enumerate(self.rules) if rule.column == column)
        target = {
            "top": 0,
            "up": source - 1,
            "down": source + 1,
            "bottom": len(self.rules) - 1,
        }[destination]
        self._move(source, target)

    def _reset_to_defaults(self) -> None:
        defaults = AppSettingsState().sorting
        self.rules = [
            SortRule(rule.column, rule.descending)
            for rule in defaults
            if rule.column in self.row_elements
        ]
        existing = {rule.column for rule in self.rules}
        self.rules.extend(
            SortRule(column)
            for column, _label in self.columns
            if column not in existing
        )
        self._sync_rows()

    def _drag_start(self, column: str) -> None:
        self.dragged_column = column

    def _drop_on(self, target_column: str) -> None:
        if self.dragged_column is None or self.dragged_column == target_column:
            return
        source = next(
            i for i, rule in enumerate(self.rules) if rule.column == self.dragged_column
        )
        target = next(
            i for i, rule in enumerate(self.rules) if rule.column == target_column
        )
        self._move(source, target)
        self.dragged_column = None

    def _move(self, source: int, target: int) -> None:
        target = max(0, min(target, len(self.rules) - 1))
        if source == target:
            return
        rule = self.rules.pop(source)
        self.rules.insert(target, rule)
        self._sync_rows()

    def _set_direction(self, column: str, descending: bool) -> None:
        self.rules = [
            SortRule(rule.column, descending) if rule.column == column else rule
            for rule in self.rules
        ]

    def _apply(self) -> None:
        self.dialog.close()
        self.on_apply(list(self.rules))

    def _cancel(self) -> None:
        self.dialog.close()
        self.on_cancel()


class AppSettingsDialog(UIElem):
    """Dashboard presentation settings."""

    def __init__(
        self,
        on_apply: Callable[[set[str]], None],
        on_sorting: Callable[[], None],
    ) -> None:
        self.on_apply = on_apply
        self.on_sorting = on_sorting

    def render(self) -> None:
        with ui.dialog() as self.dialog, ui.card().classes("w-96 max-w-full"):
            ui.label("Settings").classes("text-lg font-semibold")
            ui.label("Optional columns").classes("text-sm font-medium text-slate-600")
            self.tags_checkbox = ui.checkbox("Tags")
            self.notes_checkbox = ui.checkbox("Notes")
            ui.separator()
            ui.label("Table sorting").classes("text-sm font-medium text-slate-600")
            ui.button(
                "Configure sorting", icon="sort", on_click=self._open_sorting
            ).props("outline no-caps")
            with ui.row().classes("w-full justify-end gap-2"):
                ui.button("Cancel", on_click=self.dialog.close).props("flat no-caps")
                ui.button("Apply", on_click=self._apply).props("unelevated no-caps")

    def open(self, visible_columns: set[str]) -> None:
        self.tags_checkbox.set_value("tags" in visible_columns)
        self.notes_checkbox.set_value("notes" in visible_columns)
        self.dialog.open()

    def _open_sorting(self) -> None:
        self.dialog.close()
        self.on_sorting()

    def _apply(self) -> None:
        visible_columns = {
            column
            for column, selected in (
                ("tags", self.tags_checkbox.value),
                ("notes", self.notes_checkbox.value),
            )
            if selected
        }
        self.dialog.close()
        self.on_apply(visible_columns)


@dataclass(frozen=True, slots=True)
class ExportSelections:
    main_csv: bool
    main_text: bool
    main_csv_partition_links: bool
    summary_csv: bool
    summary_text: bool


class ExportDialog(UIElem):
    """Choose one or more report downloads."""

    def __init__(
        self,
        *,
        selections: ExportSelections,
        on_change: Callable[[ExportSelections], None],
        on_download: Callable[[ExportSelections], None],
    ) -> None:
        self.selections = selections
        self.on_change = on_change
        self.on_download = on_download

    def render(self) -> None:
        with ui.dialog() as self.dialog, ui.card().classes("w-96 max-w-full"):
            ui.label("Export report").classes("text-lg font-semibold")
            ui.label("Main table").classes("text-sm font-semibold text-slate-700")
            self.main_csv = ui.checkbox(
                "CSV download",
                value=self.selections.main_csv,
                on_change=self._changed,
            )
            self.main_csv_partition_links = ui.checkbox(
                "Include partition links",
                value=self.selections.main_csv_partition_links,
                on_change=self._changed,
            ).classes("ml-7")
            self.main_text = ui.checkbox(
                "Plain text download",
                value=self.selections.main_text,
                on_change=self._changed,
            )
            ui.separator()
            ui.label("Summary table").classes("text-sm font-semibold text-slate-700")
            self.summary_csv = ui.checkbox(
                "CSV download",
                value=self.selections.summary_csv,
                on_change=self._changed,
            )
            self.summary_text = ui.checkbox(
                "Plain text download",
                value=self.selections.summary_text,
                on_change=self._changed,
            )
            with ui.row().classes("w-full justify-end gap-2"):
                ui.button("Cancel", on_click=self.dialog.close).props("flat no-caps")
                self.download_button = ui.button(
                    "Download all selected",
                    icon="download",
                    on_click=self._download,
                ).props("unelevated no-caps")
        self._update_enabled_state()

    def open(self) -> None:
        self.dialog.open()

    def _current(self) -> ExportSelections:
        return ExportSelections(
            main_csv=bool(self.main_csv.value),
            main_text=bool(self.main_text.value),
            main_csv_partition_links=bool(self.main_csv_partition_links.value),
            summary_csv=bool(self.summary_csv.value),
            summary_text=bool(self.summary_text.value),
        )

    def _changed(self) -> None:
        self.selections = self._current()
        self._update_enabled_state()
        self.on_change(self.selections)

    def _update_enabled_state(self) -> None:
        self.main_csv_partition_links.set_enabled(bool(self.main_csv.value))
        self.download_button.set_enabled(
            any(
                (
                    self.main_csv.value,
                    self.main_text.value,
                    self.summary_csv.value,
                    self.summary_text.value,
                )
            )
        )

    def _download(self) -> None:
        self.dialog.close()
        self.on_download(self._current())


class SnapshotPartitionTypeFilter(UIElem):
    """Staged hierarchical partition-type selector for snapshot rows."""

    def __init__(self, on_apply: Callable[[set[str]], object]) -> None:
        self.on_apply = on_apply
        self.available_types: set[str] = {"daily", "repoint"}
        self.selected_types: set[str] = set(self.available_types)
        self.pending_types: set[str] = set(self.selected_types)
        self._updating = False
        self.other_checkboxes: dict[str, Any] = {}

    def render(self) -> None:
        with ui.element("div") as self.container:
            self.button = (
                ui.button(
                    "Partition types: All",
                    icon="arrow_drop_down",
                    on_click=self._open,
                )
                .props("outline no-caps align=left")
                .classes("w-56")
            )
            with ui.menu() as self.menu:
                with ui.column().classes("p-3 gap-1 min-w-64"):
                    self.daily_checkbox = ui.checkbox(
                        "Daily", on_change=self._daily_changed
                    )
                    self.repoint_checkbox = ui.checkbox(
                        "Repoint", on_change=self._repoint_changed
                    )
                    self.other_checkbox = ui.checkbox(
                        "Other", on_change=self._other_changed
                    ).props("indeterminate-icon=remove")
                    with ui.column().classes("pl-7 gap-0") as self.other_container:
                        pass
                    with ui.row().classes("w-full justify-end pt-2"):
                        ui.button("Apply", on_click=self._apply).props(
                            "unelevated no-caps"
                        )
        self._rebuild_other_checkboxes()

    def set_available_types(self, values: set[str]) -> None:
        available = {"daily", "repoint", *values}
        newly_available = available - self.available_types
        self.available_types = available
        self.selected_types = (self.selected_types & available) | newly_available
        self.pending_types = set(self.selected_types)
        self._rebuild_other_checkboxes()
        self._update_button_label()

    def _other_types(self) -> list[str]:
        return sorted(self.available_types - {"daily", "repoint"})

    def _rebuild_other_checkboxes(self) -> None:
        self.other_container.clear()
        self.other_checkboxes = {}
        with self.other_container:
            for value in self._other_types():
                self.other_checkboxes[value] = ui.checkbox(
                    value,
                    on_change=lambda event, partition=value: self._child_changed(
                        partition, event
                    ),
                )
        self._sync_checkboxes()

    def _open(self) -> None:
        self.pending_types = set(self.selected_types)
        self._sync_checkboxes()
        self.menu.open()

    def _daily_changed(self, event: object) -> None:
        self._set_pending("daily", bool(getattr(event, "value", False)))

    def _repoint_changed(self, event: object) -> None:
        self._set_pending("repoint", bool(getattr(event, "value", False)))

    def _other_changed(self, event: object) -> None:
        if self._updating:
            return
        checked = bool(getattr(event, "value", False))
        if checked:
            self.pending_types.update(self._other_types())
        else:
            self.pending_types.difference_update(self._other_types())
        self._sync_checkboxes()

    def _child_changed(self, partition: str, event: object) -> None:
        self._set_pending(partition, bool(getattr(event, "value", False)))

    def _set_pending(self, partition: str, checked: bool) -> None:
        if self._updating:
            return
        if checked:
            self.pending_types.add(partition)
        else:
            self.pending_types.discard(partition)
        self._sync_checkboxes()

    def _sync_checkboxes(self) -> None:
        self._updating = True
        try:
            self.daily_checkbox.set_value("daily" in self.pending_types)
            self.repoint_checkbox.set_value("repoint" in self.pending_types)
            for value, checkbox in self.other_checkboxes.items():
                checkbox.set_value(value in self.pending_types)
            other_types = set(self._other_types())
            selected_other = other_types & self.pending_types
            other_value: bool | None = (
                True
                if other_types and selected_other == other_types
                else None
                if selected_other
                else False
            )
            cast(Any, self.other_checkbox).set_value(other_value)
        finally:
            self._updating = False

    def _apply(self) -> None:
        self.selected_types = set(self.pending_types)
        self._update_button_label()
        self.menu.close()
        self.on_apply(set(self.selected_types))

    def _update_button_label(self) -> None:
        selected = len(self.selected_types)
        total = len(self.available_types)
        label = "All" if selected == total else f"{selected} of {total}"
        self.button.set_text(f"Partition types: {label}")


class SnapshotDataLevelFilter(UIElem):
    """Staged hierarchical data-level selector for snapshot rows."""

    CATEGORIES = ("l0", "l1", "l2", "l3", "other")

    def __init__(self, on_apply: Callable[[set[str]], object]) -> None:
        self.on_apply = on_apply
        self.available_levels: set[str] = set()
        self.selected_levels: set[str] = set()
        self.pending_levels: set[str] = set()
        self._updating = False
        self.parent_checkboxes: dict[str, Any] = {}
        self.child_checkboxes: dict[str, Any] = {}

    def render(self) -> None:
        with ui.element("div") as self.container:
            self.button = (
                ui.button(
                    "Data levels: All",
                    icon="arrow_drop_down",
                    on_click=self._open,
                )
                .props("outline no-caps align=left")
                .classes("w-48")
            )
            with ui.menu() as self.menu:
                with ui.column().classes("p-3 gap-1 min-w-64"):
                    with ui.column().classes("gap-1") as self.options_container:
                        pass
                    with ui.row().classes("w-full justify-end pt-2"):
                        ui.button("Apply", on_click=self._apply).props(
                            "unelevated no-caps"
                        )
        self._rebuild_checkboxes()

    @staticmethod
    def _category(level: str) -> str:
        normalized = level.casefold()
        return normalized[:2] if re.match(r"^l[0-3]", normalized) else "other"

    def _levels_for(self, category: str) -> list[str]:
        return sorted(
            level
            for level in self.available_levels
            if self._category(level) == category
        )

    def set_available_levels(self, values: set[str]) -> None:
        newly_available = values - self.available_levels
        self.available_levels = set(values)
        self.selected_levels = (
            self.selected_levels & self.available_levels
        ) | newly_available
        self.pending_levels = set(self.selected_levels)
        self._rebuild_checkboxes()
        self._update_button_label()

    def _rebuild_checkboxes(self) -> None:
        self.options_container.clear()
        self.parent_checkboxes = {}
        self.child_checkboxes = {}
        with self.options_container:
            for category in self.CATEGORIES:
                self.parent_checkboxes[category] = ui.checkbox(
                    category.upper() if category != "other" else "Other",
                    on_change=lambda event, group=category: self._parent_changed(
                        group, event
                    ),
                ).props("indeterminate-icon=remove")
                with ui.column().classes("pl-7 gap-0"):
                    for level in self._levels_for(category):
                        self.child_checkboxes[level] = ui.checkbox(
                            level,
                            on_change=lambda event, value=level: self._child_changed(
                                value, event
                            ),
                        )
        self._sync_checkboxes()

    def _open(self) -> None:
        self.pending_levels = set(self.selected_levels)
        self._sync_checkboxes()
        self.menu.open()

    def _parent_changed(self, category: str, event: object) -> None:
        if self._updating:
            return
        levels = set(self._levels_for(category))
        if bool(getattr(event, "value", False)):
            self.pending_levels.update(levels)
        else:
            self.pending_levels.difference_update(levels)
        self._sync_checkboxes()

    def _child_changed(self, level: str, event: object) -> None:
        if self._updating:
            return
        if bool(getattr(event, "value", False)):
            self.pending_levels.add(level)
        else:
            self.pending_levels.discard(level)
        self._sync_checkboxes()

    def _sync_checkboxes(self) -> None:
        self._updating = True
        try:
            for level, checkbox in self.child_checkboxes.items():
                checkbox.set_value(level in self.pending_levels)
            for category, checkbox in self.parent_checkboxes.items():
                levels = set(self._levels_for(category))
                selected = levels & self.pending_levels
                value: bool | None = (
                    True
                    if levels and selected == levels
                    else None
                    if selected
                    else False
                )
                cast(Any, checkbox).set_value(value)
        finally:
            self._updating = False

    def _apply(self) -> None:
        self.selected_levels = set(self.pending_levels)
        self._update_button_label()
        self.menu.close()
        self.on_apply(set(self.selected_levels))

    def _update_button_label(self) -> None:
        selected = len(self.selected_levels)
        total = len(self.available_levels)
        label = "All" if selected == total else f"{selected} of {total}"
        self.button.set_text(f"Data levels: {label}")


class AssetToolbar(UIElem):
    def __init__(
        self,
        *,
        on_instrument_change: Callable[..., object],
        on_start_change: Callable[..., object],
        on_end_change: Callable[..., object],
        on_timestamp_filtering_change: Callable[..., object],
        on_unpartitioned_asset_change: Callable[..., object],
        on_view_change: Callable[..., object],
        on_dependency_graph_instrument_change: Callable[..., object],
        on_date_aggregation_change: Callable[..., object],
        on_aggregation_days_change: Callable[..., object],
        on_partition_types_change: Callable[[set[str]], object],
        on_data_levels_change: Callable[[set[str]], object],
        on_settings: Callable[..., object],
        on_export: Callable[..., object],
        on_refresh: Callable[..., object],
        on_cancel_load: Callable[..., object],
        settings: AppSettingsState,
    ) -> None:
        self.on_instrument_change = on_instrument_change
        self.on_start_change = on_start_change
        self.on_end_change = on_end_change
        self.on_timestamp_filtering_change = on_timestamp_filtering_change
        self.on_unpartitioned_asset_change = on_unpartitioned_asset_change
        self.on_view_change = on_view_change
        self.on_dependency_graph_instrument_change = (
            on_dependency_graph_instrument_change
        )
        self.on_date_aggregation_change = on_date_aggregation_change
        self.on_aggregation_days_change = on_aggregation_days_change
        self.on_partition_types_change = on_partition_types_change
        self.on_data_levels_change = on_data_levels_change
        self.on_settings = on_settings
        self.on_export = on_export
        self.on_refresh = on_refresh
        self.on_cancel_load = on_cancel_load
        self.settings = settings
        self.instrument_select: Select
        self.start_select: Select
        self.end_select: Select
        self.timestamp_filtering_select: Select
        self.unpartitioned_asset_select: Select
        self.refresh_button: Button
        self.view_select: Select
        self.dependency_graph_instrument_select: Select
        self.settings_button: Button
        self.export_button: Button
        self.cancel_load_button: Button
        self.partition_type_filter: SnapshotPartitionTypeFilter
        self.data_level_filter: SnapshotDataLevelFilter

    def render(self) -> None:
        with ui.column().classes("w-full gap-3"):
            with ui.row().classes("w-full items-end gap-3"):
                self.instrument_select = (
                    ui.select(
                        ["all", *ALL_INSTRUMENTS],
                        value=self.settings.instrument,
                        label="Instrument",
                        on_change=self.on_instrument_change,
                    )
                    .props("outlined")
                    .classes("w-40")
                )
                self.start_select = (
                    ui.select(
                        options={
                            "days_1": "1 day before",
                            "days_2": "2 days before",
                            "days_3": "3 days before",
                            "days_7": "7 days before",
                            "days_14": "14 days before",
                            "days_30": "30 days before",
                            "custom_days": "Custom days before…",
                            "custom_date": "Custom start date…",
                        },
                        value=self.settings.start_mode,
                        label="Start timestamp",
                        on_change=self._start_changed,
                    )
                    .props("outlined")
                    .classes("w-52")
                )
                self.start_select.on("popup-show", self._start_popup_opened)
                self.start_select.on("popup-hide", self._start_popup_closed)
                self.end_select = (
                    ui.select(
                        options={
                            "now": "Now",
                            "custom": "Custom end date…",
                        },
                        value=self.settings.end_mode,
                        label="End timestamp",
                        on_change=self._end_changed,
                    )
                    .props("outlined")
                    .classes("w-52")
                )
                self.end_select.on("popup-show", self._end_popup_opened)
                self.end_select.on("popup-hide", self._end_popup_closed)
                self.timestamp_filtering_select = (
                    ui.select(
                        options={
                            TimestampFiltering.ACTIVE_ONLY.value: "Active only",
                            TimestampFiltering.ACTIVE_OR_PARTITION.value: (
                                "Active + partition in range"
                            ),
                            TimestampFiltering.PARTITION_ONLY.value: (
                                "Partition in range only"
                            ),
                        },
                        value=self.settings.timestamp_filtering,
                        label="Timestamp filtering",
                        on_change=self.on_timestamp_filtering_change,
                    )
                    .props("outlined")
                    .classes("w-64")
                )
                self.refresh_button = ui.button(
                    "Refresh", icon="refresh", on_click=self.on_refresh
                ).props("unelevated")
                self.cancel_load_button = ui.button(
                    "Cancel load",
                    icon="cancel",
                    on_click=self.on_cancel_load,
                ).props("outline no-caps color=negative")
                self.cancel_load_button.set_visibility(False)
            with ui.row().classes("w-full items-end gap-3"):
                self.view_select = (
                    ui.select(
                        options={
                            "all_rows": "All rows",
                            "summary": "Summary",
                            "snapshot": "Snapshot",
                            "dependency_graph": "Dependency graph",
                        },
                        value=self.settings.view_mode,
                        label="View",
                        on_change=self.on_view_change,
                    )
                    .props("outlined")
                    .classes("w-40")
                )
                self.dependency_graph_instrument_select = (
                    ui.select(
                        list(DEPENDENCY_GRAPH_INSTRUMENTS),
                        value=self.settings.dependency_graph_instrument,
                        label="Instrument",
                        on_change=self.on_dependency_graph_instrument_change,
                    )
                    .props("outlined")
                    .classes("w-40")
                )
                self.dependency_graph_instrument_select.set_visibility(
                    self.settings.view_mode == "dependency_graph"
                )
                self.date_aggregation_select = (
                    ui.select(
                        options={
                            "all": "All dates",
                            "day": "Single days",
                            "week": "Weeks (Mon-Sun)",
                            "days": "Multiple days",
                        },
                        value=self.settings.summary_date_aggregation,
                        label="Date aggregation",
                        on_change=self.on_date_aggregation_change,
                    )
                    .props("outlined")
                    .classes("w-56")
                )
                self.date_aggregation_select.set_visibility(
                    self.settings.view_mode in {"summary", "snapshot"}
                )
                self.aggregation_days_input = (
                    ui.number(
                        label="Days per period",
                        value=self.settings.summary_aggregation_days,
                        min=MIN_AGGREGATION_DAYS,
                        step=1,
                        on_change=self.on_aggregation_days_change,
                    )
                    .props("outlined")
                    .classes("w-40")
                )
                self.aggregation_days_input.set_visibility(
                    self.settings.view_mode in {"summary", "snapshot"}
                    and self.settings.summary_date_aggregation == "days"
                )
                self.partition_type_filter = SnapshotPartitionTypeFilter(
                    self.on_partition_types_change
                ).build()
                self.partition_type_filter.container.set_visibility(
                    self.settings.view_mode == "snapshot"
                )
                self.data_level_filter = SnapshotDataLevelFilter(
                    self.on_data_levels_change
                ).build()
                self.data_level_filter.container.set_visibility(
                    self.settings.view_mode == "snapshot"
                )
                self.unpartitioned_asset_select = (
                    ui.select(
                        options={"hide": "Hide", "show": "Show"},
                        value=(
                            "show"
                            if self.settings.show_unpartitioned_assets
                            else "hide"
                        ),
                        label="Assets without partitions",
                        on_change=self.on_unpartitioned_asset_change,
                    )
                    .props("outlined")
                    .classes("w-56")
                )
                self.export_button = ui.button(
                    "Export", icon="download", on_click=self.on_export
                ).props("outline no-caps")
                self.settings_button = ui.button(
                    icon="settings", on_click=self.on_settings
                ).props("flat round")
                self.settings_button.tooltip("Settings")

    def set_loading(self, loading: bool, *, background: bool = False) -> None:
        interactive = not loading or background
        self.instrument_select.set_enabled(interactive)
        self.view_select.set_enabled(interactive)
        self.dependency_graph_instrument_select.set_enabled(interactive)
        self.date_aggregation_select.set_enabled(interactive)
        self.aggregation_days_input.set_enabled(interactive)
        self.partition_type_filter.button.set_enabled(interactive)
        self.data_level_filter.button.set_enabled(interactive)
        self.start_select.set_enabled(not loading)
        self.end_select.set_enabled(not loading)
        self.timestamp_filtering_select.set_enabled(not loading)
        self.unpartitioned_asset_select.set_enabled(interactive)
        self.refresh_button.set_enabled(not loading)
        self.export_button.set_enabled(interactive)
        self.settings_button.set_enabled(interactive)
        if loading:
            self.cancel_load_button.set_text(
                "Cancel update" if background else "Cancel load"
            )
        self.cancel_load_button.set_visibility(loading)
        self.cancel_load_button.set_enabled(loading)

    def _start_changed(self, event: object) -> object | None:
        return self.on_start_change(event)

    def _end_changed(self, event: object) -> object | None:
        return self.on_end_change(event)

    def _start_popup_opened(self) -> None:
        self._start_value_when_opened = self.start_select.value

    def _end_popup_opened(self) -> None:
        self._end_value_when_opened = self.end_select.value

    def _start_popup_closed(self) -> object | None:
        value = self.start_select.value
        if value == getattr(self, "_start_value_when_opened", None) and value in {
            "custom_days",
            "custom_date",
        }:
            return self.on_start_change(SimpleNamespace(value=value))
        return None

    def _end_popup_closed(self) -> object | None:
        value = self.end_select.value
        if value == getattr(self, "_end_value_when_opened", None) and value == "custom":
            return self.on_end_change(SimpleNamespace(value=value))
        return None

    def restore_start_value(self, value: str) -> None:
        self.start_select.value = value

    def restore_end_value(self, value: str) -> None:
        self.end_select.value = value


class DependencyGraphView(UIElem):
    def render(self) -> None:
        self.expanded = False
        with ui.column().classes("w-full gap-3") as self.container:
            with ui.row().classes("w-full items-center gap-2"):
                self.status_label = ui.label().classes("text-sm text-slate-500 grow")
                ui.button(icon="zoom_in", on_click=lambda: self._zoom("in")).props(
                    "flat round dense"
                ).tooltip("Zoom in")
                ui.button(icon="zoom_out", on_click=lambda: self._zoom("out")).props(
                    "flat round dense"
                ).tooltip("Zoom out")
                ui.button(icon="fit_screen", on_click=lambda: self._zoom("fit")).props(
                    "flat round dense"
                ).tooltip("Fit graph")
                ui.button(
                    icon="center_focus_strong",
                    on_click=lambda: self._zoom("reset"),
                ).props("flat round dense").tooltip("Reset zoom")
                self.expand_button = ui.button(
                    icon="fullscreen", on_click=self._toggle_expanded
                ).props("flat round dense")
                self.expand_button.tooltip("Expand graph")
            with ui.element("div").classes(
                "relative w-full h-[70vh] overflow-hidden border rounded-lg "
                "bg-white cursor-grab touch-none select-none"
            ) as self.viewport:
                self.diagram = ui.mermaid(
                    'flowchart LR\n    empty["Select an instrument"]',
                    config={
                        "flowchart": {"useMaxWidth": False, "htmlLabels": True},
                        "securityLevel": "strict",
                    },
                ).classes("absolute left-0 top-0")
        self._install_pan_and_zoom()

    def set_loading(self, instrument: str) -> None:
        self.status_label.set_text(f"Loading {instrument} dependencies...")

    def set_graph(self, graph: DependencyGraph, instrument: str) -> None:
        self.status_label.set_text(
            f"{len(graph.nodes)} nodes, {len(graph.edges)} dependencies"
        )
        self.diagram.set_content(dependency_graph_mermaid(graph, instrument))

    def set_error(self, message: str) -> None:
        self.status_label.set_text(message)
        self.diagram.set_content("")

    def _zoom(self, action: str) -> None:
        ui.run_javascript(
            "requestAnimationFrame(() => requestAnimationFrame(() => "
            f"window.sdsDependencyGraphControl?.({self.viewport.id}, {action!r})))"
        )

    def _toggle_expanded(self) -> None:
        self.expanded = not self.expanded
        expanded_classes = (
            "fixed inset-2 z-[3000] h-[calc(100vh-1rem)] p-3 bg-slate-50 shadow-2xl"
        )
        self.container.classes(
            add=expanded_classes if self.expanded else None,
            remove=None if self.expanded else expanded_classes,
        )
        self.viewport.classes(
            add="flex-1 min-h-0 h-auto" if self.expanded else "h-[70vh]",
            remove="h-[70vh]" if self.expanded else "flex-1 min-h-0 h-auto",
        )
        self.expand_button.props(
            f"icon={'fullscreen_exit' if self.expanded else 'fullscreen'}"
        )
        self.expand_button.tooltip(
            "Exit expanded view" if self.expanded else "Expand graph"
        )
        self._zoom("fit")

    def _install_pan_and_zoom(self) -> None:
        ui.run_javascript(
            f"""
            (() => {{
              const viewportId = {self.viewport.id};
              const viewport = document.getElementById(`c${{viewportId}}`);
              const diagram = document.getElementById(`c{self.diagram.id}`);
              if (!viewport || !diagram || viewport.dataset.panZoomReady) return;
              viewport.dataset.panZoomReady = 'true';
              const state = {{x: 20, y: 20, scale: 1, dragging: false}};
              const clamp = value => Math.min(4, Math.max(0.08, value));
              const apply = () => {{
                diagram.style.transformOrigin = '0 0';
                diagram.style.transform =
                  `translate(${{state.x}}px, ${{state.y}}px) scale(${{state.scale}})`;
              }};
              const fit = () => {{
                const svg = diagram.querySelector('svg');
                if (!svg) return;
                const box = svg.viewBox?.baseVal;
                const width = box?.width || svg.getBBox().width;
                const height = box?.height || svg.getBBox().height;
                if (!width || !height) return;
                diagram.style.width = `${{width}}px`;
                diagram.style.height = `${{height}}px`;
                state.scale = clamp(Math.min(
                  (viewport.clientWidth - 40) / width,
                  (viewport.clientHeight - 40) / height,
                ));
                state.x = (viewport.clientWidth - width * state.scale) / 2;
                state.y = (viewport.clientHeight - height * state.scale) / 2;
                apply();
              }};
              const zoom = (factor, clientX, clientY) => {{
                const bounds = viewport.getBoundingClientRect();
                const px = clientX - bounds.left;
                const py = clientY - bounds.top;
                const next = clamp(state.scale * factor);
                state.x = px - (px - state.x) * next / state.scale;
                state.y = py - (py - state.y) * next / state.scale;
                state.scale = next;
                apply();
              }};
              viewport.addEventListener('wheel', event => {{
                event.preventDefault();
                zoom(event.deltaY < 0 ? 1.15 : 1 / 1.15, event.clientX, event.clientY);
              }}, {{passive: false}});
              viewport.addEventListener('pointerdown', event => {{
                state.dragging = true;
                state.pointerX = event.clientX;
                state.pointerY = event.clientY;
                viewport.setPointerCapture(event.pointerId);
                viewport.style.cursor = 'grabbing';
              }});
              viewport.addEventListener('pointermove', event => {{
                if (!state.dragging) return;
                state.x += event.clientX - state.pointerX;
                state.y += event.clientY - state.pointerY;
                state.pointerX = event.clientX;
                state.pointerY = event.clientY;
                apply();
              }});
              const stopDragging = () => {{
                state.dragging = false;
                viewport.style.cursor = 'grab';
              }};
              viewport.addEventListener('pointerup', stopDragging);
              viewport.addEventListener('pointercancel', stopDragging);
              viewport.addEventListener('dblclick', fit);
              window.sdsDependencyGraphStates ??= {{}};
              window.sdsDependencyGraphStates[viewportId] = {{state, apply, fit, zoom}};
              window.sdsDependencyGraphControl = (id, action) => {{
                const controls = window.sdsDependencyGraphStates?.[id];
                if (!controls) return;
                if (action === 'fit') controls.fit();
                if (action === 'reset') {{
                  controls.state.x = 20;
                  controls.state.y = 20;
                  controls.state.scale = 1;
                  controls.apply();
                }}
                if (action === 'in' || action === 'out') {{
                  const bounds = viewport.getBoundingClientRect();
                  controls.zoom(
                    action === 'in' ? 1.25 : 0.8,
                    bounds.left + bounds.width / 2,
                    bounds.top + bounds.height / 2,
                  );
                }}
              }};
              new MutationObserver(() => requestAnimationFrame(fit)).observe(
                diagram, {{childList: true}}
              );
              requestAnimationFrame(fit);
            }})()
            """
        )


class AssetsStatusView(UIElem):
    def __init__(
        self,
        data_source: DagsterAssetsDataSource
        | CachedDagsterAssetsDataSource
        | None = None,
        metadata_store: AttemptMetadataStore | None = None,
        settings_store: AppSettingsStore | None = None,
        dependency_graph_loader: Callable[[str], DependencyGraph] | None = None,
    ) -> None:
        self.data_source = data_source or CachedDagsterAssetsDataSource()
        self.metadata_store = metadata_store or AttemptMetadataStore()
        self.settings_store = settings_store or AppSettingsStore()
        self.dependency_graph_loader: Callable[[str], DependencyGraph] = (
            dependency_graph_loader or load_dependency_graph
        )
        self.settings = self.settings_store.load()
        self.all_assets: list[AssetOption] = []
        self.assets: list[AssetOption] = []
        self.instrument: str = self.settings.instrument
        self.start_mode: str = self.settings.start_mode
        self.end_mode: str = self.settings.end_mode
        self.custom_days_before = self.settings.custom_days_before
        self.custom_start = self.settings.custom_start
        self.custom_end = self.settings.custom_end
        self.timestamp_filtering = TimestampFiltering(self.settings.timestamp_filtering)
        self.view_mode: str = self.settings.view_mode
        self.dependency_graph_instrument: str = (
            self.settings.dependency_graph_instrument
        )
        self._dependency_graph_cache: dict[str, DependencyGraph] = {}
        self._dependency_graph_generation = 0
        self._load_task: asyncio.Task[Any] | None = None
        self._load_generation = 0
        self.snapshot_summary_filter: tuple[str, str] | None = None
        self.snapshot_return_instrument: str | None = None
        combined_columns = cast(
            list[dict[str, object]],
            AssetsStatusTable.COLUMNS + AssetsStatusSummaryTable.COLUMNS,
        )
        self.sorting_columns = list(
            {
                str(column["name"]): str(column["label"]) for column in combined_columns
            }.items()
        )
        available_sort_columns = {column for column, _label in self.sorting_columns}
        saved_sorting = [
            SortRule(item.column, item.descending)
            for item in self.settings.sorting
            if item.column in available_sort_columns
        ]
        saved_columns = {rule.column for rule in saved_sorting}
        self.sorting_rules = saved_sorting + [
            SortRule(column)
            for column, _label in self.sorting_columns
            if column not in saved_columns
        ]
        self.export_selections = ExportSelections(
            main_csv=self.settings.export_main_csv,
            main_text=self.settings.export_main_text,
            main_csv_partition_links=(self.settings.export_main_csv_partition_links),
            summary_csv=self.settings.export_summary_csv,
            summary_text=self.settings.export_summary_text,
        )
        self.toolbar: AssetToolbar
        self.summary: StatusSummary
        self.table: AssetsStatusTable
        self.range_label: Label
        self.loading_label: Label

    def render(self) -> None:
        with ui.column().classes("w-[95vw] max-w-none mx-auto p-6 gap-5"):
            PageHeader().build()
            self.toolbar = AssetToolbar(
                on_instrument_change=self._on_instrument_change,
                on_start_change=self._on_start_change,
                on_end_change=self._on_end_change,
                on_timestamp_filtering_change=self._on_timestamp_filtering_change,
                on_unpartitioned_asset_change=self._on_unpartitioned_asset_change,
                on_view_change=self._on_view_change,
                on_dependency_graph_instrument_change=(
                    self._on_dependency_graph_instrument_change
                ),
                on_date_aggregation_change=self._on_date_aggregation_change,
                on_aggregation_days_change=self._on_aggregation_days_change,
                on_partition_types_change=self._on_partition_types_change,
                on_data_levels_change=self._on_data_levels_change,
                on_settings=self._open_settings,
                on_export=self._open_export,
                on_refresh=self._refresh,
                on_cancel_load=self._cancel_load,
                settings=self.settings,
            ).build()
            with ui.column().classes("gap-0"):
                self.range_label = ui.label().classes("text-sm text-slate-500")
                self.loading_label = ui.label().classes("text-sm text-slate-500")
            self.summary = StatusSummary(
                on_change=self._on_status_filter_change,
                visible_statuses=set(self.settings.visible_statuses),
            ).build()
            with ui.row().classes("w-full items-center") as self.drilldown_bar:
                ui.button(
                    icon="arrow_back",
                    on_click=self._back_to_summary,
                ).props("flat round dense").tooltip("Back to summary")
                self.drilldown_chip = ui.chip(
                    "Summary filter",
                    icon="filter_alt",
                    removable=True,
                    on_value_change=self._on_drilldown_chip_change,
                ).props("outline")
            self.drilldown_bar.set_visibility(False)
            with ui.row().classes("w-full items-center") as self.snapshot_filter_bar:
                ui.button(
                    icon="arrow_back",
                    on_click=self._back_to_snapshot,
                ).props("flat round dense").tooltip("Back to instrument snapshot")
                self.snapshot_filter_chip = ui.chip(
                    "Snapshot filter",
                    icon="filter_alt",
                    removable=True,
                    on_value_change=self._on_snapshot_filter_chip_change,
                ).props("outline")
            self.snapshot_filter_bar.set_visibility(False)
            self.table = AssetsStatusTable(
                on_metadata_change=self._on_metadata_change,
                on_settings_change=self._on_table_settings_change,
            ).build()
            shared_filter_settings = {
                **self.settings.summary_column_filters,
                **self.settings.column_filters,
            }
            self.table.restore_settings(
                self.settings.model_copy(
                    update={"column_filters": shared_filter_settings}
                )
            )
            self.summary_table = AssetsStatusSummaryTable(
                on_settings_change=self._on_summary_settings_change,
                on_filter=self._set_shared_filter,
                on_clear_filter=self._clear_shared_filter,
                on_drilldown=self._on_summary_drilldown,
            ).build()
            self.summary_table.restore_settings(self.settings)
            self.table.set_sorting(self.sorting_rules)
            self.summary_table.set_sorting(self.sorting_rules)
            self.summary_table.set_shared_filters(self.table.column_filters)
            self.snapshot_table = AssetsStatusSnapshotTable(
                self.instrument,
                on_group_click=self._on_snapshot_group_click,
            ).build()
            self.snapshot_table.restore_settings(self.settings)
            self.snapshot_table.set_sorting(self.sorting_rules)
            self.dependency_graph_view = DependencyGraphView().build()
            self._apply_view_visibility()
            self.start_dialog = DateTimePickerDialog(
                title="Custom start date",
                on_apply=self._set_custom_start,
                on_cancel=lambda: self.toolbar.restore_start_value(self.start_mode),
            ).build()
            self.end_dialog = DateTimePickerDialog(
                title="Custom end date",
                on_apply=self._set_custom_end,
                on_cancel=lambda: self.toolbar.restore_end_value(self.end_mode),
            ).build()
            self.days_dialog = DaysBeforeDialog(
                on_apply=self._set_custom_days_before,
                on_cancel=lambda: self.toolbar.restore_start_value(self.start_mode),
            ).build()
            self.settings_dialog = AppSettingsDialog(
                on_apply=self._apply_settings,
                on_sorting=self._open_sorting_settings,
            ).build()
            self.sorting_dialog = SortingDialog(
                self.sorting_columns,
                on_apply=self._apply_sorting_settings,
                on_cancel=lambda: None,
            ).build()
            self.export_dialog = ExportDialog(
                selections=self.export_selections,
                on_change=self._export_selections_changed,
                on_download=self._download_selected_exports,
            ).build()
        ui.timer(0, self._load_assets, once=True)
        if self.view_mode == "dependency_graph":
            ui.timer(0, self._load_dependency_graph, once=True)

    async def _load_assets(self) -> None:
        self._load_generation += 1
        load_generation = self._load_generation
        load_task = asyncio.current_task()
        self._load_task = load_task
        started = perf_counter()
        self._set_loading(True, "Loading assets…")
        try:
            loaded_assets = await run.io_bound(self.data_source.list_assets)
            if loaded_assets is None:
                raise RuntimeError("Asset loader returned no result")
            self.all_assets = loaded_assets
            self._filter_assets_by_instrument()
            if load_generation != self._load_generation:
                return
            if self.assets:
                await self._load_all_assets()
            else:
                self.loading_label.set_text("No asset records found.")
            logger.info(
                "Dashboard timing: initial load %.3fs",
                perf_counter() - started,
            )
        except asyncio.CancelledError:
            logger.info("Dashboard asset-list load cancelled")
            return
        except Exception as exc:
            self._show_error("Could not list Dagster assets", exc)
        finally:
            if self._load_task is load_task:
                self._load_task = None
                self.toolbar.set_loading(False)

    async def _refresh(self) -> None:
        await self._load_all_assets()

    async def set_instrument(self, instrument: str) -> None:
        """Select an instrument and reload only its matching assets."""
        if self.snapshot_summary_filter is not None and instrument != self.instrument:
            self._clear_snapshot_summary_filter()
        previous_assets = tuple(self.assets)
        self.instrument = instrument
        self.snapshot_table.set_instrument(instrument)
        self._filter_assets_by_instrument()
        self._schedule_settings_save()
        if self.all_assets and tuple(self.assets) != previous_assets:
            await self._load_all_assets()

    async def _on_instrument_change(self, event: object) -> None:
        instrument = str(getattr(event, "value", "all"))
        await self.set_instrument(instrument)

    def _filter_assets_by_instrument(self) -> None:
        self.assets = [
            asset
            for asset in self.all_assets
            if (self.view_mode == "snapshot" and self.instrument == "all")
            or self.instrument in {"all", asset.label}
            or asset.label.startswith(f"{self.instrument}_")
        ]

    def _open_settings(self) -> None:
        self.settings_dialog.open(self.table.visible_optional_columns)

    def _apply_settings(self, visible_optional_columns: set[str]) -> None:
        self.table.set_visible_optional_columns(visible_optional_columns)
        self._schedule_settings_save()

    def _open_sorting_settings(self) -> None:
        self.sorting_dialog.open(self.sorting_rules)

    def _apply_sorting_settings(self, sorting_rules: list[SortRule]) -> None:
        self.sorting_rules = sorting_rules
        self.table.set_sorting(sorting_rules)
        self.summary_table.set_sorting(sorting_rules)
        self.snapshot_table.set_sorting(sorting_rules)
        self._schedule_settings_save()

    def _open_export(self) -> None:
        self.export_dialog.open()

    def _export_selections_changed(self, selections: ExportSelections) -> None:
        self.export_selections = selections
        self._schedule_settings_save()

    def _download_selected_exports(self, selections: ExportSelections) -> None:
        self.export_selections = selections
        self._schedule_settings_save()
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        if selections.main_csv:
            self._download_export(
                view_name="all-rows",
                format_name="csv",
                timestamp=timestamp,
                include_partition_links=selections.main_csv_partition_links,
            )
        if selections.main_text:
            self._download_export(
                view_name="all-rows",
                format_name="text",
                timestamp=timestamp,
            )
        if selections.summary_csv:
            self._download_export(
                view_name="summary",
                format_name="csv",
                timestamp=timestamp,
            )
        if selections.summary_text:
            self._download_export(
                view_name="summary",
                format_name="text",
                timestamp=timestamp,
            )

    def _download_export(
        self,
        *,
        view_name: str,
        format_name: str,
        timestamp: str,
        include_partition_links: bool = False,
    ) -> None:
        if view_name == "summary":
            headers, rows = self.summary_table.export_data()
        else:
            headers, rows = self.table.export_data(
                include_partition_link=(
                    format_name == "csv" and include_partition_links
                )
            )
        if format_name == "csv":
            content = csv_table(headers, rows)
            extension = "csv"
            media_type = "text/csv; charset=utf-8"
        else:
            content = plain_text_table(headers, rows)
            extension = "txt"
            media_type = "text/plain; charset=utf-8"
        ui.download(
            content.encode("utf-8"),
            filename=f"imap-run-status-{view_name}-{timestamp}.{extension}",
            media_type=media_type,
        )

    async def _on_view_change(self, event: object) -> None:
        value = str(getattr(event, "value", ""))
        if value not in {"all_rows", "summary", "snapshot", "dependency_graph"}:
            return
        previous_assets = tuple(self.assets)
        if value == "summary":
            self._clear_summary_drilldown()
        else:
            self._clear_snapshot_summary_filter()
        self.view_mode = value
        self._filter_assets_by_instrument()
        self._apply_view_visibility()
        self._schedule_settings_save()
        if value == "dependency_graph":
            await self._load_dependency_graph()
        if tuple(self.assets) != previous_assets:
            await self._load_all_assets()

    async def _on_snapshot_group_click(self, group: str) -> None:
        if group not in self.snapshot_table.groups:
            return
        if self.instrument == "all":
            self.toolbar.instrument_select.value = group
            await self.set_instrument(group)
            return
        self.snapshot_summary_filter = (self.instrument, group)
        self.snapshot_return_instrument = self.instrument
        self.snapshot_filter_chip.set_text(f"{self.instrument} / {group}")
        self.snapshot_filter_chip.set_value(True)
        self.view_mode = "summary"
        self.toolbar.view_select.value = "summary"
        self._update_summary()
        self._apply_view_visibility()
        self._schedule_settings_save()

    def _on_snapshot_filter_chip_change(self, event: object) -> None:
        if getattr(event, "value", True) is False:
            self._clear_snapshot_summary_filter()

    def _clear_snapshot_summary_filter(self) -> None:
        if self.snapshot_summary_filter is None:
            return
        self.snapshot_summary_filter = None
        self.snapshot_filter_bar.set_visibility(False)
        self._update_summary()

    async def _back_to_snapshot(self) -> None:
        instrument = self.snapshot_return_instrument or self.instrument
        self._clear_snapshot_summary_filter()
        self.snapshot_return_instrument = None
        self.view_mode = "snapshot"
        self.toolbar.view_select.value = "snapshot"
        self.toolbar.instrument_select.value = instrument
        await self.set_instrument(instrument)
        self._apply_view_visibility()
        self._schedule_settings_save()

    def _on_summary_drilldown(self, row: SummaryRow) -> None:
        drilldown = summary_drilldown(
            row,
            self.summary_table.enabled_dimensions,
            self.summary_table.date_aggregation,
        )
        self.table.set_summary_drilldown(drilldown)
        self._update_summary()
        self.drilldown_chip.set_text(drilldown.label)
        self.drilldown_chip.set_value(True)
        self.view_mode = "all_rows"
        self.toolbar.view_select.value = "all_rows"
        self._apply_view_visibility()
        self._schedule_settings_save()

    def _on_drilldown_chip_change(self, event: object) -> None:
        if getattr(event, "value", True) is False:
            self._clear_summary_drilldown()

    def _back_to_summary(self) -> None:
        self._clear_summary_drilldown()
        self.view_mode = "summary"
        self.toolbar.view_select.value = "summary"
        self._apply_view_visibility()
        self._schedule_settings_save()

    def _clear_summary_drilldown(self) -> None:
        if self.table.summary_drilldown is None:
            return
        self.table.set_summary_drilldown(None)
        self.drilldown_bar.set_visibility(False)
        self._update_summary()

    async def _on_dependency_graph_instrument_change(self, event: object) -> None:
        value = str(getattr(event, "value", ""))
        if value not in DEPENDENCY_GRAPH_INSTRUMENTS:
            return
        self.dependency_graph_instrument = value
        self._schedule_settings_save()
        await self._load_dependency_graph()

    def _on_date_aggregation_change(self, event: object) -> None:
        value = str(getattr(event, "value", ""))
        if value not in {"all", "day", "week", "days"}:
            return
        self.toolbar.aggregation_days_input.set_visibility(value == "days")
        self.summary_table.set_date_aggregation(cast(SummaryDateAggregation, value))
        self.snapshot_table.set_date_aggregation(cast(SummaryDateAggregation, value))

    def _on_aggregation_days_change(self, event: object) -> None:
        try:
            value = int(
                getattr(
                    event,
                    "value",
                    self.summary_table.aggregation_days,
                )
            )
        except (TypeError, ValueError):
            return
        self.summary_table.set_aggregation_days(value)
        self.snapshot_table.set_aggregation_days(value)

    def _on_partition_types_change(self, values: set[str]) -> None:
        self.snapshot_table.set_partition_types(values)

    def _on_data_levels_change(self, values: set[str]) -> None:
        self.snapshot_table.set_data_levels(values)

    async def _load_dependency_graph(self) -> None:
        instrument = self.dependency_graph_instrument
        self._dependency_graph_generation += 1
        generation = self._dependency_graph_generation
        self.dependency_graph_view.set_loading(instrument)
        self.toolbar.dependency_graph_instrument_select.set_enabled(False)
        try:
            graph = self._dependency_graph_cache.get(instrument)
            if graph is None:
                loaded_graph = await run.io_bound(
                    self.dependency_graph_loader, instrument
                )
                if loaded_graph is None:
                    raise RuntimeError("Dependency graph loader returned no result")
                graph = loaded_graph
                self._dependency_graph_cache[instrument] = graph
            if generation == self._dependency_graph_generation:
                self.dependency_graph_view.set_graph(graph, instrument)
        except Exception:
            logger.exception("Could not load dependency graph for %s", instrument)
            if generation == self._dependency_graph_generation:
                self.dependency_graph_view.set_error(
                    f"Could not load the {instrument} dependency graph."
                )
        finally:
            if generation == self._dependency_graph_generation:
                self.toolbar.dependency_graph_instrument_select.set_enabled(True)

    def _apply_view_visibility(self) -> None:
        self.table.table.set_visibility(self.view_mode == "all_rows")
        self.summary_table.table.set_visibility(self.view_mode == "summary")
        self.snapshot_table.table.set_visibility(self.view_mode == "snapshot")
        show_date_aggregation = self.view_mode in {"summary", "snapshot"}
        self.toolbar.date_aggregation_select.set_visibility(show_date_aggregation)
        self.toolbar.aggregation_days_input.set_visibility(
            show_date_aggregation and self.summary_table.date_aggregation == "days"
        )
        self.toolbar.partition_type_filter.container.set_visibility(
            self.view_mode == "snapshot"
        )
        self.toolbar.data_level_filter.container.set_visibility(
            self.view_mode == "snapshot"
        )
        self.drilldown_bar.set_visibility(
            self.view_mode == "all_rows" and self.table.summary_drilldown is not None
        )
        self.snapshot_filter_bar.set_visibility(
            self.view_mode == "summary" and self.snapshot_summary_filter is not None
        )
        show_graph = self.view_mode == "dependency_graph"
        self.toolbar.dependency_graph_instrument_select.set_visibility(show_graph)
        self.dependency_graph_view.container.set_visibility(show_graph)

    async def _on_start_change(self, event: object) -> None:
        value = str(getattr(event, "value", ""))
        if value == "custom_days":
            self.days_dialog.open(self.custom_days_before)
            return
        if value == "custom_date":
            end = self._selected_end()
            self.start_dialog.open(self._selected_start(end))
            return
        if not value.startswith("days_"):
            return
        self.start_mode = value
        self._schedule_settings_save()
        await self._load_all_assets()

    async def _on_end_change(self, event: object) -> None:
        value = str(getattr(event, "value", ""))
        if value == "custom":
            self.end_dialog.open(self._selected_end())
            return
        if value != "now":
            return
        self.end_mode = value
        self._schedule_settings_save()
        await self._load_all_assets()

    async def _on_timestamp_filtering_change(self, event: object) -> None:
        value = getattr(event, "value", None)
        if value is None:
            return
        self.timestamp_filtering = TimestampFiltering(str(value))
        self.table.set_timestamp_filtering(self.timestamp_filtering)
        self._schedule_settings_save()
        await self._load_all_assets()

    def _on_status_filter_change(self, statuses: set[str]) -> None:
        self.table.set_visible_statuses(statuses)
        self._update_summary()
        self._schedule_settings_save()

    def _on_unpartitioned_asset_change(self, event: object) -> None:
        self.table.set_show_unpartitioned_assets(
            getattr(event, "value", "hide") == "show"
        )
        self._update_summary()
        self._schedule_settings_save()

    def _on_table_settings_change(self) -> None:
        self._update_summary()
        self._schedule_settings_save()

    def _on_summary_settings_change(self) -> None:
        self._schedule_settings_save()

    def _set_shared_filter(self, column: str, mode: str, value: FilterValue) -> None:
        self.table.set_column_filter(column, mode, value)

    def _clear_shared_filter(self, column: str) -> None:
        self.table.clear_column_filter(column)

    def _update_summary(self) -> None:
        timestamp_rows = self._apply_snapshot_summary_filter(
            self.table.rows_in_timestamp_scope()
        )
        column_filter_rows = self._apply_snapshot_summary_filter(
            self.table.rows_allowed_by_column_filters()
        )
        self.summary.update(
            timestamp_rows,
            column_filter_rows,
        )
        self.summary_table.set_shared_filters(self.table.column_filters)
        visible_rows = self.table.visible_rows()
        available_partition_types = {
            partition_type(row["partition"])
            for row in self.table.rows_in_timestamp_scope()
        }
        self.toolbar.partition_type_filter.set_available_types(
            available_partition_types
        )
        available_data_levels = {
            row["data_level"]
            for row in self.table.rows_in_timestamp_scope()
            if row["data_level"] is not None
        }
        self.toolbar.data_level_filter.set_available_levels(available_data_levels)
        self.snapshot_table.set_partition_types(
            self.toolbar.partition_type_filter.selected_types
        )
        self.snapshot_table.set_data_levels(
            self.toolbar.data_level_filter.selected_levels
        )
        self.summary_table.set_source_rows(
            self._apply_snapshot_summary_filter(visible_rows)
        )
        self.snapshot_table.set_source_rows(visible_rows)

    def _apply_snapshot_summary_filter(
        self, rows: list[AssetStatusRow]
    ) -> list[AssetStatusRow]:
        if self.snapshot_summary_filter is None:
            return rows
        instrument, data_level = self.snapshot_summary_filter
        return [
            row
            for row in rows
            if row["instrument"] == instrument and row["data_level"] == data_level
        ]

    def _schedule_settings_save(self) -> None:
        ui.timer(0, self._save_settings, once=True)

    async def _save_settings(self) -> None:
        settings = AppSettingsState(
            instrument=cast(InstrumentName, self.instrument),
            column_filters={
                column: ColumnFilterSettings(
                    mode=cast(FilterMode, mode),
                    value=list(value) if isinstance(value, tuple) else value,
                )
                for column, (mode, value) in self.table.column_filters.items()
            },
            sort_column=cast(SortColumn, self.table.sort_column),
            sort_descending=self.table.sort_descending,
            visible_statuses=cast(
                list[StatusName],
                [
                    status
                    for status in StatusSummary.STATUSES
                    if status in self.table.visible_statuses
                ],
            ),
            start_mode=cast(StartMode, self.start_mode),
            end_mode=cast(EndMode, self.end_mode),
            custom_days_before=self.custom_days_before,
            custom_start=self.custom_start,
            custom_end=self.custom_end,
            timestamp_filtering=self.timestamp_filtering.value,
            show_unpartitioned_assets=self.table.show_unpartitioned_assets,
            view_mode=cast(ViewMode, self.view_mode),
            dependency_graph_instrument=cast(
                DependencyGraphInstrument, self.dependency_graph_instrument
            ),
            summary_column_filters={
                column: ColumnFilterSettings(
                    mode=cast(FilterMode, mode),
                    value=list(value) if isinstance(value, tuple) else value,
                )
                for column, (mode, value) in self.summary_table.date_filters.items()
            },
            summary_sort_column=cast(SummarySortColumn, self.summary_table.sort_column),
            summary_sort_descending=self.summary_table.sort_descending,
            sorting=[
                ColumnSortSettings(
                    column=rule.column,
                    descending=rule.descending,
                )
                for rule in self.sorting_rules
            ],
            summary_group_dimensions=cast(
                list[SummaryGroupDimension],
                [
                    dimension
                    for dimension in SUMMARY_DIMENSIONS
                    if dimension in self.summary_table.enabled_dimensions
                ],
            ),
            summary_date_aggregation=self.summary_table.date_aggregation,
            summary_aggregation_days=self.summary_table.aggregation_days,
            visible_optional_columns=cast(
                list[OptionalColumn],
                [
                    column
                    for column in self.table.OPTIONAL_COLUMNS
                    if column in self.table.visible_optional_columns
                ],
            ),
            export_main_csv=self.export_selections.main_csv,
            export_main_text=self.export_selections.main_text,
            export_main_csv_partition_links=(
                self.export_selections.main_csv_partition_links
            ),
            export_summary_csv=self.export_selections.summary_csv,
            export_summary_text=self.export_selections.summary_text,
        )
        try:
            await run.io_bound(self.settings_store.save, settings)
        except Exception:
            logger.exception("Could not save application settings")
            ui.notify("Could not save application settings", type="negative")

    async def _on_metadata_change(self, event: GenericEventArguments) -> None:
        attempt_id = event.args.get("attempt_id")
        if not attempt_id:
            return
        row_id = str(event.args["row_id"])
        field = str(event.args["field"])
        value = str(event.args.get("value") or "")
        try:
            if field == "tags":
                tags = [tag.strip() for tag in value.split(";") if tag.strip()]
                metadata = await run.io_bound(
                    self.metadata_store.set_tags, str(attempt_id), tags
                )
                display_value = "; ".join(metadata.tags)
            elif field == "notes":
                metadata = await run.io_bound(
                    self.metadata_store.set_notes, str(attempt_id), value
                )
                display_value = metadata.notes
            else:
                return
            self.table.update_metadata(row_id, field, display_value)
        except ValidationError as exc:
            self.table.refresh()
            message = exc.errors()[0].get("msg", "Invalid metadata")
            ui.notify(str(message).removeprefix("Value error, "), type="negative")
        except Exception:
            self.table.refresh()
            logger.exception("Could not save attempt metadata")
            ui.notify("Could not save metadata", type="negative")

    def _set_custom_start(self, value: datetime) -> None:
        self.custom_start = value
        self.start_mode = "custom_date"
        self.toolbar.restore_start_value(self.start_mode)
        self._schedule_settings_save()
        ui.timer(0, self._load_all_assets, once=True)

    def _set_custom_end(self, value: datetime) -> None:
        self.custom_end = value
        self.end_mode = "custom"
        self.toolbar.restore_end_value(self.end_mode)
        self._schedule_settings_save()
        ui.timer(0, self._load_all_assets, once=True)

    def _set_custom_days_before(self, value: float) -> None:
        self.custom_days_before = value
        self.start_mode = "custom_days"
        self.toolbar.restore_start_value(self.start_mode)
        self._schedule_settings_save()
        ui.timer(0, self._load_all_assets, once=True)

    def _selected_end(self) -> datetime:
        return datetime.now(UTC) if self.end_mode == "now" else self.custom_end

    def _selected_start(self, end: datetime) -> datetime:
        if self.start_mode == "custom_date":
            return self.custom_start
        days = (
            self.custom_days_before
            if self.start_mode == "custom_days"
            else float(self.start_mode.removeprefix("days_"))
        )
        return end - timedelta(days=days)

    async def _load_all_assets(self) -> None:
        self._load_generation += 1
        load_generation = self._load_generation
        self._load_task = asyncio.current_task()
        total_started = perf_counter()
        window_end = self._selected_end()
        window_start = self._selected_start(window_end)
        logger.info(
            "Dashboard query: start=%s end=%s start_mode=%s end_mode=%s "
            "timestamp_filtering=%s include_unpartitioned=%s assets=%d",
            window_start.isoformat(),
            window_end.isoformat(),
            self.start_mode,
            self.end_mode,
            self.timestamp_filtering.value,
            self.table.show_unpartitioned_assets,
            len(self.assets),
        )
        if window_start >= window_end:
            self._show_error(
                "Could not load assets",
                ValueError("start timestamp must be before end timestamp"),
            )
            return
        include_recent_activity = (
            self.timestamp_filtering is not TimestampFiltering.PARTITION_ONLY
        )
        include_partition_ranges = (
            self.timestamp_filtering is not TimestampFiltering.ACTIVE_ONLY
        )
        self.table.set_activity_window(window_start, window_end)
        self._set_range_label(window_start, window_end)
        cached_rows = await self._load_cached_rows(
            window_start,
            window_end,
            include_recent_activity=include_recent_activity,
            include_partition_ranges=include_partition_ranges,
        )
        self._show_cached_rows(cached_rows, window_start, window_end)
        self._set_loading(
            True,
            (
                "Checking Dagster for updates…"
                if cached_rows is not None
                else "Loading all assets for the selected date range…"
            ),
            background=cached_rows is not None,
        )
        try:
            data_started = perf_counter()
            rows = await run.io_bound(
                self.data_source.load_recent_status_rows,
                start=window_start,
                end=window_end,
                include_recent_activity=include_recent_activity,
                include_partition_ranges=include_partition_ranges,
                assets=tuple(self.assets),
            )
            if rows is None:
                raise RuntimeError("Asset status loader returned no result")
            if load_generation != self._load_generation:
                return
            metadata_by_attempt = await run.io_bound(
                self.metadata_store.get_many,
                (
                    attempt_id
                    for row in rows
                    if (attempt_id := row["attempt_id"]) is not None
                ),
            )
            if metadata_by_attempt is None:
                raise RuntimeError("Attempt metadata loader returned no result")
            if load_generation != self._load_generation:
                return
            for row in rows:
                attempt_id = row["attempt_id"]
                if attempt_id is None or attempt_id not in metadata_by_attempt:
                    continue
                metadata = metadata_by_attempt[attempt_id]
                row["tags"] = "; ".join(metadata.tags)
                row["notes"] = metadata.notes
            data_elapsed = perf_counter() - data_started
            ui_started = perf_counter()
            self.table.set_rows(rows)
            self._update_summary()
            self._set_range_label(
                window_start,
                window_end,
                prefix=f"{len(rows):,} partitions loaded",
            )
            ui_elapsed = perf_counter() - ui_started
            logger.info(
                "Dashboard timing: background data %.3fs; UI application %.3fs; "
                "load total %.3fs (%d rows)",
                data_elapsed,
                ui_elapsed,
                perf_counter() - total_started,
                len(rows),
            )
        except asyncio.CancelledError:
            logger.info("Dashboard load cancelled")
            return
        except Exception as exc:
            self._show_error("Could not load Dagster status rows", exc)
        finally:
            if load_generation == self._load_generation:
                self._set_loading(False)
                self._load_task = None

    async def _load_cached_rows(
        self,
        start: datetime,
        end: datetime,
        *,
        include_recent_activity: bool,
        include_partition_ranges: bool,
    ) -> list[AssetStatusRow] | None:
        if not isinstance(self.data_source, CachedDagsterAssetsDataSource):
            return None
        return await run.io_bound(
            self.data_source.load_cached_status_rows,
            start=start,
            end=end,
            include_recent_activity=include_recent_activity,
            include_partition_ranges=include_partition_ranges,
            assets=tuple(self.assets),
        )

    def _show_cached_rows(
        self,
        rows: list[AssetStatusRow] | None,
        start: datetime,
        end: datetime,
    ) -> None:
        self.table.set_rows(rows or [])
        self._update_summary()
        if rows is not None:
            self._set_range_label(
                start,
                end,
                prefix=f"{len(rows):,} cached partitions loaded",
            )

    def _cancel_load(self) -> None:
        self._load_generation += 1
        task = self._load_task
        self._load_task = None
        if task is not None and not task.done():
            task.cancel()
        retained_rows = len(self.table.table.rows)
        self.loading_label.set_text(
            f"Update cancelled · {retained_rows:,} displayed partitions retained"
        )
        self.toolbar.set_loading(False)

    def _set_loading(
        self,
        loading: bool,
        message: str = "",
        *,
        background: bool = False,
    ) -> None:
        self.toolbar.set_loading(loading, background=background)
        if loading:
            self.loading_label.set_text(message)
        else:
            self.loading_label.set_text("")

    def _set_range_label(
        self,
        start: datetime,
        end: datetime,
        *,
        prefix: str = "Selected range",
    ) -> None:
        self.range_label.set_text(
            f"{prefix} · {_format_status_timestamp(start)} to "
            f"{_format_status_timestamp(end)}"
        )

    def _show_error(self, message: str, exc: Exception) -> None:
        response = getattr(exc, "response", None)
        if response is not None:
            request = getattr(response, "request", None)
            request_method = getattr(request, "method", "unknown")
            request_url = getattr(request, "url", "unknown")
            try:
                response_body = response.text
            except Exception:  # pragma: no cover - defensive diagnostics
                response_body = "<response body could not be decoded>"
            logger.error(
                "%s: %s: %s\nHTTP request: %s %s\nHTTP status: %s\n"
                "HTTP response body:\n%s",
                message,
                type(exc).__name__,
                exc,
                request_method,
                request_url,
                getattr(response, "status_code", "unknown"),
                response_body[:8_000],
                exc_info=(type(exc), exc, exc.__traceback__),
            )
        else:
            logger.error(
                "%s: %s: %s",
                message,
                type(exc).__name__,
                exc,
                exc_info=(type(exc), exc, exc.__traceback__),
            )
        self.loading_label.set_text(f"{message}: {exc}")
        ui.notify(message, type="negative")
