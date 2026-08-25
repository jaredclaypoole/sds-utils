"""NiceGUI views and controls for Dagster backfills."""

import logging
from collections.abc import Callable
from dataclasses import asdict
from typing import cast
from urllib.parse import quote

from nicegui import run, ui
from nicegui.elements.button import Button
from nicegui.elements.label import Label
from nicegui.elements.select import Select
from nicegui.elements.table import Table
from nicegui.events import GenericEventArguments

from sds_utils.dashboard.dagster_graphql_client.enums import BulkActionStatus

from ..backend.backfills import (
    BackfillDetail,
    BackfillRunDetail,
    DagsterBackfillsDataSource,
)
from .elems import (
    FilterableSortableTable,
    SortingDialog,
    SortRule,
    StatusFilterCard,
    UIElem,
    _sorted_rows,
)
from .models import (
    BackfillTableSettingsState,
    ColumnFilterSettings,
    ColumnSortSettings,
    FilterMode,
)
from .settings_store import BackfillTableSettingsStore

logger = logging.getLogger("uvicorn.error.sds_utils.dashboard.frontend.backfills")

BACKFILL_STATUSES = tuple(status.value for status in BulkActionStatus)
DEFAULT_BACKFILL_STATUSES = (BulkActionStatus.REQUESTED.value,)


PARTITION_STATUSES = (
    "materialized",
    "failed",
    "skipped",
    "log-skipped",
    "missing-output",
    "in_progress",
)
PARTITION_STATUS_CLASSES = {
    "materialized": "border-green-400 bg-green-50 text-green-900",
    "failed": "border-red-400 bg-red-50 text-red-900",
    "skipped": "border-amber-400 bg-amber-50 text-amber-900",
    "log-skipped": "border-cyan-400 bg-cyan-50 text-cyan-900",
    "missing-output": "border-blue-400 bg-blue-50 text-blue-900",
    "in_progress": "border-violet-400 bg-violet-50 text-violet-900",
}


class PartitionStatusFilterCard(StatusFilterCard):
    ACTIVE_CLASSES = PARTITION_STATUS_CLASSES


class PartitionStatusSummary(UIElem):
    """Clickable partition-status counts which control table visibility."""

    def __init__(
        self,
        on_change: Callable[[set[str]], None],
        visible_statuses: set[str],
    ) -> None:
        self.on_change = on_change
        self.visible_statuses = visible_statuses
        self.cards: dict[str, PartitionStatusFilterCard] = {}

    def render(self) -> None:
        with ui.row().classes("w-full gap-3 flex-wrap"):
            for status in PARTITION_STATUSES:
                self.cards[status] = PartitionStatusFilterCard(
                    status,
                    self._status_toggled,
                    active=status in self.visible_statuses,
                    item_label="partitions",
                ).build()

    def update(
        self,
        rows: list[dict[str, object]],
        shown_rows: list[dict[str, object]],
    ) -> None:
        totals = {status: 0 for status in PARTITION_STATUSES}
        shown = {status: 0 for status in PARTITION_STATUSES}
        for row in rows:
            totals[str(row["status"])] += 1
        for row in shown_rows:
            shown[str(row["status"])] += 1
        for status, card in self.cards.items():
            card.set_count(shown[status], totals[status])

    def _status_toggled(self, _status: str) -> None:
        self.on_change({status for status, card in self.cards.items() if card.active})


class BackfillRunsTable(FilterableSortableTable):
    """Filterable and sortable asset/partition/run table."""

    STATUS_FILTER_OPTIONS = PARTITION_STATUSES
    COLUMNS = [
        {"name": "instrument", "label": "Instrument", "field": "instrument"},
        {"name": "data_level", "label": "Data level", "field": "data_level"},
        {"name": "descriptor", "label": "Descriptor", "field": "descriptor"},
        {"name": "partition", "label": "Partition", "field": "partition"},
        {"name": "status", "label": "Partition status", "field": "status"},
        {"name": "run_status", "label": "Run status", "field": "run_status"},
        {"name": "run_id", "label": "Run", "field": "run_id"},
    ]

    def __init__(
        self,
        *,
        on_settings_change: Callable[[], None],
        on_rows_change: Callable[
            [list[dict[str, object]], list[dict[str, object]]], None
        ],
        visible_statuses: set[str],
    ) -> None:
        super().__init__(
            on_settings_change=on_settings_change,
            columns=self.COLUMNS,
        )
        self.on_rows_change = on_rows_change
        self.visible_statuses = visible_statuses
        self.all_rows: list[dict[str, object]] = []

    def render(self) -> None:
        self.table = ui.table(
            columns=self.columns,
            rows=[],
            row_key="row_id",
            pagination={"rowsPerPage": 50},
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
                <q-icon v-if="props.col.filter_value" name="filter_alt" color="primary" size="xs" />
                <q-icon
                  v-if="props.col.sort_direction"
                  :name="props.col.sort_direction === 'asc' ? 'arrow_upward' : 'arrow_downward'"
                  color="primary" size="xs"
                />
                <q-icon name="expand_more" size="xs" />
              </div>
            </q-th>
            """,
        )
        self.table.add_slot(
            "body-cell-partition",
            """
            <q-td :props="props">
              <a
                :href="props.row.partition_url" target="_blank"
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
              <q-badge
                :color="({
                  'materialized': 'positive',
                  'failed': 'negative',
                  'skipped': 'warning',
                  'log-skipped': 'cyan-8',
                  'missing-output': 'blue-8',
                  'in_progress': 'deep-purple',
                })[props.value]"
                :label="props.value" outline
              />
            </q-td>
            """,
        )
        self.table.add_slot(
            "body-cell-run_status",
            """
            <q-td :props="props">
              <q-badge
                :color="props.value === 'SUCCESS' ? 'positive'
                  : props.value === 'FAILURE' ? 'negative'
                  : ['STARTING', 'STARTED'].includes(props.value) ? 'deep-purple'
                  : 'grey-7'"
                :label="props.value" outline
              />
            </q-td>
            """,
        )
        self.table.add_slot(
            "body-cell-run_id",
            """
            <q-td :props="props">
              <a
                :href="props.row.run_url" target="_blank" rel="noopener noreferrer"
                class="text-blue-700 hover:text-blue-900 hover:underline"
              >{{ props.value }}</a>
            </q-td>
            """,
        )
        self._attach_column_controls()

    def set_rows(self, rows: tuple[BackfillRunDetail, ...]) -> None:
        self.all_rows = [asdict(row) for row in rows]
        self._apply_filters()

    def set_visible_statuses(self, statuses: set[str]) -> None:
        self.visible_statuses = statuses
        self._apply_filters()
        self.on_settings_change()

    def restore_settings(self, settings: BackfillTableSettingsState) -> None:
        available = {str(column["name"]) for column in self.COLUMNS}
        self.column_filters = {
            column: (
                item.mode,
                tuple(item.value) if isinstance(item.value, list) else item.value,
            )
            for column, item in settings.column_filters.items()
            if column in available
            and not (
                column == "status"
                and item.mode == "one_of"
                and isinstance(item.value, list)
                and not set(item.value) & set(PARTITION_STATUSES)
            )
        }
        self.visible_statuses = _visible_partition_statuses(settings)
        self.set_sorting(
            [
                SortRule(item.column, item.descending)
                for item in settings.sorting
                if item.column in available
            ]
        )

    def rows_allowed_by_column_filters(self) -> list[dict[str, object]]:
        return [row for row in self.all_rows if self._matches_column_filters(row)]

    def _apply_filters(self) -> None:
        column_rows = self.rows_allowed_by_column_filters()
        self.table.rows = _sorted_rows(
            [row for row in column_rows if row["status"] in self.visible_statuses],
            self.sorting_rules,
        )
        self.table.update()
        self.on_rows_change(self.all_rows, column_rows)


class BackfillsView(UIElem):
    """Browse Dagster backfills and drill into their asset/partition runs."""

    def __init__(self, data_source: DagsterBackfillsDataSource | None = None) -> None:
        self.data_source = data_source or DagsterBackfillsDataSource()
        self.selected_statuses = list(DEFAULT_BACKFILL_STATUSES)

    def render(self) -> None:
        with ui.column().classes("w-[95vw] max-w-none mx-auto p-6 gap-5"):
            with ui.row().classes("w-full items-center justify-between"):
                with ui.column().classes("gap-1"):
                    ui.label("Dagster backfills").classes(
                        "text-3xl font-semibold text-slate-900"
                    )
                    ui.label(
                        "Inspect backfill progress and open partition-run logs."
                    ).classes("text-sm text-slate-500")
                ui.button(
                    "Asset status dashboard",
                    icon="arrow_back",
                    on_click=lambda: ui.navigate.to("/assets"),
                ).props("flat no-caps")

            with ui.row().classes("w-full items-end gap-3"):
                self.status_select: Select = (
                    ui.select(
                        options={
                            status: _status_label(status)
                            for status in BACKFILL_STATUSES
                        },
                        value=self.selected_statuses,
                        label="Backfill status",
                        multiple=True,
                        on_change=self._on_status_change,
                    )
                    .props("outlined use-chips")
                    .classes("w-[34rem] max-w-full")
                )
                self.refresh_button: Button = ui.button(
                    "Refresh", icon="refresh", on_click=self._load_backfills
                ).props("unelevated")

            self.loading_label: Label = ui.label().classes("text-sm text-slate-500")
            self.backfills_table: Table = ui.table(
                columns=[
                    {"name": "status", "label": "Status", "field": "status"},
                    {"name": "title", "label": "Title", "field": "title"},
                    {"name": "id", "label": "Backfill ID", "field": "id"},
                    {"name": "created", "label": "Created", "field": "created"},
                    {
                        "name": "partitions",
                        "label": "Partitions",
                        "field": "partitions",
                    },
                    {"name": "actions", "label": "", "field": "actions"},
                ],
                rows=[],
                row_key="id",
                pagination={"rowsPerPage": 25},
            ).classes("w-full shadow-none border rounded-lg")
            self.backfills_table.props("flat bordered separator=horizontal")
            self.backfills_table.add_slot(
                "body-cell-actions",
                """
                <q-td :props="props">
                  <q-btn
                    flat dense no-caps color="primary" label="View"
                    @click="$parent.$emit('select-backfill', props.row)"
                  />
                </q-td>
                """,
            )
            self.backfills_table.on("select-backfill", self._select_backfill)
        ui.timer(0, self._load_backfills, once=True)

    async def _on_status_change(self, event: object) -> None:
        self.selected_statuses = list(getattr(event, "value", None) or [])
        await self._load_backfills()

    async def _load_backfills(self) -> None:
        self._set_loading(True, "Loading backfills…")
        try:
            summaries = await run.io_bound(
                self.data_source.list_backfills, self.selected_statuses
            )
            if summaries is None:
                raise RuntimeError("Backfill loader returned no result")
            self.backfills_table.rows = [
                {
                    **asdict(summary),
                    "actions": "",
                }
                for summary in summaries
            ]
            self.backfills_table.update()
            self.loading_label.set_text(
                f"{len(summaries):,} backfill{'s' if len(summaries) != 1 else ''}"
            )
        except Exception:
            logger.exception("Could not load Dagster backfills")
            self.loading_label.set_text("Could not load backfills.")
            ui.notify("Could not load Dagster backfills", type="negative")
        finally:
            self._set_loading(False)

    def _select_backfill(self, event: GenericEventArguments) -> None:
        backfill_id = str(event.args["id"])
        ui.navigate.to(f"/backfills/{quote(backfill_id, safe='')}")

    def _set_loading(self, loading: bool, message: str | None = None) -> None:
        self.status_select.set_enabled(not loading)
        self.refresh_button.set_enabled(not loading)
        self.backfills_table.set_visibility(not loading)
        if message is not None:
            self.loading_label.set_text(message)


class BackfillDetailView(UIElem):
    """Display one backfill's per-asset counts and partition runs."""

    def __init__(
        self,
        backfill_id: str,
        data_source: DagsterBackfillsDataSource | None = None,
        settings_store: BackfillTableSettingsStore | None = None,
    ) -> None:
        self.backfill_id = backfill_id
        self.data_source = data_source or DagsterBackfillsDataSource()
        self.settings_store = settings_store or BackfillTableSettingsStore()
        self.table_settings = self.settings_store.load()
        self.visible_partition_statuses = _visible_partition_statuses(
            self.table_settings
        )
        self.backfill: BackfillDetail | None = None
        self.runs_loaded = False
        self.runs_loading = False

    def render(self) -> None:
        with ui.column().classes("w-[95vw] max-w-none mx-auto p-6 gap-5"):
            with ui.row().classes("w-full items-center justify-between"):
                with ui.column().classes("gap-1"):
                    self.title_label = ui.label(f"Backfill {self.backfill_id}").classes(
                        "text-3xl font-semibold text-slate-900"
                    )
                    self.summary_label = ui.label("Loading backfill…").classes(
                        "text-sm text-slate-500"
                    )
                with ui.row().classes("items-center gap-2"):
                    ui.button(
                        "All backfills",
                        icon="arrow_back",
                        on_click=lambda: ui.navigate.to("/backfills"),
                    ).props("flat no-caps")
                    self.dagster_button = ui.button(
                        "Open in Dagster",
                        icon="open_in_new",
                        on_click=self._open_in_dagster,
                    ).props("flat no-caps")

            self.loading_label = ui.label().classes("text-sm text-slate-500")
            self.content = ui.column().classes("w-full gap-4")
            with self.content:
                with ui.tabs(on_change=self._on_tab_change) as self.tabs:
                    self.counts_tab = ui.tab("counts", "Backfill counts")
                    self.runs_tab = ui.tab("runs", "Detailed runs")
                with ui.tab_panels(self.tabs, value=self.counts_tab).classes("w-full"):
                    with ui.tab_panel(self.counts_tab).classes("p-0"):
                        self._build_counts_table()
                    with ui.tab_panel(self.runs_tab).classes("p-0"):
                        self.runs_loading_label = ui.label(
                            "Select this tab to load detailed runs."
                        ).classes("text-sm text-slate-500")
                        self._build_runs_table()
            self.content.set_visibility(False)
        ui.timer(0, self._load, once=True)

    async def _load(self) -> None:
        self.loading_label.set_text(f"Loading backfill {self.backfill_id}…")
        self.dagster_button.set_enabled(False)
        try:
            detail = await run.io_bound(
                self.data_source.load_backfill, self.backfill_id
            )
            self.backfill = detail
            self.title_label.set_text(detail.summary.title)
            self.summary_label.set_text(
                f"{detail.summary.status.replace('_', ' ').title()} · "
                f"created {detail.summary.created} · "
                f"{detail.summary.partitions or 0:,} partitions"
            )
            self.asset_counts_table.rows = [
                asdict(count) for count in detail.asset_counts
            ]
            self.asset_counts_table.update()
            self.loading_label.set_text(f"{len(detail.asset_counts):,} assets")
            self.content.set_visibility(True)
            self.dagster_button.set_enabled(True)
        except Exception:
            logger.exception("Could not load Dagster backfill %s", self.backfill_id)
            self.loading_label.set_text("Could not load backfill details.")
            ui.notify("Could not load backfill details", type="negative")

    async def _on_tab_change(self, event: object) -> None:
        value = getattr(event, "value", None)
        tab_name = getattr(value, "name", value)
        if tab_name == "runs":
            await self._load_runs_once()

    async def _load_runs_once(self) -> None:
        if self.runs_loaded or self.runs_loading:
            return
        self.runs_loading = True
        self.runs_loading_label.set_text("Loading detailed runs…")
        try:
            rows = await run.io_bound(
                self.data_source.load_backfill_runs, self.backfill_id
            )
            if rows is None:
                raise RuntimeError("Backfill run loader returned no result")
            self.runs_table.set_rows(rows)
            self.runs_loaded = True
            self.runs_loading_label.set_text(f"{len(rows):,} asset/partition/run rows")
        except Exception:
            logger.exception(
                "Could not load runs for Dagster backfill %s", self.backfill_id
            )
            self.runs_loading_label.set_text("Could not load detailed runs.")
            ui.notify("Could not load detailed runs", type="negative")
        finally:
            self.runs_loading = False

    def _build_counts_table(self) -> None:
        self.asset_counts_table = ui.table(
            columns=[
                {"name": "asset", "label": "Asset", "field": "asset"},
                {"name": "targeted", "label": "Targeted", "field": "targeted"},
                {
                    "name": "materialized",
                    "label": "Materialized",
                    "field": "materialized",
                },
                {"name": "failed", "label": "Failed", "field": "failed"},
                {
                    "name": "in_progress",
                    "label": "In progress",
                    "field": "in_progress",
                },
                {"name": "remaining", "label": "Remaining", "field": "remaining"},
            ],
            rows=[],
            row_key="asset",
            pagination={"rowsPerPage": 25},
        ).classes("w-full shadow-none border rounded-lg")
        self.asset_counts_table.props("flat bordered separator=cell")
        for column, classes in (
            ("materialized", "bg-green-50 text-green-700"),
            ("failed", "bg-red-50 text-red-600"),
            ("in_progress", "bg-violet-50 text-violet-700"),
            ("remaining", "bg-slate-50 text-slate-700"),
        ):
            self.asset_counts_table.add_slot(
                f"body-cell-{column}",
                f"""
                <q-td :props="props" class="{classes} text-center font-semibold">
                  {{{{ props.value }}}}
                </q-td>
                """,
            )

    def _build_runs_table(self) -> None:
        self.partition_status_summary = PartitionStatusSummary(
            self._set_visible_partition_statuses,
            self.visible_partition_statuses,
        ).build()
        with ui.row().classes("w-full justify-end"):
            ui.button(
                "Sorting",
                icon="sort",
                on_click=self._open_sorting_settings,
            ).props("outline no-caps")
        self.runs_table = BackfillRunsTable(
            on_settings_change=self._schedule_settings_save,
            on_rows_change=self.partition_status_summary.update,
            visible_statuses=self.visible_partition_statuses,
        ).build()
        self.runs_table.restore_settings(self.table_settings)
        self.visible_partition_statuses = self.runs_table.visible_statuses
        self.sorting_dialog = SortingDialog(
            [
                (str(column["name"]), str(column["label"]))
                for column in BackfillRunsTable.COLUMNS
            ],
            on_apply=self._apply_sorting_settings,
            on_cancel=lambda: None,
        ).build()

    def _set_visible_partition_statuses(self, statuses: set[str]) -> None:
        self.visible_partition_statuses = statuses
        self.runs_table.set_visible_statuses(statuses)

    def _open_sorting_settings(self) -> None:
        self.sorting_dialog.open(self.runs_table.sorting_rules)

    def _apply_sorting_settings(self, rules: list[SortRule]) -> None:
        self.runs_table.set_sorting(rules)
        self._schedule_settings_save()

    def _schedule_settings_save(self) -> None:
        ui.timer(0, self._save_settings, once=True)

    async def _save_settings(self) -> None:
        settings = BackfillTableSettingsState(
            column_filters={
                column: ColumnFilterSettings(
                    mode=cast(FilterMode, mode),
                    value=list(value) if isinstance(value, tuple) else value,
                )
                for column, (mode, value) in self.runs_table.column_filters.items()
            },
            sorting=[
                ColumnSortSettings(
                    column=rule.column,
                    descending=rule.descending,
                )
                for rule in self.runs_table.sorting_rules
            ],
            visible_statuses=[
                status
                for status in PARTITION_STATUSES
                if status in self.visible_partition_statuses
            ],
            known_statuses=list(PARTITION_STATUSES),
        )
        await run.io_bound(self.settings_store.save, settings)

    def _open_in_dagster(self) -> None:
        if self.backfill is not None:
            ui.navigate.to(self.backfill.summary.url, new_tab=True)


def _status_label(status: str) -> str:
    return status.replace("_", " ").title()


def _visible_partition_statuses(
    settings: BackfillTableSettingsState,
) -> set[str]:
    if not settings.visible_statuses:
        return set(PARTITION_STATUSES)
    visible = set(settings.visible_statuses) & set(PARTITION_STATUSES)
    known = set(settings.known_statuses) or {
        "materialized",
        "failed",
        "in_progress",
    }
    # Newly introduced statuses start visible once, without overriding a later
    # explicit choice to hide them.
    visible.update(set(PARTITION_STATUSES) - known)
    return visible
