"""NiceGUI rendering for filtered and aggregated dashboard tables."""

import datetime
import json
import re
from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd
from nicegui import ui
from nicegui.events import ValueChangeEventArguments

from ..backend.aggbase import AggPreset, AggSpec
from ..backend.data import QuerySpec
from ..backend.filteredtable import FilteredTable, SortSpec
from ..backend.filtersbase import (
    FilterArguments,
    StringRegisteredFilter,
)
from ..backend.settings import DashboardSettings
from .filters import StringFilterMenu
from .status import (
    SNAPSHOT_TOTAL_CLASS,
    SNAPSHOT_ZERO_CLASS,
    STATUS_BADGE_COLORS,
    StatusSummary,
    format_status_counts,
    status_count_columns,
)
from .uielem import UIElem


@dataclass(frozen=True)
class _PaneState:
    agg_spec: AggSpec
    temporary_filters: dict[str, str]


class FilteredTableViewBase(UIElem):
    """Interface for queryable dashboard table views."""

    @abstractmethod
    def update_query(self, query: QuerySpec) -> None:
        """Update the table query and ultimately the transforms as well."""

    @abstractmethod
    def update_table(self) -> None:
        """Update table transforms but not the query."""


class FilteredTableView(FilteredTableViewBase):
    """Render a filtered table with summaries and aggregation controls."""

    def __init__(
        self,
        table: FilteredTable,
        *,
        dagster_url: str,
        settings: DashboardSettings,
        sync_settings: Callable[[DashboardSettings], None],
    ) -> None:
        self.table = table
        self.dagster_url = dagster_url.rstrip("/")
        self.agg_spec = settings.agg
        self.temporary_filters: dict[str, str] = {}
        self._pane_history: list[_PaneState] = []
        self._setting_summary_view = False
        self._initial_filtering: FilterArguments | None = settings.filtering
        self._sync_settings = sync_settings

    def render(self) -> None:
        """Create table controls, filters, summaries, and the initial table."""
        self.filter_menus = {
            filter_.name: StringFilterMenu.from_filter(filter_, [], self.update_table)
            for filter_ in self.table.filters
        }
        preset_options = {
            preset.value: preset.name.replace("_", " ").capitalize()
            for preset in AggPreset
        }
        self.summary_view = ui.select(
            preset_options,
            value=self.agg_spec.preset.value,
            label="Summary view",
            on_change=self._on_agg_preset_change,
        ).classes("min-w-56")
        self.status_summary = StatusSummary(self.filter_menus["status"]).build()
        self.standalone_filters_container = ui.row().classes(
            "w-full items-center gap-3 flex-wrap"
        )
        self.table_container = ui.column().classes("w-full")
        query = QuerySpec(
            start_time=datetime.datetime(2026, 8, 1),
            end_time=datetime.datetime(2026, 9, 30) - datetime.timedelta(seconds=1),
            # date_mode="partition",
            version_mode="latest",
        )
        self.update_query(query)

    def update_query(self, query: QuerySpec) -> None:
        """Reload backend data and update all query-dependent controls."""
        self.table.set_query(query)
        self.table.refresh_data()
        self.full_data_df = self.table.transform_data()

        for name, menu in self.filter_menus.items():
            if name == "status":
                continue
            menu.update_query(self.full_data_df, select_new_values=True)
        self.status_summary.update_query(
            self.full_data_df,
            select_new_values=True,
        )
        if self._initial_filtering is not None:
            for name, arguments in self._initial_filtering.items():
                restored_menu = self.filter_menus.get(name)
                if restored_menu is not None:
                    restored_menu.set_arguments(arguments)
            self._initial_filtering = None
        self.update_table()

    def update_table(self) -> None:
        """Rebuild the displayed table using current filters and aggregation."""
        filter_arguments: FilterArguments = {}
        for name, menu in self.filter_menus.items():
            arguments = menu.arguments()
            if arguments is not None:
                filter_arguments[name] = arguments
        effective_arguments = self._with_temporary_filters(filter_arguments)

        sort_specs = dict(
            instrument=SortSpec(),
            data_level=SortSpec(),
            descriptor=SortSpec(),
            start_time=SortSpec(),
        )

        filtered_df = self.table.transform_data(effective_arguments)
        data_df = self.table.transform_data(
            filter_kwargs=effective_arguments,
            agg_spec=self.agg_spec,
            sort_specs=sort_specs,
        )
        display_df = self._display_data(data_df)

        self.standalone_filters_container.clear()
        with self.standalone_filters_container:
            self._render_navigation()
            for name, menu in self.filter_menus.items():
                if name not in display_df.columns:
                    menu.render_dropdown(
                        name.replace("_", " ").title(),
                        disabled=name in self.temporary_filters,
                    )

        self.table_container.clear()
        with self.table_container:
            self._build_table(display_df)
        self.status_summary.update(self.full_data_df, filtered_df)
        self._sync_settings(
            DashboardSettings(
                agg=self.agg_spec,
                filtering=filter_arguments,
            )
        )

    def _on_agg_preset_change(
        self,
        event: ValueChangeEventArguments[Any],
    ) -> None:
        if self._setting_summary_view:
            return
        self.agg_spec = AggSpec(preset=AggPreset(event.value))
        self.temporary_filters.clear()
        self._pane_history.clear()
        self.update_table()

    def _with_temporary_filters(
        self,
        filter_arguments: FilterArguments,
    ) -> FilterArguments:
        effective = {
            name: arguments.copy() for name, arguments in filter_arguments.items()
        }
        for name, value in self.temporary_filters.items():
            effective.setdefault(name, {})["included_values_regex"] = re.escape(value)
        return effective

    def _render_navigation(self) -> None:
        if self._pane_history:
            ui.button("Back", icon="arrow_back", on_click=self._go_back).props(
                "flat no-caps"
            )
        for name, value in self.temporary_filters.items():
            label = name.replace("_", " ").title()
            ui.chip(
                f"{label}: {value}",
                removable=True,
                on_value_change=lambda event, filter_name=name: (
                    self._remove_temporary_filter(filter_name)
                    if not event.value
                    else None
                ),
            ).props("outline")

    def _drill_down(
        self,
        *,
        filter_name: str,
        value: str,
        next_preset: AggPreset,
    ) -> None:
        self._pane_history.append(
            _PaneState(
                agg_spec=self.agg_spec.model_copy(deep=True),
                temporary_filters=self.temporary_filters.copy(),
            )
        )
        self.temporary_filters[filter_name] = value
        self.agg_spec = AggSpec(preset=next_preset)
        self._set_summary_view(next_preset)
        self.update_table()

    def _go_back(self) -> None:
        if not self._pane_history:
            return
        state = self._pane_history.pop()
        self.agg_spec = state.agg_spec
        self.temporary_filters = state.temporary_filters
        self._set_summary_view(self.agg_spec.preset)
        self.update_table()

    def _set_summary_view(self, preset: AggPreset) -> None:
        self._setting_summary_view = True
        try:
            self.summary_view.value = preset.value
        finally:
            self._setting_summary_view = False

    def _remove_temporary_filter(self, name: str) -> None:
        self.temporary_filters.pop(name, None)
        self.update_table()

    def _build_table(self, display_df: pd.DataFrame) -> None:
        status_columns = status_count_columns(display_df)
        table_df = display_df.copy()
        for column in status_columns:
            table_df[column] = format_status_counts(table_df[column])

        self.table_elem = ui.table.from_pandas(
            table_df.reset_index(drop=True),
            pagination=25,
        ).classes("w-full shadow-none border rounded-lg")
        for column in status_columns:
            for row, value in zip(
                self.table_elem.rows,
                table_df[column],
                strict=True,
            ):
                row[column] = value
        self.table_elem.props("flat bordered separator=horizontal wrap-cells")
        if "run_id" in table_df.columns:
            dagster_url = json.dumps(self.dagster_url)
            self.table_elem.add_slot(
                "body-cell-run_id",
                f"""
                <q-td :props="props">
                    <a
                        :href='{dagster_url} + "/runs/" + props.value'
                        class="text-blue-8"
                        target="_blank"
                        rel="noopener noreferrer"
                    >{{{{ props.value.slice(0, 8) }}}}</a>
                </q-td>
                """,
            )
        self.table_elem.add_slot(
            "body-cell-partition_link",
            """
            <q-td :props="props">
                <a
                    :href="props.value"
                    class="text-blue-8"
                    target="_blank"
                    rel="noopener noreferrer"
                >link</a>
            </q-td>
            """,
        )
        badge_colors = json.dumps(STATUS_BADGE_COLORS)
        self.table_elem.add_slot(
            "body-cell-status",
            f"""
            <q-td :props="props">
                <q-badge
                    :color='({badge_colors})[props.value]'
                    :label="props.value"
                    outline
                />
            </q-td>
            """,
        )
        for column in status_columns:
            self.table_elem.add_slot(
                f"body-cell-{column}",
                f"""
                <q-td :props="props">
                    <template v-if="Array.isArray(props.value)">
                        <span :class="props.value[0].class">
                            {{{{ props.value[0].text }}}}
                        </span><span class="{SNAPSHOT_TOTAL_CLASS}">: </span>
                        <template
                            v-for="(part, index) in props.value.slice(1)"
                            :key="index"
                        >
                            <span
                                v-if="index"
                                class="{SNAPSHOT_ZERO_CLASS}"
                            > / </span>
                            <span :class="part.class">{{{{ part.text }}}}</span>
                        </template>
                    </template>
                </q-td>
                """,
            )
        self._render_drill_down_headers(status_columns)
        column_labels = {
            column["name"]: column["label"] for column in self.table_elem.columns
        }
        for filter_ in self.table.filters:
            if not isinstance(filter_, StringRegisteredFilter):
                raise ValueError(f"Unrecognized filter type: {type(filter_)}")
            if filter_.name not in display_df.columns:
                continue

            menu = self.filter_menus[filter_.name]
            menu.render_header(
                self.table_elem,
                column_labels[filter_.name],
                disabled=filter_.name in self.temporary_filters,
            )

    def _render_drill_down_headers(self, columns: list[str]) -> None:
        match self.agg_spec.preset:
            case AggPreset.INSTRUMENTS_SNAPSHOT:
                filter_name = "instrument"
                next_preset = AggPreset.DATA_LEVELS_SNAPSHOT
            case AggPreset.DATA_LEVELS_SNAPSHOT:
                filter_name = "data_level"
                next_preset = AggPreset.DATES_SUMMARY
            case _:
                return

        for column in columns:
            with self.table_elem.add_slot(f"header-cell-{column}"):
                with self.table_elem.header(column):
                    ui.button(
                        column,
                        on_click=lambda _event, value=column: self._drill_down(
                            filter_name=filter_name,
                            value=value,
                            next_preset=next_preset,
                        ),
                    ).props("flat dense no-caps").classes("w-full")

    @staticmethod
    def _display_data(data_df: pd.DataFrame) -> pd.DataFrame:
        cols_to_hide = [
            "asset",
            "partition",
            "partition_prefix",
            "job_name",
            "job_key",
            "tags",
            "selected_assets",
            "parent_run_id",
            "root_run_id",
        ]
        display_df = data_df.drop(columns=cols_to_hide, errors="ignore")
        if any(name is not None for name in display_df.index.names):
            display_df = display_df.reset_index()
        for column in ("start_date", "end_date"):
            if column in display_df.columns:
                display_df[column] = display_df[column].dt.strftime("%Y-%m-%d")
        return display_df
