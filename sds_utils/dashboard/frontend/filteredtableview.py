import datetime
import json
from abc import abstractmethod
from typing import Any

import pandas as pd
from nicegui import ui
from nicegui.events import ValueChangeEventArguments

from ..backend.aggbase import AggPreset, AggSpec
from ..backend.data import QuerySpec
from ..backend.filteredtable import FilteredTable
from ..backend.filtersbase import (
    FilterArguments,
    StringRegisteredFilter,
)
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


class FilteredTableViewBase(UIElem):
    @abstractmethod
    def update_query(self, query: QuerySpec) -> None:
        """Update the table query and ultimately the transforms as well."""

    @abstractmethod
    def update_table(self) -> None:
        """Update table transforms but not the query."""


class FilteredTableView(FilteredTableViewBase):
    def __init__(self, table: FilteredTable, *, dagster_url: str) -> None:
        self.table = table
        self.dagster_url = dagster_url.rstrip("/")
        self.agg_preset = AggPreset.RAW

    def render(self) -> None:
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
            value=self.agg_preset.value,
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
        self.update_table()

    def update_table(self) -> None:
        filter_arguments: FilterArguments = {}
        for name, menu in self.filter_menus.items():
            arguments = menu.arguments()
            if arguments is not None:
                filter_arguments[name] = arguments

        filtered_df = self.table.transform_data(filter_arguments)
        data_df = self.table.transform_data(
            filter_arguments,
            AggSpec(preset=self.agg_preset),
        )
        display_df = self._display_data(data_df)

        self.standalone_filters_container.clear()
        with self.standalone_filters_container:
            for name, menu in self.filter_menus.items():
                if name not in display_df.columns:
                    menu.render_dropdown(name.replace("_", " ").title())

        self.table_container.clear()
        with self.table_container:
            self._build_table(display_df)
        self.status_summary.update(self.full_data_df, filtered_df)

    def _on_agg_preset_change(
        self,
        event: ValueChangeEventArguments[Any],
    ) -> None:
        self.agg_preset = AggPreset(event.value)
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
        column_labels = {
            column["name"]: column["label"] for column in self.table_elem.columns
        }
        for filter_ in self.table.filters:
            if not isinstance(filter_, StringRegisteredFilter):
                raise ValueError(f"Unrecognized filter type: {type(filter_)}")
            if filter_.name not in display_df.columns:
                continue

            menu = self.filter_menus[filter_.name]
            menu.render_header(self.table_elem, column_labels[filter_.name])

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
        display_df = data_df.drop(
            columns=cols_to_hide, errors="ignore"
        )
        if any(name is not None for name in display_df.index.names):
            display_df = display_df.reset_index()
        for column in ("start_date", "end_date"):
            if column in display_df.columns:
                display_df[column] = display_df[column].dt.strftime("%Y-%m-%d")
        return display_df
