import datetime
import json
from abc import abstractmethod

import pandas as pd
from nicegui import ui

from ..backend.data import QuerySpec
from ..backend.filteredtable import FilteredTable
from ..backend.filtersbase import (
    FilterArguments,
    StringRegisteredFilter,
)
from .filters import StringFilterMenu
from .status import STATUS_BADGE_COLORS, StatusSummary
from .uielem import UIElem


class FilteredTableViewBase(UIElem):
    @abstractmethod
    def update_query(self, query: QuerySpec) -> None:
        """Update the table query and ultimately the transforms as well."""

    @abstractmethod
    def update_table(self) -> None:
        """Update table transforms but not the query."""


class FilteredTableView(FilteredTableViewBase):
    def __init__(self, table: FilteredTable) -> None:
        self.table = table

    def render(self) -> None:
        self.filter_menus = {
            filter_.name: StringFilterMenu.from_filter(filter_, [], self.update_table)
            for filter_ in self.table.filters
        }
        self.status_summary = StatusSummary(self.filter_menus["status"]).build()
        self.table_container = ui.column().classes("w-full")
        query = QuerySpec(
            start_time=datetime.datetime(2026, 8, 1),
            end_time=datetime.datetime(2026, 8, 30) - datetime.timedelta(seconds=1),
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

        data_df = self.table.transform_data(filter_arguments)
        display_df = self._display_data(data_df)

        self.table_container.clear()
        with self.table_container:
            self._build_table(display_df)
        self.status_summary.update(self.full_data_df, data_df)

    def _build_table(self, display_df: pd.DataFrame) -> None:
        self.table_elem = ui.table.from_pandas(
            display_df.reset_index(drop=True),
            pagination=25,
        ).classes("w-full shadow-none border rounded-lg")
        self.table_elem.props("flat bordered separator=horizontal wrap-cells")
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
        return data_df.drop(columns=["asset", "partition"])
