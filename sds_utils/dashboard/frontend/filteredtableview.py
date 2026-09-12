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
        self.status_container = ui.column().classes("w-full")
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

        statuses = self.full_data_df["status"].dropna().astype(str).unique().tolist()
        self.status_container.clear()
        with self.status_container:
            self.status_summary = StatusSummary(
                statuses,
                self._on_status_card_toggle,
            ).build()

        self.filter_menus: dict[str, StringFilterMenu] = {}
        self.update_table()

    def update_table(self) -> None:
        selected_values = {
            name: set(menu.selected) for name, menu in self.filter_menus.items()
        }
        filter_arguments: FilterArguments = {}
        for name, menu in self.filter_menus.items():
            arguments = menu.arguments()
            if arguments is not None:
                filter_arguments[name] = arguments

        data_df = self.table.transform_data(filter_arguments)
        display_df = self._display_data(data_df)

        self.table_container.clear()
        self.filter_menus = {}
        with self.table_container:
            self._build_table(display_df, selected_values)
        self._update_status_summary(data_df)

    def _build_table(
        self,
        display_df: pd.DataFrame,
        selected_values: dict[str, set[str]],
    ) -> None:
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

            if filter_.name == "status":
                values = list(self.status_summary.statuses)
            else:
                values = sorted(
                    self.full_data_df[filter_.name]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()
                )
            menu = StringFilterMenu(filter_, values, self.update_table)
            if filter_.name in selected_values:
                menu.selected = selected_values[filter_.name].intersection(values)
            self.filter_menus[filter_.name] = menu
            with self.table_elem.add_slot(f"header-cell-{filter_.name}"):
                with self.table_elem.header(filter_.name):
                    with ui.button(
                        column_labels[filter_.name],
                        icon="filter_list",
                    ).props("flat dense no-caps"):
                        with ui.menu():
                            menu.build()
            if filter_.name in selected_values:
                menu._sync_checkboxes()

    def _on_status_card_toggle(self, status: str, active: bool) -> None:
        status_menu = self.filter_menus.get("status")
        if status_menu is not None:
            status_menu.set_value_selected(status, active)

    def _update_status_summary(self, shown_df: pd.DataFrame) -> None:
        status_menu = self.filter_menus.get("status")
        active_statuses = (
            set(status_menu.selected)
            if status_menu is not None
            else set(self.status_summary.statuses)
        )
        self.status_summary.update(
            self.full_data_df,
            shown_df,
            active_statuses,
        )

    @staticmethod
    def _display_data(data_df: pd.DataFrame) -> pd.DataFrame:
        return data_df.drop(columns=["asset", "partition"])
