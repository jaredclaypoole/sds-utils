import datetime
import json

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


class FilteredTableView(UIElem):
    def __init__(self, table: FilteredTable) -> None:
        self.table = table

    def render(self) -> None:
        query = QuerySpec(
            start_time=datetime.datetime(2026, 8, 1),
            end_time=datetime.datetime(2026, 8, 30) - datetime.timedelta(seconds=1),
        )
        self.table.set_query(query)
        data_df = self.table.transform_data()
        self.source_data_df = data_df

        statuses = data_df["status"].dropna().astype(str).unique().tolist()
        self.status_summary = StatusSummary(
            statuses,
            self._on_status_card_toggle,
        ).build()

        display_df = self._display_data(data_df)

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
        self.filter_menus: dict[str, StringFilterMenu] = {}
        column_labels = {
            column["name"]: column["label"] for column in self.table_elem.columns
        }
        for filter_ in self.table.filters:
            if not isinstance(filter_, StringRegisteredFilter):
                continue
            if filter_.name not in display_df.columns:
                continue

            if filter_.name == "status":
                values = list(self.status_summary.statuses)
            else:
                values = sorted(
                    display_df[filter_.name]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()
                )
            menu = StringFilterMenu(filter_, values, self._apply_filters)
            self.filter_menus[filter_.name] = menu
            with self.table_elem.add_slot(f"header-cell-{filter_.name}"):
                with self.table_elem.header(filter_.name):
                    with ui.button(
                        column_labels[filter_.name],
                        icon="filter_list",
                    ).props("flat dense no-caps"):
                        with ui.menu():
                            menu.build()
        self._update_status_summary(data_df)

    def _apply_filters(self) -> None:
        filter_arguments: FilterArguments = {}
        for name, menu in self.filter_menus.items():
            arguments = menu.arguments()
            if arguments is not None:
                filter_arguments[name] = arguments

        data_df = self.table.transform_data(filter_arguments)
        rows = json.loads(
            self._display_data(data_df).to_json(
                orient="records",
                date_format="iso",
            )
        )
        self.table_elem.update_rows(rows)
        self._update_status_summary(data_df)

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
            self.source_data_df,
            shown_df,
            active_statuses,
        )

    @staticmethod
    def _display_data(data_df: pd.DataFrame) -> pd.DataFrame:
        return data_df.drop(columns=["asset", "partition"])
