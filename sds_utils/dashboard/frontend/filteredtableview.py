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

        cols_to_drop = [
            "asset",
            "partition",
        ]
        data_df = data_df.drop(columns=cols_to_drop)

        self.table_elem = ui.table.from_pandas(
            data_df.reset_index(drop=True),
            pagination=25,
        ).classes("w-full")
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
        self.filter_menus: dict[str, StringFilterMenu] = {}
        column_labels = {
            column["name"]: column["label"] for column in self.table_elem.columns
        }
        for filter_ in self.table.filters:
            if not isinstance(filter_, StringRegisteredFilter):
                continue
            if filter_.name not in data_df.columns:
                continue

            values = data_df[filter_.name].dropna().astype(str).unique().tolist()
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

    def _apply_filters(self) -> None:
        filter_arguments: FilterArguments = {}
        for name, menu in self.filter_menus.items():
            arguments = menu.arguments()
            if arguments is not None:
                filter_arguments[name] = arguments

        data_df = self.table.transform_data(filter_arguments)
        rows = json.loads(data_df.to_json(orient="records", date_format="iso"))
        self.table_elem.update_rows(rows)
