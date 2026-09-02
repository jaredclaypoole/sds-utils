import datetime

import pandas as pd
from nicegui import ui

from .uielem import UIElem
from ..backend.filteredtable import FilteredTable, QuerySpec


class FilteredTableView(UIElem):
    def __init__(self, table: FilteredTable) -> None:
        self.table = table

    def render(self) -> None:
        query = QuerySpec(
            start_time=datetime.datetime(2026, 8, 1),
            end_time=datetime.datetime(2026, 8, 30) - datetime.timedelta(seconds=1),
        )
        self.table.set_query(query)
        example_kwargs = dict(
            instrument=dict(excluded_values_regex="codice|glows"),
            partition_label=dict(excluded_values_regex="daily")
        )
        data_df = self.table.transform_data(example_kwargs)

        self.table_elem = ui.table.from_pandas(
            data_df,
            pagination=25,
        ).classes("w-full")
