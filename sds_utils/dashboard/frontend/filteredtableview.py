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
        data = self.table.data()
        self.table_view = AutoTableView(data).build()


class AutoTableView(UIElem):
    def __init__(self, data_df: pd.DataFrame) -> None:
        self.data_df = data_df

    def render(self) -> None:
        self.table = ui.table.from_pandas(
            self.data_df,
            pagination=25,
        ).classes("w-full")
