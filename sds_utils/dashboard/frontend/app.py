from nicegui import ui

from ..backend.db import engine
from ..backend.dbdata import DBDataSource
from ..backend.filteredtable import FilteredTable
from .filteredtableview import FilteredTableView
from .uielem import UIElem


class TableApp(UIElem):
    def render(self) -> None:
        data_source = DBDataSource(
            engine=engine,
            dagster_namespace="prod",
        )
        table = FilteredTable(data_source)
        self.table_view = FilteredTableView(table).build()


def render() -> None:
    TableApp().build()


def main() -> None:
    ui.run(
        root=render,
        title="IMAP Processing Status Dashboard",
        favicon="📈",
        reload=False,
        port=8893,
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
