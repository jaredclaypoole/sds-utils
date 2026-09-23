"""NiceGUI entry point for the processing-status dashboard."""

import os

from nicegui import ui

from ..backend.db import engine
from ..backend.dbdata import DBDataSource
from ..backend.filteredtable import FilteredTable
from .filteredtableview import FilteredTableView
from .uielem import UIElem


class TableApp(UIElem):
    """Construct the database-backed dashboard application."""

    def render(self) -> None:
        """Build the dashboard's data source and table view."""
        data_source = DBDataSource(
            engine=engine,
            dagster_namespace="prod",
        )
        table = FilteredTable(data_source)
        self.table_view = FilteredTableView(
            table,
            dagster_url=os.environ["DAGSTER_BASE_URL"],
        ).build()


def render() -> None:
    """Render the root dashboard component."""
    TableApp().build()


def main() -> None:
    """Start the NiceGUI dashboard server."""
    ui.run(
        root=render,
        title="IMAP Processing Status Dashboard",
        favicon="📈",
        reload=False,
        port=8893,
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
