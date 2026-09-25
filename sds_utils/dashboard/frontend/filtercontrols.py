"""Coordination for the dashboard's registered frontend filters."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import partial

import pandas as pd
from nicegui.elements.table import Table

from ..backend.filtersbase import (
    FilterArguments,
    FilterBase,
    StringRegisteredFilter,
)
from .filters import StringFilterMenu


@dataclass(frozen=True)
class GroupingControls:
    """Describe optional grouping controls attached to filter headers."""

    columns: set[str]
    enabled: set[str]
    on_toggle: Callable[[str], None]


class FilterControls:
    """Own filter menus and translate them to backend arguments."""

    def __init__(
        self,
        filters: Iterable[FilterBase],
        *,
        initial_arguments: FilterArguments,
        on_change: Callable[[], None],
    ) -> None:
        self.menus = {
            filter_.name: StringFilterMenu.from_filter(filter_, [], on_change)
            for filter_ in filters
        }
        self._initial_arguments: FilterArguments | None = initial_arguments

    def update_query(self, data_df: pd.DataFrame) -> None:
        """Refresh non-status choices from newly queried data."""
        for name, menu in self.menus.items():
            if name != "status":
                menu.update_query(data_df, select_new_values=True)

    def restore_initial_arguments(self) -> None:
        """Restore persisted selections once initial choices are available."""
        if self._initial_arguments is None:
            return
        for name, arguments in self._initial_arguments.items():
            menu = self.menus.get(name)
            if menu is not None:
                menu.set_arguments(arguments)
        self._initial_arguments = None

    def arguments(self) -> FilterArguments:
        """Return effective persistent arguments from all filter menus."""
        result: FilterArguments = {}
        for name, menu in self.menus.items():
            arguments = menu.arguments()
            if arguments is not None:
                result[name] = arguments
        return result

    def render_standalone(
        self,
        displayed_columns: Iterable[str],
        *,
        disabled: set[str],
    ) -> None:
        """Render filters whose fields are absent from the displayed table."""
        displayed = set(displayed_columns)
        for name, menu in self.menus.items():
            if name not in displayed:
                menu.render_dropdown(
                    name.replace("_", " ").title(),
                    disabled=name in disabled,
                )

    def render_headers(
        self,
        table: Table,
        displayed_columns: Iterable[str],
        *,
        disabled: set[str],
        grouping: GroupingControls | None = None,
    ) -> None:
        """Render filters integrated into matching table column headers."""
        displayed = set(displayed_columns)
        labels = {column["name"]: column["label"] for column in table.columns}
        for name, menu in self.menus.items():
            if name not in displayed:
                continue
            if not isinstance(menu.filter, StringRegisteredFilter):
                raise ValueError(f"Unrecognized filter type: {type(menu.filter)}")
            is_grouping_column = grouping is not None and name in grouping.columns
            toggle_grouping = (
                partial(grouping.on_toggle, name) if grouping is not None else None
            )
            menu.render_header(
                table,
                labels[name],
                disabled=name in disabled,
                grouping_enabled=(name in grouping.enabled)
                if is_grouping_column and grouping is not None
                else None,
                on_toggle_grouping=toggle_grouping if is_grouping_column else None,
            )
