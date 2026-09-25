"""Frontend controls for registered dashboard filters."""

import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any, Self

import pandas as pd
from nicegui import ui
from nicegui.elements.checkbox import Checkbox
from nicegui.elements.table import Table

from ..backend.filtersbase import (
    FilterBase,
    StrHierarchySpec,
    StringRegisteredFilter,
)
from .uielem import UIElem


@dataclass
class CheckboxNode:
    """A checkbox and the concrete values controlled by it."""

    label: str
    values: tuple[str, ...]
    children: list["CheckboxNode"] = field(default_factory=list)
    checkbox: Checkbox = field(init=False)


class StringFilterMenu(UIElem):
    """Render and track a hierarchical string-filter checkbox menu."""

    def __init__(
        self,
        filter_: StringRegisteredFilter,
        values: list[str],
        on_change: Callable[[], None],
    ) -> None:
        self.filter = filter_
        self.values = tuple(dict.fromkeys(values))
        self.all_values = list(self.values)
        self.selected = set(self.values)
        self.on_change = on_change
        self._updating = False
        self.hierarchy = filter_.hierarchy or StrHierarchySpec(hierarchy={})
        self.nodes: list[CheckboxNode] = []

    @classmethod
    def from_filter(
        cls,
        filter_: FilterBase,
        values: list[str],
        on_change: Callable[[], None],
    ) -> Self:
        """Construct the appropriate frontend menu for a registered filter."""
        if isinstance(filter_, StringRegisteredFilter):
            return cls(filter_, values, on_change)
        raise NotImplementedError(type(filter_).__name__)

    def update_values(
        self,
        values: list[str],
        *,
        select_new_values: bool,
    ) -> None:
        """Update visible values while retaining known selection states."""
        self.values = tuple(dict.fromkeys(values))
        known_values = set(self.all_values)
        new_values = [value for value in self.values if value not in known_values]
        self.all_values.extend(new_values)
        if select_new_values:
            self.selected.update(new_values)

    def update_query(
        self,
        data_df: pd.DataFrame,
        *,
        select_new_values: bool,
    ) -> None:
        """Update visible choices from this filter's column in queried data."""
        values = sorted(
            data_df[self.filter.name].dropna().astype(str).unique().tolist()
        )
        self.update_values(values, select_new_values=select_new_values)

    def render_header(
        self,
        table: Table,
        label: str,
        *,
        disabled: bool = False,
    ) -> None:
        """Render this filter as a dropdown in its table column header."""
        with table.add_slot(f"header-cell-{self.filter.name}"):
            with table.header(self.filter.name):
                props = (
                    "flat dense no-caps disable" if disabled else "flat dense no-caps"
                )
                with ui.button(label, icon="filter_list").props(props):
                    with ui.menu():
                        self.build()

    def render_dropdown(self, label: str, *, disabled: bool = False) -> None:
        """Render this filter as a standalone dropdown control."""
        props = "outline no-caps disable" if disabled else "outline no-caps"
        with ui.dropdown_button(label, icon="filter_list").props(props):
            self.build()

    def _build_nodes(self, spec: StrHierarchySpec) -> list[CheckboxNode]:
        hierarchy = spec.build_hierarchy(self.values)
        if not hierarchy:
            nodes = [CheckboxNode(value, (value,)) for value in self.values]
        else:
            assigned: set[str] = set()
            nodes = []
            for parent, configured_values in hierarchy.items():
                children = [
                    CheckboxNode(value, (value,))
                    for value in configured_values
                    if value in self.values and value not in assigned
                ]
                assigned.update(value for child in children for value in child.values)
                if children:
                    nodes.append(
                        CheckboxNode(
                            parent,
                            tuple(
                                value for child in children for value in child.values
                            ),
                            children,
                        )
                    )

            unmatched = [value for value in self.values if value not in assigned]
            if unmatched and spec.other is not None:
                children = [CheckboxNode(value, (value,)) for value in unmatched]
                nodes.append(CheckboxNode(spec.other, tuple(unmatched), children))

        if spec.all is None:
            return nodes
        return [CheckboxNode(spec.all, self.values, nodes)]

    def render(self) -> None:
        """Create the checkbox hierarchy inside the current menu slot."""
        self.nodes = self._build_nodes(self.hierarchy)
        with ui.column().classes("gap-1 p-3 min-w-52"):
            for node in self.nodes:
                self._render_node(node, depth=0)
        self._sync_checkboxes()

    def _render_node(self, node: CheckboxNode, depth: int) -> None:
        node.checkbox = (
            ui.checkbox(
                node.label,
                value=True,
                on_change=lambda event, current=node: self._toggle_node(
                    current, bool(event.value)
                ),
            )
            .props("dense")
            .classes("py-0.5")
        )
        if depth:
            node.checkbox.style(f"margin-left: {depth * 1.5}rem")
        for child in node.children:
            self._render_node(child, depth + 1)

    def _toggle_node(self, node: CheckboxNode, checked: bool) -> None:
        if self._updating:
            return
        if checked:
            self.selected.update(node.values)
        else:
            self.selected.difference_update(node.values)
        self._sync_checkboxes()
        self.on_change()

    def _sync_checkboxes(self) -> None:
        self._updating = True
        try:
            for node in self._walk_nodes(self.nodes):
                selected_count = len(self.selected.intersection(node.values))
                if selected_count == len(node.values):
                    node.checkbox.value = True
                elif selected_count == 0:
                    node.checkbox.value = False
                else:
                    node.checkbox.value = None
        finally:
            self._updating = False

    def arguments(self) -> dict[str, Any] | None:
        """Return backend arguments, or None when every value is selected."""
        excluded = set(self.values) - self.selected
        if not excluded:
            return None
        return {
            "excluded_values_regex": "|".join(
                re.escape(value) for value in sorted(excluded)
            )
        }

    def set_arguments(self, arguments: dict[str, Any]) -> None:
        """Restore selections represented by backend string-filter arguments."""
        included = arguments.get("included_values_regex")
        excluded = arguments.get("excluded_values_regex")
        if included is not None and not isinstance(included, str):
            return
        if excluded is not None and not isinstance(excluded, str):
            return

        selected = set(self.all_values)
        try:
            if included is not None:
                selected = {
                    value for value in selected if re.fullmatch(included, value)
                }
            if excluded is not None:
                selected = {
                    value for value in selected if not re.fullmatch(excluded, value)
                }
        except re.error:
            return
        self.selected = selected
        self._sync_checkboxes()

    def set_value_selected(self, value: str, selected: bool) -> None:
        """Select or clear one concrete value and notify the table view."""
        if value not in self.all_values:
            return
        if selected:
            self.selected.add(value)
        else:
            self.selected.discard(value)
        self._sync_checkboxes()
        self.on_change()

    @classmethod
    def _walk_nodes(cls, nodes: list[CheckboxNode]) -> Iterator[CheckboxNode]:
        for node in nodes:
            yield node
            yield from cls._walk_nodes(node.children)
