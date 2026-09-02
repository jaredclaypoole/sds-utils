"""Frontend controls for registered dashboard filters."""

import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from nicegui import ui

from ..backend.filtersbase import StrHierarchySpec, StringRegisteredFilter
from .uielem import UIElem


@dataclass
class CheckboxNode:
    """A checkbox and the concrete values controlled by it."""

    label: str
    values: tuple[str, ...]
    children: list["CheckboxNode"] = field(default_factory=list)
    checkbox: Any = None


class StringFilterMenu(UIElem):
    """Render and track a hierarchical string-filter checkbox menu."""

    def __init__(
        self,
        filter_: StringRegisteredFilter,
        values: list[str],
        on_change: Any,
    ) -> None:
        self.filter = filter_
        self.values = tuple(sorted(set(values)))
        self.selected = set(self.values)
        self.on_change = on_change
        self._updating = False
        hierarchy = filter_.hierarchy or StrHierarchySpec(hierarchy={})
        self.nodes = self._build_nodes(hierarchy)

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
                            tuple(value for child in children for value in child.values),
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
        with ui.column().classes("gap-0 p-2 min-w-48"):
            for node in self.nodes:
                self._render_node(node, depth=0)

    def _render_node(self, node: CheckboxNode, depth: int) -> None:
        node.checkbox = ui.checkbox(
            node.label,
            value=True,
            on_change=lambda event, current=node: self._toggle_node(
                current, bool(event.value)
            ),
        ).props("dense")
        if depth:
            node.checkbox.style(f"margin-left: {depth * 1.25}rem")
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

    @classmethod
    def _walk_nodes(
        cls, nodes: list[CheckboxNode]
    ) -> Iterator[CheckboxNode]:
        for node in nodes:
            yield node
            yield from cls._walk_nodes(node.children)
