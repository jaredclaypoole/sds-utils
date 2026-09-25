"""Aggregation-pane navigation and temporary drill-down filters."""

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from nicegui import ui
from nicegui.events import ValueChangeEventArguments

from ..backend.aggbase import AggPreset, AggSpec
from ..backend.filtersbase import FilterArguments
from .uielem import UIElem

_DRILLDOWNS = {
    AggPreset.INSTRUMENTS_SNAPSHOT: (
        "instrument",
        AggPreset.DATA_LEVELS_SNAPSHOT,
    ),
    AggPreset.DATA_LEVELS_SNAPSHOT: (
        "data_level",
        AggPreset.DATES_SUMMARY,
    ),
}


@dataclass(frozen=True)
class PaneState:
    """Restorable aggregation and temporary-filter state."""

    agg_spec: AggSpec
    temporary_filters: dict[str, str]


class PaneNavigator(UIElem):
    """Manage summary panes, drill-down history, and temporary filters."""

    def __init__(self, agg_spec: AggSpec, on_change: Callable[[], None]) -> None:
        self.agg_spec = agg_spec
        self.temporary_filters: dict[str, str] = {}
        self._history: list[PaneState] = []
        self._on_change = on_change
        self._setting_select = False

    @property
    def disabled_filters(self) -> set[str]:
        """Return filters temporarily overridden by drill-down state."""
        return set(self.temporary_filters)

    @property
    def drilldown_filter(self) -> str | None:
        """Return the field represented by clickable columns in this pane."""
        drilldown = _DRILLDOWNS.get(self.agg_spec.preset)
        return drilldown[0] if drilldown is not None else None

    def render(self) -> None:
        """Render the summary selector and navigation controls."""
        preset_options = {
            preset.value: preset.name.replace("_", " ").capitalize()
            for preset in AggPreset
        }
        self.summary_view = ui.select(
            preset_options,
            value=self.agg_spec.preset.value,
            label="Summary view",
            on_change=self._select_preset,
        ).classes("min-w-56")
        self.navigation_container = ui.row().classes("w-full items-center gap-2")
        self._render_navigation()

    def apply_temporary_filters(
        self,
        filter_arguments: FilterArguments,
    ) -> FilterArguments:
        """Compose temporary exact-value filters over persistent arguments."""
        effective = {
            name: arguments.copy() for name, arguments in filter_arguments.items()
        }
        for name, value in self.temporary_filters.items():
            effective.setdefault(name, {})["included_values_regex"] = re.escape(value)
        return effective

    def drill_down(self, value: str) -> None:
        """Move to the configured child pane for a clicked column value."""
        drilldown = _DRILLDOWNS.get(self.agg_spec.preset)
        if drilldown is None:
            return
        filter_name, next_preset = drilldown
        self._history.append(
            PaneState(
                agg_spec=self.agg_spec.model_copy(deep=True),
                temporary_filters=self.temporary_filters.copy(),
            )
        )
        self.temporary_filters[filter_name] = value
        self.agg_spec = AggSpec(preset=next_preset)
        self._set_select(next_preset)
        self._state_changed()

    def _select_preset(self, event: ValueChangeEventArguments[Any]) -> None:
        if self._setting_select:
            return
        self.agg_spec = AggSpec(preset=AggPreset(event.value))
        self.temporary_filters.clear()
        self._history.clear()
        self._state_changed()

    def _go_back(self) -> None:
        if not self._history:
            return
        state = self._history.pop()
        self.agg_spec = state.agg_spec
        self.temporary_filters = state.temporary_filters
        self._set_select(self.agg_spec.preset)
        self._state_changed()

    def _remove_filter(self, name: str) -> None:
        self.temporary_filters.pop(name, None)
        self._state_changed()

    def _set_select(self, preset: AggPreset) -> None:
        self._setting_select = True
        try:
            self.summary_view.value = preset.value
        finally:
            self._setting_select = False

    def _state_changed(self) -> None:
        self._render_navigation()
        self._on_change()

    def _render_navigation(self) -> None:
        self.navigation_container.clear()
        with self.navigation_container:
            if self._history:
                ui.button("Back", icon="arrow_back", on_click=self._go_back).props(
                    "flat no-caps"
                )
            for name, value in self.temporary_filters.items():
                label = name.replace("_", " ").title()
                ui.chip(
                    f"{label}: {value}",
                    removable=True,
                    on_value_change=lambda event, filter_name=name: (
                        self._remove_filter(filter_name) if not event.value else None
                    ),
                ).props("outline")
