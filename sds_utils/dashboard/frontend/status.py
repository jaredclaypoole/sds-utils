"""Status colors and summary cards shared by dashboard views."""

from collections.abc import Callable

import pandas as pd
from nicegui import ui

from .filters import StringFilterMenu
from .uielem import UIElem

STATUS_ORDER = (
    "materialized",
    "materializing",
    "failed",
    "skipped",
    "not-run",
    "not-found",
)

STATUS_CARD_CLASSES = {
    "materialized": "border-green-400 bg-green-50 text-green-900",
    "materializing": "border-violet-400 bg-violet-50 text-violet-900",
    "failed": "border-red-400 bg-red-50 text-red-900",
    "skipped": "border-amber-400 bg-amber-50 text-amber-900",
    "not-run": "border-slate-400 bg-slate-100 text-slate-900",
    "not-found": "border-blue-400 bg-blue-50 text-blue-900",
}

STATUS_BADGE_COLORS = {
    "materialized": "positive",
    "materializing": "info",
    "failed": "negative",
    "skipped": "warning",
    "not-run": "grey-7",
    "not-found": "deep-purple",
}

SNAPSHOT_STATUS_CLASSES = {
    "materialized": "text-green-700",
    "materializing": "text-violet-700",
    "failed": "text-red-600",
    "skipped": "text-yellow-600",
    "not-run": "text-black",
    "not-found": "text-blue-700",
}


class StatusFilterCard(UIElem):
    """Clickable status count card matching the established dashboard style."""

    BASE_CLASSES = (
        "min-w-36 px-4 py-3 shadow-none border-2 cursor-pointer "
        "select-none transition-all duration-150"
    )
    INACTIVE_CLASSES = "border-slate-200 bg-white text-slate-400 opacity-50"

    def __init__(
        self,
        status: str,
        on_toggle: Callable[[str, bool], None],
        *,
        active: bool = True,
    ) -> None:
        self.status = status
        self.on_toggle = on_toggle
        self.active = active

    def render(self) -> None:
        """Render the clickable card and its count."""
        self.card = ui.card().classes(replace=self._classes())
        self.card.props("role=button tabindex=0")
        self.card.tooltip(f"Toggle {self.status} partitions")
        self.card.on("click", self._toggle)
        with self.card:
            ui.label(self.status).classes("text-xs uppercase tracking-wide")
            self.count_label = ui.label("0 / 0").classes("text-2xl font-semibold")

    def set_count(self, shown: int, total: int) -> None:
        """Update the displayed shown and total counts."""
        shown_text = f"{shown:,}" if self.active else "-"
        self.count_label.set_text(f"{shown_text} / {total:,}")

    def set_active(self, active: bool) -> None:
        """Synchronize the card's active appearance without invoking callbacks."""
        self.active = active
        self.card.classes(replace=self._classes())

    def _toggle(self) -> None:
        self.set_active(not self.active)
        self.on_toggle(self.status, self.active)

    def _classes(self) -> str:
        state_classes = (
            STATUS_CARD_CLASSES[self.status] if self.active else self.INACTIVE_CLASSES
        )
        return f"{self.BASE_CLASSES} {state_classes}"


class StatusSummary(UIElem):
    """Row of status cards reflecting the table's current filters."""

    def __init__(
        self,
        status_filter: StringFilterMenu,
    ) -> None:
        self.status_filter = status_filter
        self.statuses = list(STATUS_ORDER)
        self.cards: dict[str, StatusFilterCard] = {}

    def render(self) -> None:
        """Render cards in the canonical status order."""
        self.container = ui.row().classes("w-full gap-3 flex-wrap")
        self._render_cards()

    def _render_cards(self) -> None:
        self.cards = {}
        with self.container:
            for status in self.statuses:
                self.cards[status] = StatusFilterCard(
                    status,
                    self.status_filter.set_value_selected,
                ).build()

    def update_query(self, source_df: pd.DataFrame) -> None:
        """Reset status choices for newly queried source data."""
        self._update_statuses(source_df)
        self.status_filter.update_values(self.statuses, reset=True)

    def update(
        self,
        source_df: pd.DataFrame,
        shown_df: pd.DataFrame,
    ) -> None:
        """Update counts and active state from source and displayed rows."""
        self._update_statuses(source_df)

        total_counts = source_df["status"].value_counts()
        shown_counts = shown_df["status"].value_counts()
        for status, card in self.cards.items():
            card.set_active(status in self.status_filter.selected)
            card.set_count(
                int(shown_counts.get(status, 0)),
                int(total_counts.get(status, 0)),
            )

    def _update_statuses(self, source_df: pd.DataFrame) -> None:
        unknown = sorted(set(source_df["status"].dropna()) - set(STATUS_ORDER))
        statuses = [*STATUS_ORDER, *unknown]
        if statuses != self.statuses:
            self.statuses = statuses
            self.container.clear()
            self._render_cards()
