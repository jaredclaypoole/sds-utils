"""Canonical dashboard statuses and their aggregate counts."""

from __future__ import annotations

from typing import NamedTuple

STATUS_ORDER = (
    "materialized",
    "materializing",
    "canceling",
    "canceled",
    "failed",
    "skipped",
    "missing",
    "not-run",
    "unknown",
    "not-found",
)


class StatusCounts(NamedTuple):
    """Counts for every dashboard status in canonical display order."""

    materialized: int = 0
    materializing: int = 0
    canceling: int = 0
    canceled: int = 0
    failed: int = 0
    skipped: int = 0
    missing: int = 0
    not_run: int = 0
    unknown: int = 0
    not_found: int = 0

    def __add__(self, other: object) -> StatusCounts:
        """Add corresponding status counts."""
        if not isinstance(other, StatusCounts):
            return NotImplemented
        sum_tup = tuple(a + b for a, b in zip(self, other, strict=True))
        return type(self)(*sum_tup)
