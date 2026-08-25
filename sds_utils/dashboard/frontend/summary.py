"""Asset-name parsing and status aggregation for the summary view."""

from collections import Counter
from collections.abc import Iterable
from typing import TypedDict

from ..backend.data import AssetStatusRow, parse_asset_name
from ..backend.partition_time import partition_timestamp_range

SUMMARY_DIMENSIONS = ("instrument", "data_level", "descriptor", "missing_file")
SUMMARY_STATUSES = (
    "materialized",
    "materializing",
    "failed",
    "skipped",
    "not-run",
    "not-found",
)


class SummaryRow(TypedDict):
    row_id: str
    instrument: str | None
    data_level: str | None
    descriptor: str
    missing_file: str
    first_date: str
    last_date: str
    materialized: int
    materializing: int
    failed: int
    skipped: int
    not_run: int
    not_found: int


def summarize_status_rows(
    rows: Iterable[AssetStatusRow],
    enabled_dimensions: set[str],
) -> list[SummaryRow]:
    """Count statuses for each enabled asset-name dimension."""
    grouped: dict[
        tuple[str | None, str | None, str | None, str | None], Counter[str]
    ] = {}
    start_dates: dict[
        tuple[str | None, str | None, str | None, str | None], list[str]
    ] = {}
    for row in rows:
        parsed_instrument, parsed_data_level, parsed_descriptor = parse_asset_name(
            row["asset"]
        )
        values: tuple[str | None, str | None, str | None, str | None] = (
            parsed_instrument if "instrument" in enabled_dimensions else None,
            parsed_data_level if "data_level" in enabled_dimensions else None,
            parsed_descriptor if "descriptor" in enabled_dimensions else None,
            row["missing_file"] if "missing_file" in enabled_dimensions else None,
        )
        grouped.setdefault(values, Counter())[row["status"]] += 1
        timestamp_range = partition_timestamp_range(row.get("partition", ""))
        if timestamp_range is not None:
            start_dates.setdefault(values, []).append(
                timestamp_range[0].date().isoformat()
            )

    result: list[SummaryRow] = []
    for dimensions, counts in grouped.items():
        instrument, data_level, descriptor, missing_file = dimensions
        dates = start_dates.get(dimensions, [])
        labels: tuple[str, ...] = tuple(
            value if value is not None else "All"
            for value in (
                instrument,
                data_level,
                descriptor,
                missing_file,
            )
        )
        result.append(
            {
                "row_id": "\0".join(labels),
                "instrument": instrument,
                "data_level": data_level,
                "descriptor": descriptor or "",
                "missing_file": missing_file or "",
                "first_date": min(dates) if dates else "",
                "last_date": max(dates) if dates else "",
                "materialized": counts["materialized"],
                "materializing": counts["materializing"],
                "failed": counts["failed"],
                "skipped": counts["skipped"],
                "not_run": counts["not-run"],
                "not_found": counts["not-found"],
            }
        )
    return result
