"""Asset-name parsing and status aggregation for the summary view."""

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TypedDict

from ..backend.data import AssetStatusRow, parse_asset_name
from ..backend.partition_time import partition_timestamp_range
from .models import SummaryDateAggregation

SUMMARY_DIMENSIONS = ("instrument", "data_level", "descriptor", "missing_file")
SUMMARY_STATUSES = (
    "materialized",
    "materializing",
    "failed",
    "skipped",
    "not-run",
    "not-found",
)
MIN_AGGREGATION_DAYS = 2


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


@dataclass(frozen=True)
class SummaryDrilldown:
    """Transient detail-row filter represented by one summary row."""

    dimensions: tuple[tuple[str, str | None], ...]
    first_date: date | None = None
    last_date: date | None = None
    require_unpartitioned: bool = False

    def matches(self, row: AssetStatusRow) -> bool:
        """Return whether a detail row belongs to this summary group."""
        if any(
            _detail_dimension_value(row, dimension) != value
            for dimension, value in self.dimensions
        ):
            return False
        timestamp_range = partition_timestamp_range(row.get("partition", ""))
        if self.require_unpartitioned:
            return timestamp_range is None
        if self.first_date is None or self.last_date is None:
            return True
        if timestamp_range is None:
            return False
        partition_date = timestamp_range[0].date()
        return self.first_date <= partition_date <= self.last_date

    @property
    def label(self) -> str:
        """Return a compact chip label for this filter."""
        parts = [
            "(empty)" if value is None or value == "" else value
            for _dimension, value in self.dimensions
        ]
        if self.first_date is not None and self.last_date is not None:
            parts.append(
                self.first_date.isoformat()
                if self.first_date == self.last_date
                else f"{self.first_date.isoformat()} to {self.last_date.isoformat()}"
            )
        elif self.require_unpartitioned:
            parts.append("without partition date")
        return "Summary: " + (" / ".join(parts) if parts else "all rows")


def summary_drilldown(
    row: SummaryRow,
    enabled_dimensions: set[str],
    date_aggregation: SummaryDateAggregation,
) -> SummaryDrilldown:
    """Build a transient detail filter from a rendered summary row."""
    values: dict[str, str | None] = {
        "instrument": row["instrument"],
        "data_level": row["data_level"],
        "descriptor": row["descriptor"],
        "missing_file": row["missing_file"],
    }
    dimensions = tuple(
        (dimension, values[dimension])
        for dimension in SUMMARY_DIMENSIONS
        if dimension in enabled_dimensions
    )
    if date_aggregation == "all":
        return SummaryDrilldown(dimensions)
    if not row["first_date"]:
        return SummaryDrilldown(dimensions, require_unpartitioned=True)
    return SummaryDrilldown(
        dimensions,
        first_date=date.fromisoformat(row["first_date"]),
        last_date=date.fromisoformat(row["last_date"]),
    )


def _detail_dimension_value(row: AssetStatusRow, dimension: str) -> str | None:
    if dimension == "instrument":
        return row["instrument"]
    if dimension == "data_level":
        return row["data_level"]
    if dimension == "descriptor":
        return row["descriptor"]
    if dimension == "missing_file":
        return row["missing_file"]
    raise ValueError(f"Unknown summary dimension: {dimension}")


def summarize_status_rows(
    rows: Iterable[AssetStatusRow],
    enabled_dimensions: set[str],
    date_aggregation: SummaryDateAggregation = "all",
    aggregation_days: int = MIN_AGGREGATION_DAYS,
) -> list[SummaryRow]:
    """Count statuses for each enabled asset-name dimension and date bucket."""
    if aggregation_days < MIN_AGGREGATION_DAYS:
        raise ValueError(f"aggregation_days must be at least {MIN_AGGREGATION_DAYS}")
    grouped: dict[
        tuple[str | None, str | None, str | None, str | None, date | None],
        Counter[str],
    ] = {}
    start_dates: dict[
        tuple[str | None, str | None, str | None, str | None, date | None],
        list[date],
    ] = {}
    for row in rows:
        parsed_instrument, parsed_data_level, parsed_descriptor = parse_asset_name(
            row["asset"]
        )
        timestamp_range = partition_timestamp_range(row.get("partition", ""))
        partition_date = timestamp_range[0].date() if timestamp_range else None
        bucket_start = _date_bucket_start(
            partition_date, date_aggregation, aggregation_days
        )
        values = (
            parsed_instrument if "instrument" in enabled_dimensions else None,
            parsed_data_level if "data_level" in enabled_dimensions else None,
            parsed_descriptor if "descriptor" in enabled_dimensions else None,
            row["missing_file"] if "missing_file" in enabled_dimensions else None,
            bucket_start,
        )
        grouped.setdefault(values, Counter())[row["status"]] += 1
        if partition_date is not None:
            start_dates.setdefault(values, []).append(partition_date)

    result: list[SummaryRow] = []
    for dimensions, counts in grouped.items():
        instrument, data_level, descriptor, missing_file, bucket_start = dimensions
        dates = start_dates.get(dimensions, [])
        labels: tuple[str, ...] = tuple(
            value if value is not None else "All"
            for value in (
                instrument,
                data_level,
                descriptor,
                missing_file,
                bucket_start.isoformat() if bucket_start else None,
            )
        )
        if bucket_start is None or date_aggregation == "all":
            first_date = min(dates).isoformat() if dates else ""
            last_date = max(dates).isoformat() if dates else ""
        else:
            bucket_days = (
                1
                if date_aggregation == "day"
                else 7
                if date_aggregation == "week"
                else aggregation_days
            )
            first_date = bucket_start.isoformat()
            last_date = (bucket_start + timedelta(days=bucket_days - 1)).isoformat()
        result.append(
            {
                "row_id": "\0".join(labels),
                "instrument": instrument,
                "data_level": data_level,
                "descriptor": descriptor or "",
                "missing_file": missing_file or "",
                "first_date": first_date,
                "last_date": last_date,
                "materialized": counts["materialized"],
                "materializing": counts["materializing"],
                "failed": counts["failed"],
                "skipped": counts["skipped"],
                "not_run": counts["not-run"],
                "not_found": counts["not-found"],
            }
        )
    return result


def _date_bucket_start(
    value: date | None,
    aggregation: SummaryDateAggregation,
    aggregation_days: int,
) -> date | None:
    if value is None or aggregation == "all":
        return None
    if aggregation == "day":
        return value
    if aggregation == "week":
        return value - timedelta(days=value.weekday())
    epoch = date(1970, 1, 1)
    bucket_number = (value - epoch).days // aggregation_days
    return epoch + timedelta(days=bucket_number * aggregation_days)
