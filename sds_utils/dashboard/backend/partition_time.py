"""Application-aware timestamp ranges encoded in partition names."""

import re
from datetime import UTC, datetime
from enum import StrEnum


class TimestampFiltering(StrEnum):
    """Supported combinations of activity and partition-time filtering."""

    ACTIVE_ONLY = "active_only"
    ACTIVE_OR_PARTITION = "active_or_partition"
    PARTITION_ONLY = "partition_only"


_TIMESTAMP = (
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?"
)
_TIMESTAMP_RANGE_SUFFIX = re.compile(
    rf"_(?P<start>{_TIMESTAMP})_to_(?P<end>{_TIMESTAMP})$"
)


def partition_type(partition: str) -> str:
    """Return the stable type prefix from a timestamp-ranged partition name."""
    if not partition:
        return "unpartitioned"
    match = _TIMESTAMP_RANGE_SUFFIX.search(partition)
    prefix = partition[: match.start()] if match is not None else partition
    if re.fullmatch(r"repoint\d*", prefix, flags=re.IGNORECASE):
        return "repoint"
    if re.fullmatch(r"(?:daily|day)\d*", prefix, flags=re.IGNORECASE):
        return "daily"
    return prefix


def partition_timestamp_range(partition: str) -> tuple[datetime, datetime] | None:
    """Parse a trailing ``_<timestamp>_to_<timestamp>`` partition range."""
    match = _TIMESTAMP_RANGE_SUFFIX.search(partition)
    if match is None:
        return None
    start = _parse_timestamp(match.group("start"))
    end = _parse_timestamp(match.group("end"))
    if start > end:
        return None
    return start, end


def is_stale_partition(
    partition: str,
    *,
    window_start: datetime,
    window_end: datetime,
) -> bool:
    """Return whether a recognized partition range does not overlap a window."""
    timestamp_range = partition_timestamp_range(partition)
    if timestamp_range is None:
        return False
    partition_start, partition_end = timestamp_range
    return partition_end < window_start or partition_start > window_end


def partition_overlaps_window(
    partition: str,
    *,
    window_start: datetime,
    window_end: datetime,
) -> bool:
    """Return whether a recognized partition range overlaps the window."""
    timestamp_range = partition_timestamp_range(partition)
    if timestamp_range is None:
        return False
    partition_start, partition_end = timestamp_range
    return partition_end >= window_start and partition_start <= window_end


def include_partition(
    partition: str,
    updated_in_window: bool,
    *,
    mode: TimestampFiltering,
    window_start: datetime,
    window_end: datetime,
) -> bool:
    """Apply the selected timestamp-filtering policy to one status row."""
    partition_in_range = partition_overlaps_window(
        partition,
        window_start=window_start,
        window_end=window_end,
    )
    if mode is TimestampFiltering.ACTIVE_ONLY:
        return updated_in_window
    if mode is TimestampFiltering.ACTIVE_OR_PARTITION:
        return updated_in_window or partition_in_range
    return partition_in_range


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (
        parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    )
