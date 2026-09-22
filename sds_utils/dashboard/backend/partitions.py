"""Parse time intervals encoded in Dagster partition names."""

import datetime
import re
from typing import NamedTuple

_PARTITION_PATTERN = re.compile(
    r"^(?P<prefix>.+)_"
    r"(?P<start_time>\d{4}-\d{2}-\d{2}T[^_]+)_to_"
    r"(?P<end_time>\d{4}-\d{2}-\d{2}T.+)$"
)
_REPOINT_PATTERN = re.compile(r"^repoint(?P<repoint>\d*)$")


class PartitionParts(NamedTuple):
    """Fields parsed from a Dagster partition name."""

    prefix: str | None
    label: str | None
    repoint: int | None
    start_time: datetime.datetime | None
    end_time: datetime.datetime | None


def parse_partition(partition: str | None) -> PartitionParts:
    """Parse identity and UTC interval fields from a partition name."""
    if partition is None or (match := _PARTITION_PATTERN.fullmatch(partition)) is None:
        return PartitionParts(None, None, None, None, None)
    try:
        start_time = datetime.datetime.fromisoformat(match.group("start_time"))
        end_time = datetime.datetime.fromisoformat(match.group("end_time"))
    except ValueError:
        return PartitionParts(None, None, None, None, None)
    if (
        start_time.tzinfo is None
        or start_time.utcoffset() is None
        or end_time.tzinfo is None
        or end_time.utcoffset() is None
    ):
        return PartitionParts(None, None, None, None, None)

    prefix = match.group("prefix")
    repoint_match = _REPOINT_PATTERN.fullmatch(prefix)
    label = "repoint" if repoint_match is not None else prefix
    repoint_text = repoint_match.group("repoint") if repoint_match is not None else ""
    repoint = int(repoint_text) if repoint_text else None
    return PartitionParts(
        prefix,
        label,
        repoint,
        start_time.astimezone(datetime.UTC),
        end_time.astimezone(datetime.UTC),
    )
