"""Tests for Dagster partition-name parsing."""

import datetime

from sds_utils.dashboard.backend.partitions import PartitionParts, parse_partition


def test_parse_repoint_partition() -> None:
    partition = (
        "repoint343_2026-09-05T00:00:00+00:00_to_"
        "2026-09-06T23:59:59+00:00"
    )

    assert parse_partition(partition) == PartitionParts(
        prefix="repoint343",
        label="repoint",
        repoint=343,
        start_time=datetime.datetime(2026, 9, 5, tzinfo=datetime.UTC),
        end_time=datetime.datetime(2026, 9, 6, 23, 59, 59, tzinfo=datetime.UTC),
    )


def test_parse_regular_partition() -> None:
    partition = (
        "long-label_2026-09-07T00:00:00+00:00_to_"
        "2026-09-08T00:00:00+00:00"
    )

    assert parse_partition(partition) == PartitionParts(
        prefix="long-label",
        label="long-label",
        repoint=None,
        start_time=datetime.datetime(2026, 9, 7, tzinfo=datetime.UTC),
        end_time=datetime.datetime(2026, 9, 8, tzinfo=datetime.UTC),
    )


def test_digits_in_non_repoint_prefix_are_not_a_repoint_number() -> None:
    partition = (
        "idex10_2026-09-07T00:00:00+00:00_to_"
        "2026-09-08T00:00:00+00:00"
    )

    parsed = parse_partition(partition)

    assert parsed.prefix == "idex10"
    assert parsed.label == "idex10"
    assert parsed.repoint is None
