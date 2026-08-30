from datetime import UTC, datetime
from unittest import TestCase

from sds_utils.dashboard.backend.partition_time import (
    TimestampFiltering,
    include_partition,
    is_stale_partition,
    partition_overlaps_window,
    partition_timestamp_range,
    partition_type,
)


class PartitionTimestampRangeTests(TestCase):
    def test_extracts_partition_types(self) -> None:
        self.assertEqual(
            partition_type(
                "repoint251_2026-05-17T10:03:12_to_2026-05-18T10:03:11"
            ),
            "repoint",
        )
        self.assertEqual(
            partition_type("idex10day_2026-05-17T00:00:00_to_2026-05-27T00:00:00"),
            "idex10day",
        )
        self.assertEqual(partition_type(""), "unpartitioned")

    WINDOW_START = datetime(2026, 5, 17, tzinfo=UTC)
    WINDOW_END = datetime(2026, 5, 19, tzinfo=UTC)

    def test_parses_timestamp_range_suffix(self) -> None:
        result = partition_timestamp_range(
            "repoint251_2026-05-17T10:03:12_to_2026-05-18T10:03:11"
        )
        self.assertEqual(
            result,
            (
                datetime(2026, 5, 17, 10, 3, 12, tzinfo=UTC),
                datetime(2026, 5, 18, 10, 3, 11, tzinfo=UTC),
            ),
        )

    def test_supports_timezone_offsets(self) -> None:
        result = partition_timestamp_range(
            "desc_2026-05-17T10:00:00+02:00_to_2026-05-17T11:00:00+02:00"
        )
        self.assertEqual(result[0], datetime(2026, 5, 17, 8, tzinfo=UTC))

    def test_stale_range_has_no_overlap(self) -> None:
        self.assertTrue(
            is_stale_partition(
                "old_2026-05-10T00:00:00_to_2026-05-11T00:00:00",
                window_start=self.WINDOW_START,
                window_end=self.WINDOW_END,
            )
        )

    def test_overlapping_and_touching_ranges_are_not_stale(self) -> None:
        for partition in (
            "overlap_2026-05-16T00:00:00_to_2026-05-18T00:00:00",
            "touch_2026-05-16T00:00:00_to_2026-05-17T00:00:00",
        ):
            self.assertFalse(
                is_stale_partition(
                    partition,
                    window_start=self.WINDOW_START,
                    window_end=self.WINDOW_END,
                )
            )

    def test_unrecognized_partition_is_not_stale(self) -> None:
        self.assertFalse(
            is_stale_partition(
                "ordinary_partition",
                window_start=self.WINDOW_START,
                window_end=self.WINDOW_END,
            )
        )

    def test_partition_overlap_requires_a_recognized_overlapping_range(self) -> None:
        self.assertTrue(
            partition_overlaps_window(
                "current_2026-05-18T00:00:00_to_2026-05-20T00:00:00",
                window_start=self.WINDOW_START,
                window_end=self.WINDOW_END,
            )
        )
        for partition in (
            "old_2026-05-10T00:00:00_to_2026-05-11T00:00:00",
            "ordinary_partition",
            "",
        ):
            self.assertFalse(
                partition_overlaps_window(
                    partition,
                    window_start=self.WINDOW_START,
                    window_end=self.WINDOW_END,
                )
            )

    def test_timestamp_filtering_modes(self) -> None:
        overlapping = "current_2026-05-18T00:00:00_to_2026-05-20T00:00:00"
        outside = "old_2026-05-10T00:00:00_to_2026-05-11T00:00:00"

        def included(
            partition: str, updated: bool, mode: TimestampFiltering
        ) -> bool:
            return include_partition(
                partition,
                updated,
                mode=mode,
                window_start=self.WINDOW_START,
                window_end=self.WINDOW_END,
            )

        self.assertTrue(included(outside, True, TimestampFiltering.ACTIVE_ONLY))
        self.assertFalse(included(overlapping, False, TimestampFiltering.ACTIVE_ONLY))
        self.assertTrue(
            included(overlapping, False, TimestampFiltering.ACTIVE_OR_PARTITION)
        )
        self.assertTrue(
            included(outside, True, TimestampFiltering.ACTIVE_OR_PARTITION)
        )
        self.assertTrue(
            included(overlapping, False, TimestampFiltering.PARTITION_ONLY)
        )
        self.assertFalse(
            included(outside, True, TimestampFiltering.PARTITION_ONLY)
        )
