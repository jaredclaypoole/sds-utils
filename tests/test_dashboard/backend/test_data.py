from datetime import UTC, datetime
from unittest import TestCase

from sds_utils.dashboard.backend.data import (
    AssetOption,
    _ActivityRow,
    _apply_native_statuses,
    _overlapping_asset_partition_keys,
    parse_missing_file,
)
from sds_utils.dashboard.backend.partition_classifier import PartitionCategory


class PartitionKeyReductionTests(TestCase):
    def test_parses_unique_names_and_preserves_actual_asset_ownership(self) -> None:
        in_range = "repoint_2026-05-17T00:00:00_to_2026-05-18T00:00:00"
        out_of_range = "repoint_2026-05-10T00:00:00_to_2026-05-11T00:00:00"
        only_on_second = "other_2026-05-17T12:00:00_to_2026-05-19T00:00:00"
        assets = (
            AssetOption("first", ("first",), True, (in_range, out_of_range)),
            AssetOption("second", ("second",), True, (in_range, only_on_second)),
        )

        keys, unique_count, matching_count = _overlapping_asset_partition_keys(
            assets,
            window_start=datetime(2026, 5, 17, tzinfo=UTC),
            window_end=datetime(2026, 5, 18, tzinfo=UTC),
        )

        self.assertEqual(unique_count, 3)
        self.assertEqual(matching_count, 2)
        self.assertEqual(
            keys,
            {
                (("first",), in_range),
                (("second",), in_range),
                (("second",), only_on_second),
            },
        )


class MissingFileParsingTests(TestCase):
    def test_dependency_message(self) -> None:
        self.assertEqual(
            parse_missing_file(
                "Not enough information to process. Missing "
                "ultra_l1a_45sensorpriority1de in range 2026-08-07"
            ),
            "ultra_l1a_45sensorpriority1de",
        )

    def test_supported_messages_may_contain_newlines(self) -> None:
        self.assertEqual(
            parse_missing_file(
                "Not enough information\nto process. Missing\nmag_l1a_data"
            ),
            "mag_l1a_data",
        )
        self.assertEqual(parse_missing_file("Missing\nSPICE files for interval"), "SPICE")

    def test_unknown_message_is_empty(self) -> None:
        self.assertEqual(parse_missing_file("Dependency unavailable"), "")


class NativeStatusApplicationTests(TestCase):
    def test_native_status_classifies_partition_range_seed(self) -> None:
        key = (("asset",), "partition")
        rows = {
            key: _ActivityRow(
                asset_path=key[0],
                partition=key[1],
                timestamp="",
                sort_timestamp=float("-inf"),
                status=PartitionCategory.NOT_RUN.value,
            )
        }
        native = {
            key[0]: {
                PartitionCategory.MATERIALIZED: {key[1]},
                PartitionCategory.MATERIALIZING: set(),
                PartitionCategory.FAILED: set(),
            }
        }

        _apply_native_statuses(rows, {key}, native)

        self.assertEqual(rows[key].status, PartitionCategory.MATERIALIZED.value)

    def test_native_unmaterialized_retracts_run_level_failure_inference(self) -> None:
        key = (("asset",), "partition")
        rows = {
            key: _ActivityRow(
                asset_path=key[0],
                partition=key[1],
                timestamp="2026-08-10T00:00:00+00:00",
                sort_timestamp=1.0,
                status=PartitionCategory.FAILED.value,
                attempt_id="inferred-attempt",
            )
        }
        native = {
            key[0]: {
                PartitionCategory.MATERIALIZED: set(),
                PartitionCategory.MATERIALIZING: set(),
                PartitionCategory.FAILED: set(),
            }
        }

        _apply_native_statuses(rows, {key}, native)

        self.assertEqual(rows[key].status, PartitionCategory.NOT_RUN.value)
        self.assertIsNone(rows[key].attempt_id)
