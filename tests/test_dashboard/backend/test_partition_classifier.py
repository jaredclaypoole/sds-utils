from types import SimpleNamespace
from unittest import TestCase

from sds_utils.dashboard.backend.partition_classifier import (
    PartitionCategory,
    _native_partition_categories,
)


class NativePartitionStatusTests(TestCase):
    def test_overlapping_statuses_use_dashboard_precedence(self) -> None:
        statuses = SimpleNamespace(
            typename__="DefaultPartitionStatuses",
            materialized_partitions=["materialized", "failed", "materializing"],
            materializing_partitions=["materializing", "active-failure"],
            failed_partitions=["failed", "active-failure"],
            unmaterialized_partitions=[],
        )
        node = SimpleNamespace(
            asset_key=SimpleNamespace(path=["example"]),
            asset_partition_statuses=statuses,
            partition_keys=[
                "materialized",
                "failed",
                "materializing",
                "active-failure",
            ],
        )

        categories = _native_partition_categories(node)

        self.assertEqual(
            categories[PartitionCategory.MATERIALIZED], {"materialized"}
        )
        self.assertEqual(categories[PartitionCategory.FAILED], {"failed"})
        self.assertEqual(
            categories[PartitionCategory.MATERIALIZING],
            {"materializing", "active-failure"},
        )
