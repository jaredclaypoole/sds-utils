from contextlib import nullcontext
import os
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from sds_utils.dashboard.dagster_graphql_client.backfill_details import BackfillDetails
from sds_utils.dashboard.dagster_graphql_client.backfill_runs import BackfillRuns
from sds_utils.dashboard.dagster_graphql_client.backfills import Backfills
from sds_utils.dashboard.dagster_graphql_client.enums import RunStatus

from sds_utils.dashboard.backend.backfills import DagsterBackfillsDataSource

class BackfillsDataSourceTests(TestCase):
    def test_lists_backfills_with_server_side_status_filter(self) -> None:
        client = MagicMock()
        client.backfills.return_value = Backfills.model_validate(
            {
                "partitionBackfillsOrError": {
                    "__typename": "PartitionBackfills",
                    "results": [
                        {
                            "id": "active-id",
                            "status": "REQUESTED",
                            "title": None,
                            "description": None,
                            "creationTime": 0,
                            "endTime": None,
                            "numPartitions": 349,
                            "isAssetBackfill": True,
                        }
                    ],
                }
            }
        )
        source = DagsterBackfillsDataSource()
        source._client = lambda: nullcontext(client)  # type: ignore[method-assign]

        with patch.dict(os.environ, {"DAGSTER_BASE_URL": "https://dagster.test"}):
            result = source.list_backfills(["REQUESTED"])

        self.assertEqual(result[0].id, "active-id")
        self.assertEqual(result[0].title, "Backfill active-id")
        self.assertEqual(result[0].partitions, 349)
        filters = client.backfills.call_args.kwargs["filters"]
        self.assertEqual([status.value for status in filters.statuses], ["REQUESTED"])

    def test_loads_asset_counts_and_partition_run_links(self) -> None:
        client = MagicMock()
        client.backfill_details.return_value = BackfillDetails.model_validate(
            {
                "partitionBackfillOrError": {
                    "__typename": "PartitionBackfill",
                    "id": "backfill-id",
                    "status": "REQUESTED",
                    "title": "Example",
                    "description": "",
                    "creationTime": 0,
                    "endTime": None,
                    "numPartitions": 3,
                    "partitionNames": ["p1", "p2", "p3"],
                    "isAssetBackfill": True,
                    "assetBackfillData": {
                        "assetBackfillStatuses": [
                            {
                                "__typename": "AssetPartitionsStatusCounts",
                                "assetKey": {"path": ["mag_asset"]},
                                "numPartitionsTargeted": 3,
                                "numPartitionsInProgress": 1,
                                "numPartitionsMaterialized": 1,
                                "numPartitionsFailed": 1,
                            }
                        ]
                    },
                }
            }
        )
        client.backfill_runs.return_value = BackfillRuns.model_validate(
            {
                "partitionBackfillOrError": {
                    "__typename": "PartitionBackfill",
                    "id": "backfill-id",
                    "runs": [
                        {
                            "runId": "run-id",
                            "status": "STARTED",
                            "assetSelection": [{"path": ["mag_l1a_norm"]}],
                            "tags": [
                                {"key": "dagster/backfill", "value": "backfill-id"},
                                {"key": "dagster/partition", "value": "p3"},
                            ],
                        }
                    ],
                }
            }
        )
        source = DagsterBackfillsDataSource()
        source._client = lambda: nullcontext(client)  # type: ignore[method-assign]

        materialization = SimpleNamespace(
            typename__="MaterializationEvent",
            asset_key=SimpleNamespace(path=["mag_l1a_norm"]),
        )
        with (
            patch.dict(os.environ, {"DAGSTER_BASE_URL": "https://dagster.test"}),
            patch(
                "sds_utils.dashboard.backend.backfills.load_run_events_batch",
                return_value={"run-id": [materialization]},
            ),
        ):
            result = source.load_backfill("backfill-id")
            client.backfill_runs.assert_not_called()
            runs = source.load_backfill_runs("backfill-id")

        self.assertEqual(result.asset_counts[0].targeted, 3)
        self.assertEqual(result.asset_counts[0].remaining, 0)
        self.assertEqual(runs[0].asset, "mag_l1a_norm")
        self.assertEqual(runs[0].partition, "p3")
        self.assertEqual(
            runs[0].partition_url,
            "https://dagster.test/assets/mag_l1a_norm?view=partitions&partition=p3",
        )
        self.assertEqual(runs[0].status, "materialized")
        self.assertEqual(runs[0].run_status, "STARTED")
        self.assertEqual(runs[0].instrument, "mag")
        self.assertEqual(
            runs[0].run_url,
            "https://dagster.test/runs/run-id",
        )
