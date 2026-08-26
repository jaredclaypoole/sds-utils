from contextlib import nullcontext
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock

from sds_utils.dashboard.backend.data import DagsterAssetsDataSource
from sds_utils.dashboard.dagster_graphql_client.asset_partition_state import (
    AssetPartitionState,
)
from sds_utils.dashboard.dagster_graphql_client.enums import RunStatus


def _event(event_type: str) -> SimpleNamespace:
    return SimpleNamespace(typename__=event_type)


def _page(events: list[SimpleNamespace], cursor: str) -> SimpleNamespace:
    return SimpleNamespace(
        asset_or_error=SimpleNamespace(
            typename__="Asset",
            asset_event_history=SimpleNamespace(results=events, cursor=cursor),
        )
    )


def _configure_partitions(
    client: MagicMock,
    *partitions: str,
    failed: tuple[str, ...] = (),
) -> None:
    client.asset_partition_state.return_value = AssetPartitionState.model_validate(
        {
            "assetNodeOrError": {
                "__typename": "AssetNode",
                "assetKey": {"path": ["mag_l1d_example"]},
                "partitionKeys": list(partitions),
                "partitionDefinition": None,
                "partitionKeysByDimension": [],
                "assetPartitionStatuses": {
                    "__typename": "DefaultPartitionStatuses",
                    "materializedPartitions": [
                        partition for partition in partitions if partition not in failed
                    ],
                    "materializingPartitions": [],
                    "failedPartitions": list(failed),
                    "unmaterializedPartitions": [],
                },
            }
        }
    )


class MaterializationHistoryTests(TestCase):
    def test_targeted_failure_after_previous_success(self) -> None:
        client = MagicMock()
        client.asset_partition_pair_states.return_value = SimpleNamespace(
            asset_nodes=[
                SimpleNamespace(
                    latest_materialization_by_partition=[
                        SimpleNamespace(timestamp="2000")
                    ],
                    latest_run_for_partition=SimpleNamespace(
                        status=RunStatus.FAILURE,
                        update_time=3.0,
                    ),
                )
            ]
        )
        source = DagsterAssetsDataSource()
        source._client = lambda: nullcontext(client)  # type: ignore[method-assign]

        result = source.latest_attempt_failed_after_previous_success_targeted(
            ("mag_l1d_example",), "partition"
        )

        self.assertTrue(result)

    def test_targeted_failure_without_previous_success_does_not_match(self) -> None:
        client = MagicMock()
        client.asset_partition_pair_states.return_value = SimpleNamespace(
            asset_nodes=[
                SimpleNamespace(
                    latest_materialization_by_partition=[],
                    latest_run_for_partition=SimpleNamespace(
                        status=RunStatus.FAILURE,
                        update_time=3.0,
                    ),
                )
            ]
        )
        source = DagsterAssetsDataSource()
        source._client = lambda: nullcontext(client)  # type: ignore[method-assign]

        result = source.latest_attempt_failed_after_previous_success_targeted(
            ("mag_l1d_example",), "partition"
        )

        self.assertFalse(result)

    def test_latest_failure_with_success_on_an_older_page(self) -> None:
        client = MagicMock()
        _configure_partitions(client, "partition", failed=("partition",))
        client.asset_partition_history.side_effect = [
            _page(
                [
                    _event("ObservationEvent"),
                    _event("FailedToMaterializeEvent"),
                ],
                "next",
            ),
            _page([_event("MaterializationEvent")], "done"),
        ]
        source = DagsterAssetsDataSource()
        source._client = lambda: nullcontext(client)  # type: ignore[method-assign]

        result = source.latest_attempt_failed_after_previous_success(
            ("mag_l1d_example",), "partition"
        )

        self.assertTrue(result)
        self.assertEqual(client.asset_partition_history.call_count, 2)

    def test_latest_success_does_not_match(self) -> None:
        client = MagicMock()
        _configure_partitions(client, "partition")
        client.asset_partition_history.return_value = _page(
            [
                _event("MaterializationEvent"),
                _event("FailedToMaterializeEvent"),
            ],
            "done",
        )
        source = DagsterAssetsDataSource()
        source._client = lambda: nullcontext(client)  # type: ignore[method-assign]

        result = source.latest_attempt_failed_after_previous_success(
            ("mag_l1d_example",), "partition"
        )

        self.assertFalse(result)
        client.asset_partition_history.assert_not_called()

    def test_latest_failure_without_previous_success_does_not_match(self) -> None:
        client = MagicMock()
        _configure_partitions(client, "partition", failed=("partition",))
        client.asset_partition_history.return_value = _page(
            [_event("FailedToMaterializeEvent")], ""
        )
        source = DagsterAssetsDataSource()
        source._client = lambda: nullcontext(client)  # type: ignore[method-assign]

        result = source.latest_attempt_failed_after_previous_success(
            ("mag_l1d_example",), "partition"
        )

        self.assertFalse(result)

    def test_unknown_partition_raises_value_error(self) -> None:
        client = MagicMock()
        _configure_partitions(client, "real-partition")
        source = DagsterAssetsDataSource()
        source._client = lambda: nullcontext(client)  # type: ignore[method-assign]

        with self.assertRaisesRegex(ValueError, "Unknown partition 'garbage'"):
            source.latest_attempt_failed_after_previous_success(
                ("mag_l1d_example",), "garbage"
            )

        client.asset_partition_history.assert_not_called()

    def test_unknown_asset_raises_value_error(self) -> None:
        client = MagicMock()
        client.asset_partition_state.return_value = SimpleNamespace(
            asset_node_or_error=SimpleNamespace(typename__="AssetNotFoundError")
        )
        source = DagsterAssetsDataSource()
        source._client = lambda: nullcontext(client)  # type: ignore[method-assign]

        with self.assertRaisesRegex(ValueError, "Unknown Dagster asset: garbage"):
            source.latest_attempt_failed_after_previous_success(
                ("garbage",), "partition"
            )

        client.asset_partition_history.assert_not_called()
