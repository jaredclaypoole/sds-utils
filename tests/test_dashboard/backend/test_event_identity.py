from unittest import TestCase

from sds_utils.dashboard.backend.event_identity import (
    make_attempt_id,
    make_event_id,
    make_run_event_id,
)


class EventIdentityTests(TestCase):
    def test_attempt_identity_distinguishes_assets_and_partitions(self) -> None:
        common = {"dagster_source": "http://dagster", "run_id": "run-1"}
        first = make_attempt_id(
            asset_path=("asset_a",), partition="partition_a", **common
        )
        self.assertEqual(
            first,
            make_attempt_id(
                asset_path=("asset_a",), partition="partition_a", **common
            ),
        )
        self.assertNotEqual(
            first,
            make_attempt_id(
                asset_path=("asset_b",), partition="partition_a", **common
            ),
        )

    def test_event_identity_distinguishes_exact_events(self) -> None:
        common = {"attempt_id": "attempt-1", "step_key": "step"}
        first = make_event_id(
            event_type="MaterializationEvent", timestamp="1000", **common
        )
        self.assertEqual(
            first,
            make_event_id(
                event_type="MaterializationEvent", timestamp="1000", **common
            ),
        )
        self.assertNotEqual(
            first,
            make_event_id(event_type="ObservationEvent", timestamp="1000", **common),
        )

    def test_run_event_identity_is_shared_across_asset_attempts(self) -> None:
        identity = make_run_event_id(
            dagster_source="http://dagster",
            run_id="run-1",
            event_type="RunFailureEvent",
            timestamp="1000",
            step_key=None,
        )
        self.assertEqual(
            identity,
            make_run_event_id(
                dagster_source="http://dagster/",
                run_id="run-1",
                event_type="RunFailureEvent",
                timestamp="1000",
                step_key=None,
            ),
        )
