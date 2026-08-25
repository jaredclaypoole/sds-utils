from contextlib import nullcontext
import os
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import MagicMock, patch

from sds_utils.dashboard.dagster_graphql_client.backfill_details import BackfillDetails
from sds_utils.dashboard.dagster_graphql_client.backfill_runs import BackfillRuns
from sds_utils.dashboard.dagster_graphql_client.backfills import Backfills
from sds_utils.dashboard.dagster_graphql_client.enums import RunStatus

from sds_utils.dashboard.backend.backfills import BackfillRunDetail, _partition_status
from sds_utils.dashboard.frontend.backfills import (
    BackfillDetailView,
    BackfillRunsTable,
)



class BackfillDetailViewTests(IsolatedAsyncioTestCase):
    async def test_detailed_runs_are_loaded_only_once(self) -> None:
        load_runs = MagicMock(return_value=())
        view = object.__new__(BackfillDetailView)
        view.backfill_id = "backfill-id"
        view.data_source = SimpleNamespace(load_backfill_runs=load_runs)
        view.runs_loaded = False
        view.runs_loading = False
        view.runs_loading_label = MagicMock()
        view.runs_table = MagicMock()

        await view._load_runs_once()
        await view._load_runs_once()

        load_runs.assert_called_once_with("backfill-id")
        view.runs_table.set_rows.assert_called_once_with(())
        self.assertTrue(view.runs_loaded)


class BackfillRunsTableTests(TestCase):
    def test_success_without_asset_output_is_missing_output(self) -> None:
        planned = SimpleNamespace(
            typename__="AssetMaterializationPlannedEvent",
            asset_key=SimpleNamespace(path=["mag_l1d_gradiometryoffsetsburst"]),
        )

        self.assertEqual(
            _partition_status(
                [planned],
                ("mag_l1d_gradiometryoffsetsburst",),
                RunStatus.SUCCESS,
            ),
            "missing-output",
        )

    def test_skipped_external_submission_is_log_skipped(self) -> None:
        log_event = SimpleNamespace(
            typename__="LogMessageEvent",
            message=(
                "Submit response: skipped\n"
                "- Job already completed or in progress., "
                "{'status': 'INPROGRESS'}"
            ),
        )

        self.assertEqual(
            _partition_status(
                [log_event],
                ("mag_l1d_burstdsrf",),
                RunStatus.SUCCESS,
            ),
            "log-skipped",
        )

    def test_skipped_observation_is_a_terminal_partition_status(self) -> None:
        observation = SimpleNamespace(
            typename__="ObservationEvent",
            asset_key=SimpleNamespace(path=["mag_l1d_burstdsrf"]),
            metadata_entries=[
                SimpleNamespace(
                    label="status", text="Skipped - Missing Dependencies"
                )
            ],
        )

        self.assertEqual(
            _partition_status(
                [observation],
                ("mag_l1d_burstdsrf",),
                RunStatus.SUCCESS,
            ),
            "skipped",
        )

    def test_filters_status_and_sorts_parsed_asset_columns(self) -> None:
        table = BackfillRunsTable(
            on_settings_change=MagicMock(),
            on_rows_change=MagicMock(),
            visible_statuses={"materialized"},
        )
        table.table = MagicMock()
        table.set_rows(
            (
                BackfillRunDetail(
                    "2", "swe_l2_sci", "swe", "l2", "sci", "p2", "p2-url",
                    "failed", "FAILURE", "run-2", "https://example/runs/2",
                ),
                BackfillRunDetail(
                    "1", "mag_l1a_norm", "mag", "l1a", "norm", "p1", "p1-url",
                    "materialized", "SUCCESS", "run-1", "https://example/runs/1",
                ),
            )
        )

        self.assertEqual([row["instrument"] for row in table.table.rows], ["mag"])
