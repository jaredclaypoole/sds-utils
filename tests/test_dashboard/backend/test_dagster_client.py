import os
from unittest import TestCase
from unittest.mock import patch

import httpx

from sds_utils.dashboard.dagster_graphql_client import DagsterGraphQLClient
from sds_utils.dashboard.backend.dagster_client import (
    RetryingDagsterGraphQLClient,
    asset_state_batch_size,
    graphql_max_attempts,
    graphql_timeout_seconds,
    native_status_mode,
    run_event_batch_size,
    run_event_workers,
    run_page_size,
)


class DagsterClientConfigurationTests(TestCase):
    def test_defaults(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(graphql_timeout_seconds(), 30.0)
            self.assertEqual(graphql_max_attempts(), 2)
            self.assertEqual(asset_state_batch_size(), 100)
            self.assertEqual(run_page_size(), 500)
            self.assertEqual(run_event_batch_size(), 100)
            self.assertEqual(run_event_workers(), 4)
            self.assertEqual(native_status_mode(), "pair")

    def test_environment_overrides(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DAGSTER_GRAPHQL_TIMEOUT_SECONDS": "45.5",
                "DAGSTER_GRAPHQL_MAX_ATTEMPTS": "3",
                "DAGSTER_ASSET_STATE_BATCH_SIZE": "75",
                "DAGSTER_RUN_PAGE_SIZE": "400",
                "DAGSTER_RUN_EVENT_BATCH_SIZE": "80",
                "DAGSTER_RUN_EVENT_WORKERS": "6",
                "DAGSTER_NATIVE_STATUS_MODE": "aggregate",
            },
            clear=True,
        ):
            self.assertEqual(graphql_timeout_seconds(), 45.5)
            self.assertEqual(graphql_max_attempts(), 3)
            self.assertEqual(asset_state_batch_size(), 75)
            self.assertEqual(run_page_size(), 400)
            self.assertEqual(run_event_batch_size(), 80)
            self.assertEqual(run_event_workers(), 6)
            self.assertEqual(native_status_mode(), "aggregate")

    def test_invalid_values_are_rejected(self) -> None:
        variables = {
            "DAGSTER_GRAPHQL_TIMEOUT_SECONDS": "0",
            "DAGSTER_GRAPHQL_MAX_ATTEMPTS": "nope",
            "DAGSTER_ASSET_STATE_BATCH_SIZE": "-1",
            "DAGSTER_RUN_PAGE_SIZE": "0",
            "DAGSTER_RUN_EVENT_BATCH_SIZE": "bad",
            "DAGSTER_RUN_EVENT_WORKERS": "-2",
            "DAGSTER_NATIVE_STATUS_MODE": "other",
        }
        functions = {
            "DAGSTER_GRAPHQL_TIMEOUT_SECONDS": graphql_timeout_seconds,
            "DAGSTER_GRAPHQL_MAX_ATTEMPTS": graphql_max_attempts,
            "DAGSTER_ASSET_STATE_BATCH_SIZE": asset_state_batch_size,
            "DAGSTER_RUN_PAGE_SIZE": run_page_size,
            "DAGSTER_RUN_EVENT_BATCH_SIZE": run_event_batch_size,
            "DAGSTER_RUN_EVENT_WORKERS": run_event_workers,
            "DAGSTER_NATIVE_STATUS_MODE": native_status_mode,
        }
        for name, value in variables.items():
            with self.subTest(name=name), patch.dict(
                os.environ, {name: value}, clear=True
            ), self.assertRaises(ValueError):
                functions[name]()

    def test_read_timeout_is_retried(self) -> None:
        request = httpx.Request("POST", "https://dagster.example/graphql")
        response = httpx.Response(200, request=request)
        client = RetryingDagsterGraphQLClient(
            url=str(request.url),
            headers=None,
            timeout_seconds=1,
            max_attempts=2,
        )
        try:
            with patch.object(
                DagsterGraphQLClient,
                "execute",
                side_effect=[httpx.ReadTimeout("slow", request=request), response],
            ) as execute, patch("sds_utils.dashboard.backend.dagster_client.sleep"):
                result = client.execute("query { __typename }")
        finally:
            client.http_client.close()

        self.assertIs(result, response)
        self.assertEqual(execute.call_count, 2)
