"""Configured, retrying access to the generated Dagster GraphQL client."""

import logging
import os
from collections.abc import Mapping
from time import sleep
from typing import Any

import httpx

from sds_utils.dashboard.dagster_graphql_client import DagsterGraphQLClient

logger = logging.getLogger("uvicorn.error.sds_utils.dashboard.backend.dagster_client")

DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_ATTEMPTS = 2
DEFAULT_ASSET_STATE_BATCH_SIZE = 100
DEFAULT_RUN_PAGE_SIZE = 500
DEFAULT_RUN_EVENT_BATCH_SIZE = 100
DEFAULT_RUN_EVENT_WORKERS = 4


def graphql_timeout_seconds() -> float:
    """Return the configured GraphQL request timeout."""
    return _positive_float("DAGSTER_GRAPHQL_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)


def graphql_max_attempts() -> int:
    """Return the configured maximum number of GraphQL attempts."""
    return _positive_int("DAGSTER_GRAPHQL_MAX_ATTEMPTS", DEFAULT_MAX_ATTEMPTS)


def asset_state_batch_size() -> int:
    """Return the configured asset-state query batch size."""
    return _positive_int(
        "DAGSTER_ASSET_STATE_BATCH_SIZE", DEFAULT_ASSET_STATE_BATCH_SIZE
    )


def run_page_size() -> int:
    """Return the configured run-query page size."""
    return _positive_int("DAGSTER_RUN_PAGE_SIZE", DEFAULT_RUN_PAGE_SIZE)


def run_event_batch_size() -> int:
    """Return the configured run-event query batch size."""
    return _positive_int("DAGSTER_RUN_EVENT_BATCH_SIZE", DEFAULT_RUN_EVENT_BATCH_SIZE)


def run_event_workers() -> int:
    """Return the configured number of concurrent run-event workers."""
    return _positive_int("DAGSTER_RUN_EVENT_WORKERS", DEFAULT_RUN_EVENT_WORKERS)


def native_status_mode() -> str:
    """Return the validated native partition-status query mode."""
    value = os.getenv("DAGSTER_NATIVE_STATUS_MODE", "pair")
    if value not in {"pair", "aggregate"}:
        raise ValueError(
            "DAGSTER_NATIVE_STATUS_MODE must be either 'pair' or 'aggregate'"
        )
    return value


class RetryingDagsterGraphQLClient(DagsterGraphQLClient):
    """Retry read timeouts for the application's read-only GraphQL operations."""

    def __init__(
        self,
        *,
        url: str,
        headers: Mapping[str, str] | None,
        timeout_seconds: float,
        max_attempts: int,
    ) -> None:
        self.max_attempts = max_attempts
        super().__init__(
            url=url,
            http_client=httpx.Client(headers=headers, timeout=timeout_seconds),
        )

    def execute(
        self,
        query: str,
        operation_name: str | None = None,
        variables: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute a GraphQL request, retrying transient read timeouts."""
        for attempt in range(1, self.max_attempts + 1):
            try:
                return super().execute(
                    query=query,
                    operation_name=operation_name,
                    variables=variables,
                    **kwargs,
                )
            except httpx.ReadTimeout:
                if attempt == self.max_attempts:
                    raise
                delay = 0.25 * 2 ** (attempt - 1)
                logger.warning(
                    "Dagster GraphQL read timed out; retrying operation=%s "
                    "attempt=%d/%d delay=%.2fs",
                    operation_name or "unknown",
                    attempt + 1,
                    self.max_attempts,
                    delay,
                )
                sleep(delay)
        raise AssertionError("unreachable")


def create_dagster_client(
    *,
    url: str,
    headers: Mapping[str, str] | None,
) -> RetryingDagsterGraphQLClient:
    """Create a generated client with application timeout and retry defaults."""
    return RetryingDagsterGraphQLClient(
        url=url,
        headers=headers,
        timeout_seconds=graphql_timeout_seconds(),
        max_attempts=graphql_max_attempts(),
    )


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        value = default if raw is None else int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    try:
        value = default if raw is None else float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value
