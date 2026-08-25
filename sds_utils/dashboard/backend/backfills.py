"""Dagster backfill data access and backend view models."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote, urlencode

from sds_utils.dashboard.dagster_graphql_client import DagsterGraphQLClient
from sds_utils.dashboard.dagster_graphql_client.backfill_details import (
    BackfillDetailsPartitionBackfillOrErrorPartitionBackfill as BackfillResult,
)
from sds_utils.dashboard.dagster_graphql_client.backfill_runs import (
    BackfillRunsPartitionBackfillOrErrorPartitionBackfill as BackfillRunsResult,
)
from sds_utils.dashboard.dagster_graphql_client.backfills import (
    BackfillsPartitionBackfillsOrErrorPartitionBackfills as BackfillListResult,
)
from sds_utils.dashboard.dagster_graphql_client.backfills import (
    BackfillsPartitionBackfillsOrErrorPartitionBackfillsResults as BackfillListItem,
)
from sds_utils.dashboard.dagster_graphql_client.enums import BulkActionStatus, RunStatus
from sds_utils.dashboard.dagster_graphql_client.input_types import BulkActionsFilter

from .api_utils import client_headers, dagster_ui_url, graphql_url
from .dagster_client import create_dagster_client, run_event_batch_size
from .data import load_run_events_batch, parse_asset_name

BACKFILL_PAGE_SIZE = 100
BACKFILL_RUN_LIMIT = 5_000


@dataclass(frozen=True, slots=True)
class BackfillSummary:
    """Summary information for one Dagster backfill."""

    id: str
    status: str
    title: str
    description: str
    created: str
    partitions: int | None
    is_asset_backfill: bool
    url: str


@dataclass(frozen=True, slots=True)
class BackfillAssetCounts:
    """Per-asset partition counts for a backfill."""

    asset: str
    targeted: int
    materialized: int
    failed: int
    in_progress: int
    remaining: int


@dataclass(frozen=True, slots=True)
class BackfillRunDetail:
    """Normalized asset-partition detail from a backfill run."""

    row_id: str
    asset: str
    instrument: str | None
    data_level: str | None
    descriptor: str
    partition: str
    partition_url: str
    status: str
    run_status: str
    run_id: str
    run_url: str


@dataclass(frozen=True, slots=True)
class BackfillDetail:
    """Backfill summary and its per-asset counts."""

    summary: BackfillSummary
    asset_counts: tuple[BackfillAssetCounts, ...]


class DagsterBackfillsDataSource:
    """Load backfill summaries, per-asset counts, and partition runs."""

    def list_backfills(self, statuses: Sequence[str]) -> list[BackfillSummary]:
        """List backfills matching the supplied statuses."""
        selected = [BulkActionStatus(status) for status in statuses]
        filters = BulkActionsFilter(statuses=selected or None)
        cursor: str | None = None
        summaries: list[BackfillSummary] = []
        with self._client() as client:
            while True:
                result = client.backfills(
                    limit=BACKFILL_PAGE_SIZE, cursor=cursor, filters=filters
                ).partition_backfills_or_error
                if not isinstance(result, BackfillListResult):
                    raise RuntimeError(result.message)
                summaries.extend(_backfill_summary(item) for item in result.results)
                next_cursor = result.results[-1].id if result.results else None
                if (
                    len(result.results) < BACKFILL_PAGE_SIZE
                    or not next_cursor
                    or next_cursor == cursor
                ):
                    break
                cursor = next_cursor
        return summaries

    def load_backfill(self, backfill_id: str) -> BackfillDetail:
        """Load one backfill and its per-asset counts."""
        with self._client() as client:
            result = client.backfill_details(
                backfill_id=backfill_id
            ).partition_backfill_or_error
        if not isinstance(result, BackfillResult):
            raise RuntimeError(result.message)
        return BackfillDetail(
            summary=_backfill_summary(result), asset_counts=tuple(_asset_counts(result))
        )

    def load_backfill_runs(self, backfill_id: str) -> tuple[BackfillRunDetail, ...]:
        """Load normalized asset-partition rows for a backfill's runs."""
        with self._client() as client:
            result = client.backfill_runs(
                backfill_id=backfill_id, run_limit=BACKFILL_RUN_LIMIT
            ).partition_backfill_or_error
            if not isinstance(result, BackfillRunsResult):
                raise RuntimeError(result.message)
            run_ids = [record.run_id for record in result.runs]
            events_by_run: dict[str, list[object]] = {}
            batch_size = run_event_batch_size()
            for start in range(0, len(run_ids), batch_size):
                events_by_run.update(
                    load_run_events_batch(client, run_ids[start : start + batch_size])
                )

        rows: list[BackfillRunDetail] = []
        for record in result.runs:
            partition = {tag.key: tag.value for tag in record.tags}.get(
                "dagster/partition", ""
            )
            for asset_key in record.asset_selection or []:
                asset = " / ".join(asset_key.path)
                instrument, data_level, descriptor = parse_asset_name(asset)
                route = "/".join(quote(segment, safe="") for segment in asset_key.path)
                partition_query = urlencode(
                    {"view": "partitions", "partition": partition}
                )
                rows.append(
                    BackfillRunDetail(
                        row_id=f"{record.run_id}\0{asset}",
                        asset=asset,
                        instrument=instrument,
                        data_level=data_level,
                        descriptor=descriptor,
                        partition=partition,
                        partition_url=(
                            f"{dagster_ui_url()}/assets/{route}?{partition_query}"
                        ),
                        status=_partition_status(
                            events_by_run.get(record.run_id, []),
                            tuple(asset_key.path),
                            record.status,
                        ),
                        run_status=record.status.value,
                        run_id=record.run_id,
                        run_url=(
                            f"{dagster_ui_url()}/runs/{quote(record.run_id, safe='')}"
                        ),
                    )
                )
        return tuple(rows)

    @staticmethod
    def _client() -> DagsterGraphQLClient:
        return create_dagster_client(url=graphql_url(), headers=client_headers())


def _backfill_summary(item: BackfillListItem | BackfillResult) -> BackfillSummary:
    backfill_id = str(item.id)
    title = str(getattr(item, "title", None) or "")
    return BackfillSummary(
        id=backfill_id,
        status=item.status.value,
        title=title or f"Backfill {backfill_id}",
        description=str(getattr(item, "description", None) or ""),
        created=datetime.fromtimestamp(float(item.creation_time), tz=UTC).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        ),
        partitions=getattr(item, "num_partitions", None),
        is_asset_backfill=bool(item.is_asset_backfill),
        url=f"{dagster_ui_url()}/runs/b/{quote(backfill_id, safe='')}",
    )


def _asset_counts(backfill: BackfillResult) -> list[BackfillAssetCounts]:
    if backfill.asset_backfill_data is None:
        return []
    result: list[BackfillAssetCounts] = []
    for status in backfill.asset_backfill_data.asset_backfill_statuses:
        asset = " / ".join(status.asset_key.path)
        if status.typename__ == "AssetPartitionsStatusCounts":
            targeted = status.num_partitions_targeted
            materialized = status.num_partitions_materialized
            failed = status.num_partitions_failed
            in_progress = status.num_partitions_in_progress
        else:
            targeted = 1
            materialized = int(status.materialized)
            failed = int(status.failed)
            in_progress = int(status.in_progress)
        result.append(
            BackfillAssetCounts(
                asset=asset,
                targeted=targeted,
                materialized=materialized,
                failed=failed,
                in_progress=in_progress,
                remaining=max(0, targeted - materialized - failed - in_progress),
            )
        )
    return sorted(result, key=lambda item: item.asset.casefold())


def _partition_status(
    events: list[object], asset_path: tuple[str, ...], run_status: RunStatus
) -> str:
    asset_events = [
        event
        for event in events
        if tuple(getattr(getattr(event, "asset_key", None), "path", ())) == asset_path
    ]
    if any(
        getattr(event, "typename__", "") == "MaterializationEvent"
        for event in asset_events
    ):
        return "materialized"
    if any(_is_skipped_event(event) for event in asset_events):
        return "skipped"
    if any(_is_log_skipped_event(event) for event in events):
        return "log-skipped"
    if run_status is RunStatus.FAILURE:
        return "failed"
    if run_status is RunStatus.SUCCESS:
        return "missing-output"
    return "in_progress"


def _is_skipped_event(event: object) -> bool:
    typename = getattr(event, "typename__", "")
    if typename == "FailedToMaterializeEvent":
        failure_type = getattr(event, "materialization_failure_type", None)
        return getattr(failure_type, "value", failure_type) == "SKIPPED"
    if typename != "ObservationEvent":
        return False
    return any(
        getattr(entry, "label", "") == "status"
        and str(getattr(entry, "text", "")).casefold().startswith("skipped")
        for entry in getattr(event, "metadata_entries", [])
    )


def _is_log_skipped_event(event: object) -> bool:
    if getattr(event, "typename__", "") != "LogMessageEvent":
        return False
    message = str(getattr(event, "message", "")).casefold()
    return (
        "submit response: skipped" in message
        and "job already completed or in progress" in message
    )
