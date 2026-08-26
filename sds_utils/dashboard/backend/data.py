"""Typed Dagster data access and view models for the asset-status UI."""

import logging
import re
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any, TypedDict
from urllib.parse import quote, urlencode

from sds_utils.dashboard.dagster_graphql_client import DagsterGraphQLClient
from sds_utils.dashboard.dagster_graphql_client.asset_partition_state import (
    AssetPartitionStateAssetNodeOrErrorAssetNode as AssetNode,
)
from sds_utils.dashboard.dagster_graphql_client.enums import (
    AssetMaterializationFailureType,
    RunStatus,
)
from sds_utils.dashboard.dagster_graphql_client.input_types import AssetKeyInput
from sds_utils.dashboard.dagster_graphql_client.partition_run_events import (
    PartitionRunEventsRunsOrErrorRuns as PartitionRunEvents,
)
from sds_utils.dashboard.dagster_graphql_client.partition_run_events import (
    PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunFailureEvent as BatchRunFailureEvent,
)
from sds_utils.dashboard.dagster_graphql_client.partition_runs import (
    PartitionRunsRunsOrErrorRuns as PartitionRuns,
)
from sds_utils.dashboard.dagster_graphql_client.recent_asset_activity import (
    RecentAssetActivityRunsOrErrorRuns as RecentRuns,
)
from sds_utils.dashboard.dagster_graphql_client.run_asset_activity import (
    RunAssetActivityRunOrErrorRun as ActivityRun,
)
from sds_utils.dashboard.dagster_graphql_client.run_asset_activity import (
    RunAssetActivityRunOrErrorRunEventConnectionEventsRunFailureEvent as RunFailureEvent,
)

from .api_utils import client_headers, dagster_ui_url, graphql_url
from .dagster_client import (
    asset_state_batch_size,
    create_dagster_client,
    native_status_mode,
    run_event_batch_size,
    run_event_workers,
    run_page_size,
)
from .event_identity import make_attempt_id, make_event_id, make_run_event_id
from .partition_classifier import (
    PartitionCategory,
    _native_partition_categories,
    get_native_partition_categories_batch,
)
from .partition_time import partition_overlaps_window

logger = logging.getLogger("uvicorn.error.sds_utils.dashboard.backend.data")

RUN_PAGE_SIZE = 100
EVENT_PAGE_SIZE = 1_000
ASSET_HISTORY_PAGE_SIZE = 100
ACTIVE_RUN_STATUSES = {
    RunStatus.QUEUED,
    RunStatus.NOT_STARTED,
    RunStatus.MANAGED,
    RunStatus.STARTING,
    RunStatus.STARTED,
    RunStatus.CANCELING,
}


class AssetStatusRow(TypedDict):
    """Serialized asset-partition status consumed by frontend tables."""

    row_id: str
    asset: str
    instrument: str | None
    data_level: str | None
    descriptor: str
    partition: str
    update_timestamp: str
    status: str
    skip_reason: str
    missing_file: str
    missing_files: str
    asset_url: str
    partition_url: str
    status_url: str
    updated_in_window: bool
    attempt_id: str | None
    event_id: str | None
    run_id: str | None
    event_type: str | None
    event_scope: str | None
    step_key: str | None
    tags: str
    notes: str


@dataclass(frozen=True, slots=True)
class AssetOption:
    """Dagster asset definition available for dashboard selection."""

    label: str
    path: tuple[str, ...]
    is_partitioned: bool | None
    partition_keys: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _ActivityRow:
    asset_path: tuple[str, ...]
    partition: str
    timestamp: str
    sort_timestamp: float
    status: str
    attempt_id: str | None = None
    event_id: str | None = None
    run_id: str | None = None
    event_type: str | None = None
    event_scope: str | None = None
    step_key: str | None = None
    updated_in_window: bool = True
    skip_reason: str = ""
    missing_files: str = ""


class DagsterAssetsDataSource:
    """Load asset information through the generated synchronous client."""

    def list_assets(self) -> list[AssetOption]:
        """List and normalize all Dagster asset definitions."""
        started = perf_counter()
        with self._client() as client:
            assets = {
                tuple(node.asset_key.path): AssetOption(
                    label=" / ".join(node.asset_key.path),
                    path=tuple(node.asset_key.path),
                    is_partitioned=node.is_partitioned,
                    partition_keys=tuple(node.partition_keys),
                )
                for node in client.all_asset_definitions().asset_nodes
            }
        result = sorted(
            assets.values(),
            key=lambda asset: asset.label.casefold(),
        )
        logger.info(
            "Dagster timing: asset definitions %.3fs (%d assets)",
            perf_counter() - started,
            len(result),
        )
        return result

    def latest_attempt_failed_after_previous_success(
        self,
        asset_path: Sequence[str],
        partition: str,
    ) -> bool:
        """Return whether the latest attempt failed after an earlier success.

        Dagster's native partition state determines whether the latest attempt
        failed, including run-level failures without a failed-to-materialize event.
        If it is failed, event history is paged until an earlier successful
        materialization is found.
        """
        cursor: str | None = None
        asset_key = AssetKeyInput(path=list(asset_path))
        with self._client() as client:
            state = client.asset_partition_state(
                asset_key=asset_key
            ).asset_node_or_error
            if not isinstance(state, AssetNode):
                raise ValueError(f"Unknown Dagster asset: {' / '.join(asset_path)}")
            if partition not in state.partition_keys:
                raise ValueError(
                    f"Unknown partition {partition!r} for asset "
                    f"{' / '.join(asset_path)}"
                )
            native_categories = _native_partition_categories(state)
            if partition not in native_categories[PartitionCategory.FAILED]:
                return False

            while True:
                result = client.asset_partition_history(
                    asset_key=asset_key,
                    partitions=[partition],
                    cursor=cursor,
                    limit=ASSET_HISTORY_PAGE_SIZE,
                ).asset_or_error
                if getattr(result, "typename__", "") != "Asset":
                    raise RuntimeError(getattr(result, "message", "Asset not found"))

                history = result.asset_event_history
                for event in history.results:
                    if event.typename__ == "MaterializationEvent":
                        return True

                next_cursor = history.cursor
                if not history.results or not next_cursor or next_cursor == cursor:
                    return False
                cursor = next_cursor

    def latest_attempt_failed_after_previous_success_targeted(
        self,
        asset_path: Sequence[str],
        partition: str,
    ) -> bool:
        """Use pair-oriented fields to detect a failure after an earlier success."""
        key = AssetKeyInput(path=list(asset_path))
        with self._client() as client:
            nodes = client.asset_partition_pair_states(
                asset_keys=[key],
                partitions=[partition],
                partition=partition,
            ).asset_nodes
        if not nodes:
            raise ValueError(f"Unknown Dagster asset: {' / '.join(asset_path)}")

        node = nodes[0]
        materialization = next(iter(node.latest_materialization_by_partition), None)
        latest_run = node.latest_run_for_partition
        return bool(
            materialization is not None
            and latest_run is not None
            and latest_run.status is RunStatus.FAILURE
            and latest_run.update_time is not None
            and latest_run.update_time
            >= _dagster_timestamp_sort_key(materialization.timestamp)
        )

    def load_recent_status_rows(
        self,
        *,
        start: datetime,
        end: datetime,
        include_recent_activity: bool,
        include_partition_ranges: bool,
        assets: tuple[AssetOption, ...],
    ) -> list[AssetStatusRow]:
        """Load recent partition activity without expanding every asset."""
        total_started = perf_counter()
        since = start.timestamp()
        until = end.timestamp()
        dagster_source = graphql_url()
        asset_paths = frozenset(asset.path for asset in assets)
        latest: dict[tuple[tuple[str, ...], str], _ActivityRow] = {}
        run_pages = 0
        runs = 0
        event_batches = 0
        partition_filter_started = perf_counter()
        overlapping_keys, unique_partition_count, matching_partition_count = (
            _overlapping_asset_partition_keys(
                assets,
                window_start=start,
                window_end=end,
            )
            if include_partition_ranges
            else (set(), 0, 0)
        )
        logger.info(
            "Dagster timing: partition-name filtering %.3fs "
            "(%d unique names, %d matching names, %d asset/partition pairs)",
            perf_counter() - partition_filter_started,
            unique_partition_count,
            matching_partition_count,
            len(overlapping_keys),
        )

        activity_started = perf_counter()
        with self._client() as client:
            cursor: str | None = None
            run_records: list[Any] = []
            page_size = run_page_size()
            while include_recent_activity:
                result = client.recent_asset_activity(
                    since=since,
                    until=until,
                    cursor=cursor,
                    limit=page_size,
                ).runs_or_error
                if not isinstance(result, RecentRuns):
                    raise RuntimeError(result.message)
                if not result.results:
                    break
                run_pages += 1
                runs += len(result.results)
                run_records.extend(result.results)

                next_cursor = result.results[-1].run_id
                if len(result.results) < page_size or next_cursor == cursor:
                    break
                cursor = next_cursor

            batch_size = run_event_batch_size()
            event_batches = (len(run_records) + batch_size - 1) // batch_size
            events_by_run = self._load_run_events_concurrently(
                [run_record.run_id for run_record in run_records],
                batch_size=batch_size,
            )
            for run_record in run_records:
                partition = next(
                    (
                        tag.value
                        for tag in run_record.tags
                        if tag.key == "dagster/partition"
                    ),
                    "",
                )
                events = events_by_run.get(run_record.run_id, [])
                terminal_keys: set[tuple[tuple[str, ...], str]] = set()
                for event in events:
                    activity = _activity_from_event(
                        event,
                        partition,
                        window_start=since,
                        window_end=until,
                        dagster_source=dagster_source,
                    )
                    if activity is not None:
                        terminal_keys.add((activity.asset_path, activity.partition))
                        _keep_latest(latest, activity, asset_paths)

                if (
                    run_record.status in ACTIVE_RUN_STATUSES
                    or run_record.status is RunStatus.FAILURE
                ):
                    planned_paths = {
                        tuple(event.asset_key.path)
                        for event in events
                        if event.typename__ == "AssetMaterializationPlannedEvent"
                        and event.asset_key is not None
                    }
                    selected_paths = {
                        tuple(selection.path)
                        for selection in run_record.asset_selection or []
                    }
                    for path in planned_paths | selected_paths:
                        if (path, partition) in terminal_keys:
                            continue
                        updated = run_record.update_time or since
                        inferred_status = (
                            "failed"
                            if run_record.status is RunStatus.FAILURE
                            else "materializing"
                        )
                        _keep_latest(
                            latest,
                            _ActivityRow(
                                asset_path=path,
                                partition=partition,
                                timestamp=_format_unix_seconds(updated),
                                sort_timestamp=updated,
                                status=inferred_status,
                                attempt_id=make_attempt_id(
                                    dagster_source=dagster_source,
                                    run_id=run_record.run_id,
                                    asset_path=path,
                                    partition=partition,
                                ),
                                run_id=run_record.run_id,
                                updated_in_window=since <= updated <= until,
                            ),
                            asset_paths,
                        )

            activity_elapsed = perf_counter() - activity_started

            range_rows = {
                key: _ActivityRow(
                    asset_path=key[0],
                    partition=key[1],
                    timestamp="",
                    sort_timestamp=float("-inf"),
                    status=PartitionCategory.NOT_RUN.value,
                    updated_in_window=False,
                )
                for key in overlapping_keys
            }
            not_run_count = len(overlapping_keys - set(latest))

            native_started = perf_counter()
            recent_native_keys = set(latest) & overlapping_keys
            if end >= datetime.now(UTC) - timedelta(minutes=5):
                recent_native_keys.update(
                    key
                    for key, row in latest.items()
                    if row.event_scope is None
                    and row.status
                    in {
                        PartitionCategory.FAILED.value,
                        PartitionCategory.MATERIALIZING.value,
                    }
                )
            native_target_keys = recent_native_keys | overlapping_keys
            status_mode = native_status_mode()
            native_by_asset = (
                self.load_pair_native_statuses(native_target_keys)
                if status_mode == "pair"
                else _load_native_statuses(client, native_target_keys)
            )
            _apply_native_statuses(
                latest,
                recent_native_keys,
                native_by_asset,
            )
            _apply_native_statuses(
                range_rows,
                overlapping_keys,
                native_by_asset,
            )
            native_elapsed = perf_counter() - native_started

            detail_started = perf_counter()
            detail_target_count = sum(
                row.status
                in {
                    PartitionCategory.FAILED.value,
                    PartitionCategory.NOT_RUN.value,
                }
                for row in range_rows.values()
            )
            if range_rows:
                _reconcile_partition_runs(
                    client,
                    range_rows,
                    overlapping_keys,
                    window_start=since,
                    window_end=until,
                    dagster_source=dagster_source,
                )
            detail_elapsed = perf_counter() - detail_started

            merge_started = perf_counter()
            recent_row_count = len(latest)
            for range_row in range_rows.values():
                _keep_latest(latest, range_row, asset_paths)
            merge_elapsed = perf_counter() - merge_started

        logger.info(
            "Dagster timing: recent activity %.3fs "
            "(%d run pages, %d runs, %d concurrent event batches, %d active rows)",
            activity_elapsed,
            run_pages,
            runs,
            event_batches,
            recent_row_count,
        )
        logger.info(
            "Dagster timing: native status classification %.3fs "
            "(mode=%s, %d assets, %d recent keys, %d partition-range keys)",
            native_elapsed,
            status_mode,
            len({asset_path for asset_path, _partition in native_target_keys}),
            len(recent_native_keys),
            len(overlapping_keys),
        )
        logger.info(
            "Dagster timing: partition detail resolution %.3fs "
            "(%d rows requiring failed/not-run history)",
            detail_elapsed,
            detail_target_count,
        )
        logger.info(
            "Dagster timing: branch merge %.3fs "
            "(%d recent rows, %d partition-range rows, %d merged rows)",
            merge_elapsed,
            recent_row_count,
            len(range_rows),
            len(latest),
        )

        serialization_started = perf_counter()
        ui_url = dagster_ui_url()
        rows = [_row_from_activity(row, ui_url) for row in latest.values()]
        serialization_elapsed = perf_counter() - serialization_started
        logger.info(
            "Dagster timing: in-range not-run synthesis (%d rows added)",
            not_run_count,
        )
        logger.info(
            "Dagster timing: row serialization %.3fs; total data load %.3fs (%d rows)",
            serialization_elapsed,
            perf_counter() - total_started,
            len(rows),
        )
        return rows

    @staticmethod
    def _client() -> DagsterGraphQLClient:
        return create_dagster_client(url=graphql_url(), headers=client_headers())

    def _load_run_events_concurrently(
        self,
        run_ids: list[str],
        *,
        batch_size: int,
    ) -> dict[str, list[Any]]:
        batches = [
            run_ids[index : index + batch_size]
            for index in range(0, len(run_ids), batch_size)
        ]
        if not batches:
            return {}

        def load_batch(batch: list[str]) -> dict[str, list[object]]:
            with self._client() as batch_client:
                return load_run_events_batch(batch_client, batch)

        events_by_run: dict[str, list[object]] = {}
        with ThreadPoolExecutor(
            max_workers=min(run_event_workers(), len(batches)),
            thread_name_prefix="dagster-events",
        ) as executor:
            for batch_result in executor.map(load_batch, batches):
                events_by_run.update(batch_result)
        return events_by_run

    def load_pair_native_statuses(
        self,
        keys: set[tuple[tuple[str, ...], str]],
    ) -> dict[tuple[str, ...], dict[PartitionCategory, set[str]]]:
        """Reconstruct native-like statuses using pair-oriented GraphQL fields."""
        assets_by_partition: dict[str, list[tuple[str, ...]]] = {}
        for asset_path, partition in keys:
            assets_by_partition.setdefault(partition, []).append(asset_path)

        batch_size = asset_state_batch_size()
        tasks = [
            (partition, asset_paths[index : index + batch_size])
            for partition, asset_paths in assets_by_partition.items()
            for index in range(0, len(asset_paths), batch_size)
        ]
        result = {
            asset_path: {
                PartitionCategory.MATERIALIZED: set[str](),
                PartitionCategory.MATERIALIZING: set[str](),
                PartitionCategory.FAILED: set[str](),
            }
            for asset_path, _partition in keys
        }
        if not tasks:
            return result

        def load_task(
            task: tuple[str, list[tuple[str, ...]]],
        ) -> list[tuple[tuple[str, ...], str, PartitionCategory | None]]:
            partition, asset_paths = task
            with self._client() as pair_client:
                nodes = pair_client.asset_partition_pair_states(
                    asset_keys=[
                        AssetKeyInput(path=list(asset_path))
                        for asset_path in asset_paths
                    ],
                    partitions=[partition],
                    partition=partition,
                ).asset_nodes
            states: list[tuple[tuple[str, ...], str, PartitionCategory | None]] = []
            for node in nodes:
                materialization = next(
                    iter(node.latest_materialization_by_partition),
                    None,
                )
                materialization_time = (
                    _dagster_timestamp_sort_key(materialization.timestamp)
                    if materialization is not None
                    else float("-inf")
                )
                latest_run = node.latest_run_for_partition
                run_time = (
                    latest_run.update_time
                    if latest_run is not None and latest_run.update_time is not None
                    else float("-inf")
                )
                category: PartitionCategory | None = None
                if (
                    latest_run is not None
                    and run_time >= materialization_time
                    and latest_run.status in ACTIVE_RUN_STATUSES
                ):
                    category = PartitionCategory.MATERIALIZING
                elif (
                    latest_run is not None
                    and run_time >= materialization_time
                    and latest_run.status is RunStatus.FAILURE
                ):
                    category = PartitionCategory.FAILED
                elif materialization is not None:
                    category = PartitionCategory.MATERIALIZED
                states.append((tuple(node.asset_key.path), partition, category))
            return states

        with ThreadPoolExecutor(
            max_workers=min(run_event_workers(), len(tasks)),
            thread_name_prefix="dagster-pair-status",
        ) as executor:
            for states in executor.map(load_task, tasks):
                for asset_path, partition, category in states:
                    if category is not None:
                        result[asset_path][category].add(partition)
        return result


def _overlapping_asset_partition_keys(
    assets: tuple[AssetOption, ...],
    *,
    window_start: datetime,
    window_end: datetime,
) -> tuple[set[tuple[tuple[str, ...], str]], int, int]:
    """Parse each unique partition name once, retaining its actual asset owners."""
    owners: dict[str, list[tuple[str, ...]]] = {}
    for asset in assets:
        for partition in asset.partition_keys:
            owners.setdefault(partition, []).append(asset.path)

    matching_names = {
        partition
        for partition in owners
        if partition_overlaps_window(
            partition,
            window_start=window_start,
            window_end=window_end,
        )
    }
    keys = {
        (asset_path, partition)
        for partition in matching_names
        for asset_path in owners[partition]
    }
    return (
        keys,  # set of (asset_path, partition) pairs
        len(owners),  # count of unique partition names
        len(matching_names),  # count of matching partition names
    )


def _format_dagster_timestamp(value: str) -> str:
    """Format Dagster's millisecond event timestamp as UTC ISO-8601."""
    try:
        numeric = float(value)
    except ValueError:
        return value
    return datetime.fromtimestamp(numeric / 1_000, tz=UTC).isoformat(timespec="seconds")


def _format_unix_seconds(value: float) -> str:
    return datetime.fromtimestamp(value, tz=UTC).isoformat(timespec="seconds")


def _activity_from_event(
    event: Any,
    run_partition: str,
    *,
    window_start: float,
    window_end: float,
    dagster_source: str,
) -> _ActivityRow | None:
    typename = getattr(event, "typename__", "")
    if typename not in {
        "MaterializationEvent",
        "FailedToMaterializeEvent",
        "ObservationEvent",
    }:
        return None
    asset_key = getattr(event, "asset_key", None)
    if asset_key is None:
        return None

    timestamp = event.timestamp
    partition = event.partition or run_partition
    status = "materialized"
    reason = ""
    missing_files = ""
    attempt_id = make_attempt_id(
        dagster_source=dagster_source,
        run_id=event.run_id,
        asset_path=asset_key.path,
        partition=partition,
    )

    if typename == "FailedToMaterializeEvent":
        status = (
            "skipped"
            if event.materialization_failure_type
            is AssetMaterializationFailureType.SKIPPED
            else "failed"
        )
        reason = (
            event.materialization_failure_reason.value if status == "skipped" else ""
        )
        metadata = _text_metadata(event.metadata_entries)
        missing_files = metadata.get("missing_files", "") if status == "skipped" else ""
    elif typename == "ObservationEvent":
        metadata = _text_metadata(event.metadata_entries)
        observation_status = metadata.get("status", "")
        if not observation_status.casefold().startswith("skipped"):
            return None
        status = "skipped"
        reason = observation_status
        missing_files = metadata.get("missing_files", "")

    return _ActivityRow(
        asset_path=tuple(asset_key.path),
        partition=partition,
        timestamp=_format_dagster_timestamp(timestamp),
        sort_timestamp=float(timestamp) / 1_000,
        status=status,
        attempt_id=attempt_id,
        event_id=make_event_id(
            attempt_id=attempt_id,
            event_type=typename,
            timestamp=timestamp,
            step_key=event.step_key,
        ),
        run_id=event.run_id,
        event_type=typename,
        event_scope="asset",
        step_key=event.step_key,
        updated_in_window=window_start <= float(timestamp) / 1_000 <= window_end,
        skip_reason=reason,
        missing_files=missing_files,
    )


def _text_metadata(entries: list[Any]) -> dict[str, str]:
    return {entry.label: entry.text for entry in entries if hasattr(entry, "text")}


def parse_missing_file(value: str) -> str:
    """Extract the primary missing dependency from known skip messages."""
    dependency = re.match(
        r"^Not\s+enough\s+information\s+to\s+process\.\s+"
        r"Missing\s+([A-Za-z0-9_]+)",
        value,
        flags=re.IGNORECASE,
    )
    if dependency is not None:
        return dependency.group(1)
    if re.match(r"^Missing\s+SPICE\s+files\b", value, flags=re.IGNORECASE):
        return "SPICE"
    return ""


def parse_asset_name(asset: str) -> tuple[str | None, str | None, str]:
    """Parse ``instrument_datalevel_descriptor`` asset names."""
    parts = asset.split("_", 2)
    if len(parts) != len(("instrument", "data_level", "descriptor")) or not all(parts):
        return None, None, asset
    return parts[0], parts[1], parts[2]


def _keep_latest(
    rows: dict[tuple[tuple[str, ...], str], _ActivityRow],
    candidate: _ActivityRow,
    asset_paths: frozenset[tuple[str, ...]] | None,
) -> None:
    if asset_paths is not None and candidate.asset_path not in asset_paths:
        return
    key = (candidate.asset_path, candidate.partition)
    existing = rows.get(key)
    failed_beats_in_flight = (
        candidate.status == "failed"
        and existing is not None
        and existing.status == "materializing"
    )
    in_flight_does_not_beat_failed = (
        candidate.status == "materializing"
        and existing is not None
        and existing.status == "failed"
    )
    if (
        existing is None
        or failed_beats_in_flight
        or (
            not in_flight_does_not_beat_failed
            and candidate.sort_timestamp >= existing.sort_timestamp
        )
    ):
        rows[key] = candidate


def _row_from_activity(activity: _ActivityRow, ui_base_url: str) -> AssetStatusRow:
    asset = " / ".join(activity.asset_path)
    instrument, data_level, descriptor = parse_asset_name(asset)
    asset_route = "/".join(quote(segment, safe="") for segment in activity.asset_path)
    asset_url = f"{ui_base_url}/assets/{asset_route}"
    partition_query = {"view": "partitions"}
    if activity.partition:
        partition_query["partition"] = activity.partition
    partition_url = f"{asset_url}?{urlencode(partition_query)}"
    status_query = dict(partition_query)
    if activity.partition:
        status_query["showAllEvents"] = "true"
    return {
        "row_id": f"{asset}\0{activity.partition}",
        "asset": asset,
        "instrument": instrument,
        "data_level": data_level,
        "descriptor": descriptor,
        "partition": activity.partition,
        "update_timestamp": activity.timestamp,
        "status": activity.status,
        "skip_reason": activity.skip_reason,
        "missing_file": (
            parse_missing_file(activity.missing_files)
            if activity.status == PartitionCategory.SKIPPED.value
            else ""
        ),
        "missing_files": activity.missing_files,
        "asset_url": f"{asset_url}?view=overview",
        "partition_url": partition_url,
        "status_url": f"{asset_url}?{urlencode(status_query)}",
        "updated_in_window": activity.updated_in_window,
        "attempt_id": activity.attempt_id,
        "event_id": activity.event_id,
        "run_id": activity.run_id,
        "event_type": activity.event_type,
        "event_scope": activity.event_scope,
        "step_key": activity.step_key,
        "tags": "",
        "notes": "",
    }


def _load_native_statuses(
    client: DagsterGraphQLClient,
    keys: set[tuple[tuple[str, ...], str]],
) -> dict[tuple[str, ...], dict[PartitionCategory, set[str]]]:
    """Load native statuses once for both query branches."""
    if not keys:
        return {}
    return get_native_partition_categories_batch(
        client,
        {asset_path for asset_path, _partition in keys},
        batch_size=asset_state_batch_size(),
    )


def _apply_native_statuses(
    rows: dict[tuple[tuple[str, ...], str], _ActivityRow],
    target_keys: set[tuple[tuple[str, ...], str]],
    native_by_asset: dict[tuple[str, ...], dict[PartitionCategory, set[str]]],
) -> None:
    """Replace event heuristics with already-loaded native partition statuses."""
    for asset_path, native in native_by_asset.items():
        status_by_partition = {
            partition: category.value
            for category in (
                PartitionCategory.MATERIALIZED,
                PartitionCategory.MATERIALIZING,
                PartitionCategory.FAILED,
            )
            for partition in native[category]
        }
        for key, row in tuple(rows.items()):
            if key not in target_keys or row.asset_path != asset_path:
                continue
            native_status = status_by_partition.get(row.partition)
            if native_status is None:
                if row.event_scope is None and row.status in {
                    PartitionCategory.FAILED.value,
                    PartitionCategory.MATERIALIZING.value,
                }:
                    # Recent run activity only tells us that this asset was
                    # selected/planned in a failed or active run.  When Dagster's
                    # native partition state still says unmaterialized, retract
                    # that run-level inference.  Explicit asset events have an
                    # event_scope and remain authoritative.
                    rows[key] = _ActivityRow(
                        asset_path=row.asset_path,
                        partition=row.partition,
                        timestamp="",
                        sort_timestamp=float("-inf"),
                        status=PartitionCategory.NOT_RUN.value,
                        updated_in_window=False,
                    )
                continue
            if native_status == row.status:
                continue
            rows[key] = _ActivityRow(
                asset_path=row.asset_path,
                partition=row.partition,
                timestamp=row.timestamp,
                sort_timestamp=row.sort_timestamp,
                status=native_status,
                attempt_id=row.attempt_id,
                event_id=row.event_id,
                run_id=row.run_id,
                event_type=row.event_type,
                event_scope=row.event_scope,
                step_key=row.step_key,
                updated_in_window=row.updated_in_window,
                skip_reason="",
                missing_files="",
            )


def _reconcile_partition_runs(
    client: DagsterGraphQLClient,
    rows: dict[tuple[tuple[str, ...], str], _ActivityRow],
    keys: set[tuple[tuple[str, ...], str]],
    *,
    window_start: float,
    window_end: float,
    dagster_source: str,
) -> None:
    """Resolve skipped and failed rows once per unique partition name."""
    target_keys = {
        key
        for key in keys
        if rows[key].status
        in {PartitionCategory.NOT_RUN.value, PartitionCategory.FAILED.value}
    }
    targets_by_partition: dict[str, set[tuple[tuple[str, ...], str]]] = {}
    for key in target_keys:
        targets_by_partition.setdefault(key[1], set()).add(key)

    for partition, unresolved in targets_by_partition.items():
        cursor: str | None = None
        while unresolved:
            result = client.partition_runs(
                partition=partition,
                cursor=cursor,
                limit=RUN_PAGE_SIZE,
            ).runs_or_error
            if not isinstance(result, PartitionRuns):
                raise RuntimeError(result.message)
            events_by_run = load_run_events_batch(
                client,
                [run_record.run_id for run_record in result.results],
            )
            for run_record in result.results:
                events = events_by_run.get(run_record.run_id, [])
                terminal_keys: set[tuple[tuple[str, ...], str]] = set()
                for event in events:
                    activity = _activity_from_event(
                        event,
                        partition,
                        window_start=window_start,
                        window_end=window_end,
                        dagster_source=dagster_source,
                    )
                    if activity is None:
                        continue
                    key = (activity.asset_path, activity.partition)
                    if key not in unresolved:
                        continue
                    terminal_keys.add(key)
                    _keep_latest(rows, activity, frozenset({activity.asset_path}))

                if run_record.status is RunStatus.FAILURE:
                    planned_paths = {
                        tuple(asset_key.path)
                        for event in events
                        if getattr(event, "typename__", "")
                        == "AssetMaterializationPlannedEvent"
                        and (asset_key := getattr(event, "asset_key", None)) is not None
                    }
                    selected_paths = {
                        tuple(selection.path)
                        for selection in run_record.asset_selection or []
                    }
                    failure = _latest_failure_in_events(events)
                    for asset_path in planned_paths | selected_paths:
                        key = (asset_path, partition)
                        if key not in unresolved or key in terminal_keys:
                            continue
                        # A failed run can plan many assets without any particular
                        # asset being the cause of (or even reached by) the failure.
                        # Only use the run-level failure to attach identity/details
                        # to a partition Dagster already classified as failed.  An
                        # explicit per-asset terminal event is handled above and may
                        # still promote a not-run row independently.
                        if rows[key].status != PartitionCategory.FAILED.value:
                            continue
                        updated = run_record.update_time or window_start
                        attempt_id = make_attempt_id(
                            dagster_source=dagster_source,
                            run_id=run_record.run_id,
                            asset_path=asset_path,
                            partition=partition,
                        )
                        rows[key] = _ActivityRow(
                            asset_path=asset_path,
                            partition=partition,
                            timestamp=(
                                _format_dagster_timestamp(failure.timestamp)
                                if failure is not None
                                else _format_unix_seconds(updated)
                            ),
                            sort_timestamp=(
                                _dagster_timestamp_sort_key(failure.timestamp)
                                if failure is not None
                                else updated
                            ),
                            status=PartitionCategory.FAILED.value,
                            attempt_id=attempt_id,
                            event_id=(
                                make_run_event_id(
                                    dagster_source=dagster_source,
                                    run_id=run_record.run_id,
                                    event_type=failure.typename__,
                                    timestamp=failure.timestamp,
                                    step_key=failure.step_key,
                                )
                                if failure is not None
                                else None
                            ),
                            run_id=run_record.run_id,
                            event_type=(failure.typename__ if failure else None),
                            event_scope=("run" if failure else None),
                            step_key=(failure.step_key if failure else None),
                            updated_in_window=window_start <= updated <= window_end,
                        )

                unresolved.difference_update(
                    {
                        key
                        for key in unresolved
                        if rows[key].event_id is not None
                        or rows[key].attempt_id is not None
                    }
                )
                if not unresolved:
                    break

            next_cursor = result.results[-1].run_id if result.results else None
            if (
                not unresolved
                or len(result.results) < RUN_PAGE_SIZE
                or not next_cursor
                or next_cursor == cursor
            ):
                break
            cursor = next_cursor

    for key in target_keys:
        if (
            rows[key].status == PartitionCategory.FAILED.value
            and rows[key].run_id is None
        ):
            rows[key] = replace(rows[key], status=PartitionCategory.NOT_FOUND.value)


def load_run_events_batch(
    client: DagsterGraphQLClient,
    run_ids: list[str],
) -> dict[str, list[object]]:
    """Load event histories for a batch of Dagster run IDs."""
    if not run_ids:
        return {}
    result = client.partition_run_events(
        run_ids=run_ids,
        event_limit=EVENT_PAGE_SIZE,
    ).runs_or_error
    if not isinstance(result, PartitionRunEvents):
        raise RuntimeError(result.message)
    events_by_run: dict[str, list[Any]] = {}
    for run_record in result.results:
        connection: Any = run_record.event_connection
        events: list[Any] = list(connection.events)
        cursor = connection.cursor
        while connection.has_more and cursor:
            continuation = client.run_asset_activity(
                run_id=run_record.run_id,
                cursor=cursor,
                limit=EVENT_PAGE_SIZE,
            ).run_or_error
            if not isinstance(continuation, ActivityRun):
                raise RuntimeError(continuation.message)
            connection = continuation.event_connection
            events.extend(connection.events)
            if connection.cursor == cursor:
                break
            cursor = connection.cursor
        events_by_run[run_record.run_id] = events
    return events_by_run


def _latest_failure_in_events(
    events: list[Any],
) -> RunFailureEvent | BatchRunFailureEvent | None:
    failures = [
        event
        for event in events
        if isinstance(event, (RunFailureEvent, BatchRunFailureEvent))
    ]
    return max(
        failures,
        key=lambda event: _dagster_timestamp_sort_key(event.timestamp),
        default=None,
    )


def _dagster_timestamp_sort_key(value: str) -> float:
    try:
        return float(value) / 1_000
    except ValueError:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
