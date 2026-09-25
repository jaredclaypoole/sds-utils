"""Populate cached event details and derived facts for successful Dagster runs."""

import argparse
import asyncio
import datetime
import json
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.engine import Engine
from sqlmodel import Session, col, delete, select
from tqdm.auto import tqdm

from ..db import create_db_and_tables, engine
from ..db.models import (
    CachedDagsterRun,
    CachedRunEvent,
    DagsterCacheNamespace,
    DerivedJobRun,
)
from .graphql_api import DagsterGraphQLClient
from .graphql_api.fragments import (
    LegacySkipEventDetails,
    MaterializationEventDetails,
    MaterializationEventDetailsMetadataEntriesTextMetadataEntry,
    ObservationEventDetails,
    ObservationEventDetailsMetadataEntriesTextMetadataEntry,
    PlannedMaterializationEventDetails,
)

DEFAULT_BATCH_SIZE = 25
DEFAULT_EVENT_PAGE_SIZE = 500
DEFAULT_PAGINATION_CONCURRENCY = 5
RELEVANT_EVENT_TYPES = (
    "AssetMaterializationPlannedEvent",
    "MaterializationEvent",
    "ObservationEvent",
)
LEGACY_SKIP_EVENT_TYPE = "ExecutionStepSkippedEvent"


class RunDetailsError(RuntimeError):
    """Indicate that run details could not be fetched or derived safely."""


@dataclass(frozen=True)
class _RelevantEvent:
    run_id: str
    event_type: str
    timestamp: datetime.datetime
    step_key: str | None
    asset_path: tuple[str, ...] | None
    partition: str | None
    metadata: dict[str, str]
    payload: dict[str, object]
    skip_reason: str | None


def _event_timestamp(value: str) -> datetime.datetime:
    try:
        return datetime.datetime.fromtimestamp(float(value) / 1_000, tz=datetime.UTC)
    except ValueError:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
            datetime.UTC
        )


def _normalize_event(
    event: LegacySkipEventDetails
    | PlannedMaterializationEventDetails
    | MaterializationEventDetails
    | ObservationEventDetails,
) -> _RelevantEvent:
    if isinstance(event, PlannedMaterializationEventDetails):
        event_type = "AssetMaterializationPlannedEvent"
        asset_path = (
            tuple(event.asset_key.path) if event.asset_key is not None else None
        )
        partition = None
        metadata = {}
        skip_reason = None
    elif isinstance(event, LegacySkipEventDetails):
        event_type = LEGACY_SKIP_EVENT_TYPE
        asset_path = None
        partition = None
        skip_reason = event.message
        metadata = {"skip_reason": skip_reason}
    elif isinstance(event, MaterializationEventDetails):
        event_type = "MaterializationEvent"
        asset_path = (
            tuple(event.asset_key.path) if event.asset_key is not None else None
        )
        partition = event.partition
        metadata = {
            entry.label: entry.text
            for entry in event.metadata_entries
            if isinstance(
                entry,
                MaterializationEventDetailsMetadataEntriesTextMetadataEntry,
            )
        }
        skip_reason = None
    elif isinstance(event, ObservationEventDetails):
        event_type = "ObservationEvent"
        asset_path = (
            tuple(event.asset_key.path) if event.asset_key is not None else None
        )
        partition = event.partition
        metadata = {
            entry.label: entry.text
            for entry in event.metadata_entries
            if isinstance(
                entry,
                ObservationEventDetailsMetadataEntriesTextMetadataEntry,
            )
        }
        status = next(
            (value for key, value in metadata.items() if key.casefold() == "status"),
            "",
        )
        skip_reason = status if status.casefold().startswith("skipped") else None
    else:
        raise NotImplementedError(type(event))
    payload = event.model_dump(mode="json", by_alias=True)
    if skip_reason is not None:
        payload["skip_reason"] = skip_reason

    return _RelevantEvent(
        run_id=event.run_id,
        event_type=event_type,
        timestamp=_event_timestamp(event.timestamp),
        step_key=event.step_key,
        asset_path=asset_path,
        partition=partition,
        metadata=metadata,
        payload=payload,
        skip_reason=skip_reason,
    )


def _normalize_events(events: Sequence[BaseModel]) -> list[_RelevantEvent]:
    return [
        _normalize_event(event)
        for event in events
        if isinstance(
            event,
            (
                LegacySkipEventDetails,
                PlannedMaterializationEventDetails,
                MaterializationEventDetails,
                ObservationEventDetails,
            ),
        )
    ]


def _graphql_error(response: object) -> RunDetailsError:
    typename = getattr(response, "typename__", type(response).__name__)
    message = getattr(response, "message", typename)
    return RunDetailsError(f"Dagster returned {typename}: {message}")


async def _fetch_remaining_pages(
    client: DagsterGraphQLClient,
    *,
    run_id: str,
    cursor: str,
    event_page_size: int,
    semaphore: asyncio.Semaphore,
) -> list[_RelevantEvent]:
    events: list[_RelevantEvent] = []
    seen_cursors = {cursor}
    while True:
        async with semaphore:
            response = (
                await client.run_details_page(
                    run_id=run_id,
                    cursor=cursor,
                    event_limit=event_page_size,
                )
            ).run_or_error
        if getattr(response, "typename__", "") != "Run":
            raise _graphql_error(response)

        connection = response.event_connection
        events.extend(_normalize_events(connection.events))
        if not connection.has_more:
            return events
        if connection.cursor in seen_cursors:
            msg = f"Dagster returned a repeated event cursor for run {run_id}"
            raise RunDetailsError(msg)
        seen_cursors.add(connection.cursor)
        cursor = connection.cursor


async def _fetch_details_batch(
    client: DagsterGraphQLClient,
    *,
    run_ids: list[str],
    event_page_size: int,
    pagination_concurrency: int,
) -> dict[str, list[_RelevantEvent]]:
    response = (
        await client.run_details(run_ids=run_ids, event_limit=event_page_size)
    ).runs_or_error
    if getattr(response, "typename__", "") != "Runs":
        raise _graphql_error(response)

    events_by_run: dict[str, list[_RelevantEvent]] = {}
    pagination_requests: list[tuple[str, str]] = []
    for run in response.results:
        connection = run.event_connection
        events_by_run[run.run_id] = _normalize_events(connection.events)
        if connection.has_more:
            pagination_requests.append((run.run_id, connection.cursor))

    missing_run_ids = set(run_ids).difference(events_by_run)
    if missing_run_ids:
        missing = ", ".join(sorted(missing_run_ids))
        raise RunDetailsError(f"Dagster did not return requested runs: {missing}")

    semaphore = asyncio.Semaphore(pagination_concurrency)
    remaining_pages = await asyncio.gather(
        *(
            _fetch_remaining_pages(
                client,
                run_id=run_id,
                cursor=cursor,
                event_page_size=event_page_size,
                semaphore=semaphore,
            )
            for run_id, cursor in pagination_requests
        )
    )
    for (run_id, _), events in zip(
        pagination_requests,
        remaining_pages,
        strict=True,
    ):
        events_by_run[run_id].extend(events)
    return events_by_run


def _pending_successful_runs(
    session: Session,
    *,
    namespace_id: int,
    limit: int,
) -> list[CachedDagsterRun]:
    statement = (
        select(CachedDagsterRun)
        .outerjoin(
            DerivedJobRun,
            col(DerivedJobRun.cached_run_id) == CachedDagsterRun.id,
        )
        .where(
            CachedDagsterRun.namespace_id == namespace_id,
            CachedDagsterRun.dagster_status == "SUCCESS",
            col(DerivedJobRun.id).is_(None),
        )
        .order_by(col(CachedDagsterRun.creation_time).desc())
        .limit(limit)
    )
    return list(session.exec(statement))


def _pending_successful_run_count(session: Session, *, namespace_id: int) -> int:
    statement = (
        select(func.count())
        .select_from(CachedDagsterRun)
        .outerjoin(
            DerivedJobRun,
            col(DerivedJobRun.cached_run_id) == CachedDagsterRun.id,
        )
        .where(
            CachedDagsterRun.namespace_id == namespace_id,
            CachedDagsterRun.dagster_status == "SUCCESS",
            col(DerivedJobRun.id).is_(None),
        )
    )
    return session.exec(statement).one()


def _asset_key(asset_path: tuple[str, ...] | None) -> str | None:
    if asset_path is None:
        return None
    return json.dumps(asset_path, separators=(",", ":"))


def _explicit_skip_summary(
    events: list[_RelevantEvent],
    *,
    non_materialized_assets: set[tuple[str, ...]],
) -> tuple[int, dict[str, str] | None]:
    legacy_skips = [
        event for event in events if event.event_type == LEGACY_SKIP_EVENT_TYPE
    ]
    observation_skips = [
        event
        for event in events
        if event.event_type == "ObservationEvent"
        and event.skip_reason is not None
        and event.asset_path in non_materialized_assets
    ]

    if legacy_skips:
        n_skipped = len(non_materialized_assets)
        skip_payloads = [event.metadata for event in legacy_skips]
        skip_payloads.extend(event.metadata for event in observation_skips)
    else:
        n_skipped = len({event.asset_path for event in observation_skips})
        skip_payloads = [event.metadata for event in observation_skips]

    if not skip_payloads or any(
        payload != skip_payloads[0] for payload in skip_payloads[1:]
    ):
        return n_skipped, None
    return n_skipped, skip_payloads[0]


def _derive_run(
    cached_run: CachedDagsterRun,
    events: list[_RelevantEvent],
) -> DerivedJobRun:
    if cached_run.id is None:
        raise RunDetailsError(f"Cached run {cached_run.run_id} has no database ID")

    selected_assets = {tuple(path) for path in cached_run.selected_assets}
    materialized_assets = {
        event.asset_path
        for event in events
        if event.event_type == "MaterializationEvent" and event.asset_path is not None
    }
    observed_assets = {
        event.asset_path
        for event in events
        if event.event_type == "ObservationEvent" and event.asset_path is not None
    }
    planned_assets = {
        event.asset_path
        for event in events
        if event.event_type == "AssetMaterializationPlannedEvent"
        and event.asset_path is not None
    }
    expected_assets = (
        planned_assets or selected_assets or materialized_assets | observed_assets
    )
    n_expected = len(expected_assets)
    expected_materializations = materialized_assets & expected_assets
    non_materialized_assets = expected_assets - expected_materializations
    n_materialized = len(expected_materializations)
    n_skipped, skip_info = _explicit_skip_summary(
        events,
        non_materialized_assets=non_materialized_assets,
    )
    n_missing = len(non_materialized_assets) - n_skipped

    return DerivedJobRun(
        cached_run_id=cached_run.id,
        dashboard_status=(
            "missing"
            if n_missing > 0
            else "skipped"
            if n_skipped > 0
            else "materialized"
            if n_expected > 0 and n_materialized == n_expected
            else "not-found"
        ),
        n_expected=n_expected,
        n_materialized=n_materialized,
        n_skipped=n_skipped,
        n_missing=n_missing,
        skip_info=skip_info,
    )


def _store_details_batch(
    session: Session,
    *,
    namespace_id: int,
    runs: list[CachedDagsterRun],
    events_by_run: dict[str, list[_RelevantEvent]],
    run_stored: Callable[[], None] | None = None,
) -> None:
    run_ids = [run.run_id for run in runs]
    session.exec(
        delete(CachedRunEvent).where(
            col(CachedRunEvent.namespace_id) == namespace_id,
            col(CachedRunEvent.run_id).in_(run_ids),
            col(CachedRunEvent.event_type).in_(
                (*RELEVANT_EVENT_TYPES, LEGACY_SKIP_EVENT_TYPE)
            ),
        )
    )

    for cached_run in runs:
        events = events_by_run[cached_run.run_id]
        session.add(_derive_run(cached_run, events))
        for event in events:
            asset_key = _asset_key(event.asset_path)
            session.add(
                CachedRunEvent(
                    namespace_id=namespace_id,
                    event_key=CachedRunEvent.build_event_key(
                        run_id=event.run_id,
                        event_type=event.event_type,
                        timestamp=event.timestamp,
                        step_key=event.step_key,
                        asset_key=asset_key,
                        partition=event.partition or cached_run.partition,
                    ),
                    run_id=event.run_id,
                    event_type=event.event_type,
                    timestamp=event.timestamp,
                    step_key=event.step_key,
                    asset_key=asset_key,
                    partition=event.partition or cached_run.partition,
                    event_metadata=event.metadata,
                    payload=event.payload,
                )
            )
        if run_stored is not None:
            run_stored()
    session.commit()


async def ingest_run_details(  # noqa: PLR0913
    *,
    namespace_name: str = "default",
    api_key: str | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    event_page_size: int = DEFAULT_EVENT_PAGE_SIZE,
    pagination_concurrency: int = DEFAULT_PAGINATION_CONCURRENCY,
    show_progress: bool = False,
    db_engine: Engine = engine,
    client: DagsterGraphQLClient | None = None,
) -> int:
    """Derive and cache event details for successful runs not yet processed."""
    if batch_size <= 0 or event_page_size <= 0 or pagination_concurrency <= 0:
        raise ValueError("Batch, page, and concurrency sizes must be positive")

    with Session(db_engine) as session:
        namespace = session.exec(
            select(DagsterCacheNamespace).where(
                DagsterCacheNamespace.name == namespace_name
            )
        ).one_or_none()
        if namespace is None or namespace.id is None:
            raise RunDetailsError(f"Cache namespace {namespace_name!r} does not exist")
        namespace_id = namespace.id
        graphql_url = namespace.graphql_url
        pending_count = _pending_successful_run_count(
            session,
            namespace_id=namespace_id,
        )

    if pending_count == 0:
        return 0

    owns_client = client is None
    if client is None:
        api_key = api_key or os.environ["DAGSTER_API_KEY"]
        client = DagsterGraphQLClient(
            url=graphql_url,
            headers={"x-dagster-api-key": api_key},
        )

    processed_count = 0
    progress = tqdm(
        total=pending_count,
        desc="Ingesting run details",
        disable=not show_progress,
        unit="run",
    )
    try:
        while True:
            with Session(db_engine) as session:
                runs = _pending_successful_runs(
                    session,
                    namespace_id=namespace_id,
                    limit=batch_size,
                )
            if not runs:
                return processed_count

            events_by_run = await _fetch_details_batch(
                client,
                run_ids=[run.run_id for run in runs],
                event_page_size=event_page_size,
                pagination_concurrency=pagination_concurrency,
            )
            with Session(db_engine) as session:
                _store_details_batch(
                    session,
                    namespace_id=namespace_id,
                    runs=runs,
                    events_by_run=events_by_run,
                    run_stored=progress.update,
                )
                processed_count += len(runs)
    finally:
        progress.close()
        if owns_client:
            await client.http_client.aclose()


def main() -> None:
    """Ingest outstanding successful run details from the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="prod")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--event-page-size", type=int, default=DEFAULT_EVENT_PAGE_SIZE)
    args = parser.parse_args()
    create_db_and_tables()
    processed = asyncio.run(
        ingest_run_details(
            namespace_name=args.namespace,
            batch_size=args.batch_size,
            event_page_size=args.event_page_size,
            show_progress=True,
        )
    )
    print(f"Processed {processed} successful runs")


if __name__ == "__main__":
    main()
