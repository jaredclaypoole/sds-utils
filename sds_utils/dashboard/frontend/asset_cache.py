"""Persistent, incrementally refreshed cache for dashboard asset statuses."""

import hashlib
import json
import logging
import os
import threading
from datetime import UTC, datetime, timedelta
from typing import cast
from urllib.parse import quote, urlencode

from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from ..backend.api_utils import dagster_ui_url, graphql_url
from ..backend.data import (
    AssetOption,
    AssetStatusRow,
    DagsterAssetsDataSource,
    _ActivityRow,
    _overlapping_asset_partition_keys,
    _row_from_activity,
)
from .db import engine
from .models import (
    CachedAssetActivity,
    CachedAssetDefinition,
    CachedPartitionStatus,
    DagsterCacheCoverage,
    DagsterCacheNamespace,
)

logger = logging.getLogger("uvicorn.error.sds_utils.dashboard.asset_cache")
_SYNC_LOCKS: dict[str, threading.Lock] = {}
_SYNC_LOCKS_GUARD = threading.Lock()
_WATERMARK_OVERLAP = timedelta(minutes=5)


def cache_namespace() -> str:
    """Return the stable operator-controlled cache namespace."""
    configured = os.getenv("QUERY_DAGSTER_CACHE_NAMESPACE", "").strip()
    database_suffix = os.getenv("QUERY_APP_DB_SUFFIX", "").strip()
    return configured or database_suffix or "default"


def definition_ttl() -> timedelta:
    seconds = float(os.getenv("QUERY_DAGSTER_DEFINITION_CACHE_SECONDS", "300"))
    return timedelta(seconds=max(0, seconds))


def reconciliation_ttl() -> timedelta:
    seconds = float(os.getenv("QUERY_DAGSTER_CACHE_RECONCILE_SECONDS", "3600"))
    return timedelta(seconds=max(0, seconds))


class AssetStatusCache:
    """SQL persistence and projection for cached asset status rows."""

    def __init__(
        self,
        db_engine: Engine = engine,
        namespace: str | None = None,
    ) -> None:
        self.engine = db_engine
        self.namespace = namespace or cache_namespace()

    def load_assets(self) -> tuple[list[AssetOption], datetime | None]:
        with Session(self.engine) as session:
            namespace = self._namespace(session)
            records = session.exec(
                select(CachedAssetDefinition).where(
                    CachedAssetDefinition.namespace_id == namespace.id
                )
            ).all()
            return (
                [_asset_from_payload(record.payload) for record in records],
                _as_utc(namespace.definitions_refreshed_at),
            )

    def save_assets(self, assets: list[AssetOption]) -> None:
        with Session(self.engine) as session:
            namespace = self._namespace(session)
            existing = {
                record.asset_key: record
                for record in session.exec(
                    select(CachedAssetDefinition).where(
                        CachedAssetDefinition.namespace_id == namespace.id
                    )
                ).all()
            }
            for asset in assets:
                key = _asset_key(asset.path)
                record = existing.pop(key, None)
                if record is None:
                    record = CachedAssetDefinition(
                        namespace_id=_required_id(namespace),
                        asset_key=key,
                        payload={},
                    )
                    session.add(record)
                record.payload = _asset_payload(asset)
            for removed in existing.values():
                session.delete(removed)
            namespace.definitions_refreshed_at = datetime.now(UTC)
            namespace.updated_at = datetime.now(UTC)
            session.commit()

    def covers(
        self,
        start: datetime,
        end: datetime,
        *,
        include_activity: bool,
        include_partitions: bool,
    ) -> bool:
        return not self.missing_intervals(
            start,
            end,
            include_activity=include_activity,
            include_partitions=include_partitions,
        )

    def missing_intervals(
        self,
        start: datetime,
        end: datetime,
        *,
        include_activity: bool,
        include_partitions: bool,
    ) -> list[tuple[datetime, datetime]]:
        """Return gaps not covered by the union of compatible cache intervals."""
        with Session(self.engine) as session:
            namespace = self._namespace(session)
            coverage = session.exec(
                select(DagsterCacheCoverage).where(
                    DagsterCacheCoverage.namespace_id == namespace.id,
                    DagsterCacheCoverage.end >= start,
                    DagsterCacheCoverage.start <= end,
                )
            ).all()
        intervals = sorted(
            (
                max(start, _as_utc(item.start) or start),
                min(end, _as_utc(item.end) or end),
            )
            for item in coverage
            if (not include_activity or item.includes_activity)
            and (not include_partitions or item.includes_partitions)
        )
        gaps: list[tuple[datetime, datetime]] = []
        cursor = start
        for covered_start, covered_end in intervals:
            if covered_end <= cursor:
                continue
            if covered_start > cursor:
                gaps.append((cursor, covered_start))
            cursor = max(cursor, covered_end)
            if cursor >= end:
                break
        if cursor < end:
            gaps.append((cursor, end))
        return gaps

    def save_result(  # noqa: PLR0913
        self,
        rows: list[AssetStatusRow],
        *,
        start: datetime,
        end: datetime,
        include_activity: bool,
        include_partitions: bool,
        mark_coverage: bool,
        mark_reconciliation: bool = False,
        advance_watermark: bool = True,
    ) -> None:
        """Atomically upsert rows and advance successful synchronization state."""
        now = datetime.now(UTC)
        with Session(self.engine) as session:
            namespace = self._namespace(session)
            namespace_id = _required_id(namespace)
            statuses = {
                record.row_id: record
                for record in session.exec(
                    select(CachedPartitionStatus).where(
                        CachedPartitionStatus.namespace_id == namespace_id
                    )
                ).all()
            }
            activity_keys = set(
                session.exec(
                    select(CachedAssetActivity.activity_key).where(
                        CachedAssetActivity.namespace_id == namespace_id
                    )
                ).all()
            )
            for row in rows:
                update_time = _parse_row_time(row)
                status = statuses.get(row["row_id"])
                if status is None:
                    status = CachedPartitionStatus(
                        namespace_id=namespace_id,
                        row_id=row["row_id"],
                        payload={},
                    )
                    session.add(status)
                    statuses[row["row_id"]] = status
                saved_update_time = _as_utc(status.update_time)
                if (
                    status.update_time is None
                    or update_time is None
                    or (
                        saved_update_time is not None
                        and update_time >= saved_update_time
                    )
                ):
                    status.update_time = update_time
                    status.payload = dict(row)

                if update_time is not None and row["updated_in_window"]:
                    activity_key = _activity_key(row, update_time)
                    if activity_key not in activity_keys:
                        session.add(
                            CachedAssetActivity(
                                namespace_id=namespace_id,
                                activity_key=activity_key,
                                row_id=row["row_id"],
                                update_time=update_time,
                                payload=dict(row),
                            )
                        )
                        activity_keys.add(activity_key)

            if mark_coverage:
                session.add(
                    DagsterCacheCoverage(
                        namespace_id=namespace_id,
                        start=start,
                        end=end,
                        includes_activity=include_activity,
                        includes_partitions=include_partitions,
                    )
                )
            if mark_reconciliation:
                namespace.last_full_reconciliation_at = now
            if advance_watermark:
                watermark = _as_utc(namespace.activity_watermark)
                safe_end = min(end, now) if include_activity else now
                if watermark is None or safe_end > watermark:
                    namespace.activity_watermark = safe_end
            namespace.updated_at = now
            session.commit()

    def watermark(self) -> datetime | None:
        with Session(self.engine) as session:
            return _as_utc(self._namespace(session).activity_watermark)

    def reconciliation_due(self) -> bool:
        with Session(self.engine) as session:
            last_full = _as_utc(self._namespace(session).last_full_reconciliation_at)
        return last_full is None or (
            datetime.now(UTC) - last_full >= reconciliation_ttl()
        )

    def project(
        self,
        *,
        start: datetime,
        end: datetime,
        include_activity: bool,
        include_partitions: bool,
        assets: tuple[AssetOption, ...],
    ) -> list[AssetStatusRow]:
        asset_names = {asset.label for asset in assets}
        with Session(self.engine) as session:
            namespace = self._namespace(session)
            namespace_id = _required_id(namespace)
            current = session.exec(
                select(CachedPartitionStatus).where(
                    CachedPartitionStatus.namespace_id == namespace_id
                )
            ).all()
            activities = (
                session.exec(
                    select(CachedAssetActivity).where(
                        CachedAssetActivity.namespace_id == namespace_id,
                        CachedAssetActivity.update_time >= start,
                        CachedAssetActivity.update_time <= end,
                    )
                ).all()
                if include_activity
                else []
            )

        latest: dict[str, AssetStatusRow] = {}
        if include_activity:
            for record in activities:
                row = _row_from_payload(record.payload, updated_in_window=True)
                if row["asset"] in asset_names:
                    _keep_latest_row(latest, row)

        if include_partitions:
            status_by_row_id = {
                record.row_id: _row_from_payload(
                    record.payload, updated_in_window=False
                )
                for record in current
            }
            overlapping, _unique, _matching = _overlapping_asset_partition_keys(
                assets,
                window_start=start,
                window_end=end,
            )
            for asset_path, partition in overlapping:
                row_id = f"{' / '.join(asset_path)}\0{partition}"
                partition_row = status_by_row_id.get(row_id)
                if partition_row is None:
                    partition_row = _row_from_activity(
                        _ActivityRow(
                            asset_path=asset_path,
                            partition=partition,
                            timestamp="",
                            sort_timestamp=float("-inf"),
                            status="not-run",
                            updated_in_window=False,
                        ),
                        dagster_ui_url(),
                    )
                _keep_latest_row(latest, partition_row)
        return list(latest.values())

    def _namespace(self, session: Session) -> DagsterCacheNamespace:
        url = graphql_url()
        record = session.exec(
            select(DagsterCacheNamespace).where(
                DagsterCacheNamespace.name == self.namespace
            )
        ).first()
        if record is None:
            record = DagsterCacheNamespace(
                name=self.namespace,
                current_graphql_url=url,
            )
            session.add(record)
            session.commit()
            session.refresh(record)
        elif record.current_graphql_url != url:
            previous = list(record.previous_graphql_urls)
            if record.current_graphql_url not in previous:
                previous.append(record.current_graphql_url)
            logger.warning(
                "Dagster URL for cache namespace %r changed from %s to %s; "
                "retaining cached data",
                self.namespace,
                record.current_graphql_url,
                url,
            )
            record.previous_graphql_urls = previous
            record.current_graphql_url = url
            record.updated_at = datetime.now(UTC)
            session.commit()
            session.refresh(record)
        return record


class CachedDagsterAssetsDataSource:
    """Cache-first facade around the existing Dagster data source."""

    def __init__(
        self,
        source: DagsterAssetsDataSource | None = None,
        cache: AssetStatusCache | None = None,
    ) -> None:
        self.source = source or DagsterAssetsDataSource(
            identity_source=cache_namespace()
        )
        self.cache = cache or AssetStatusCache()

    def list_assets(self) -> list[AssetOption]:
        assets, refreshed_at = self.cache.load_assets()
        if (
            assets
            and refreshed_at
            and datetime.now(UTC) - refreshed_at < definition_ttl()
        ):
            return sorted(assets, key=lambda asset: asset.label.casefold())
        try:
            assets = self.source.list_assets()
            self.cache.save_assets(assets)
            return assets
        except Exception:
            if assets:
                logger.exception("Could not refresh asset definitions; using cache")
                return sorted(assets, key=lambda asset: asset.label.casefold())
            raise

    def load_recent_status_rows(
        self,
        *,
        start: datetime,
        end: datetime,
        include_recent_activity: bool,
        include_partition_ranges: bool,
        assets: tuple[AssetOption, ...],
    ) -> list[AssetStatusRow]:
        lock = _sync_lock(self.cache.namespace)
        with lock:
            cached_assets, _refreshed_at = self.cache.load_assets()
            sync_assets = tuple(cached_assets) or assets
            missing_intervals = self.cache.missing_intervals(
                start,
                end,
                include_activity=include_recent_activity,
                include_partitions=include_partition_ranges,
            )
            reconciliation_due = self.cache.reconciliation_due()
            if reconciliation_due:
                logger.info(
                    "Asset cache reconciliation due for namespace %r; "
                    "running authoritative load",
                    self.cache.namespace,
                )
                rows = self.source.load_recent_status_rows(
                    start=start,
                    end=end,
                    include_recent_activity=include_recent_activity,
                    include_partition_ranges=include_partition_ranges,
                    assets=sync_assets,
                )
                self.cache.save_result(
                    rows,
                    start=start,
                    end=end,
                    include_activity=include_recent_activity,
                    include_partitions=include_partition_ranges,
                    mark_coverage=True,
                    mark_reconciliation=True,
                )
                return self.cache.project(
                    start=start,
                    end=end,
                    include_activity=include_recent_activity,
                    include_partitions=include_partition_ranges,
                    assets=assets,
                )

            if missing_intervals:
                logger.info(
                    "Asset cache partial miss for namespace %r: "
                    "%d uncovered interval(s)",
                    self.cache.namespace,
                    len(missing_intervals),
                )
                for missing_start, missing_end in missing_intervals:
                    rows = self.source.load_recent_status_rows(
                        start=missing_start,
                        end=missing_end,
                        include_recent_activity=include_recent_activity,
                        include_partition_ranges=include_partition_ranges,
                        assets=sync_assets,
                    )
                    self.cache.save_result(
                        rows,
                        start=missing_start,
                        end=missing_end,
                        include_activity=include_recent_activity,
                        include_partitions=include_partition_ranges,
                        mark_coverage=True,
                        advance_watermark=False,
                    )

            logger.info("Asset cache hit for namespace %r", self.cache.namespace)
            watermark = self.cache.watermark()
            sync_end = datetime.now(UTC)
            if watermark is not None and sync_end > watermark:
                sync_start = watermark - _WATERMARK_OVERLAP
                try:
                    updates = self.source.load_recent_status_rows(
                        start=sync_start,
                        end=sync_end,
                        include_recent_activity=True,
                        include_partition_ranges=False,
                        assets=sync_assets,
                    )
                    self.cache.save_result(
                        updates,
                        start=sync_start,
                        end=sync_end,
                        include_activity=True,
                        include_partitions=False,
                        mark_coverage=False,
                    )
                except Exception:
                    logger.exception("Incremental Dagster refresh failed; using cache")
            return self.cache.project(
                start=start,
                end=end,
                include_activity=include_recent_activity,
                include_partitions=include_partition_ranges,
                assets=assets,
            )

    def load_cached_status_rows(
        self,
        *,
        start: datetime,
        end: datetime,
        include_recent_activity: bool,
        include_partition_ranges: bool,
        assets: tuple[AssetOption, ...],
    ) -> list[AssetStatusRow] | None:
        """Return an immediate cache hit without contacting Dagster."""
        if not self.cache.covers(
            start,
            end,
            include_activity=include_recent_activity,
            include_partitions=include_partition_ranges,
        ):
            return None
        return self.cache.project(
            start=start,
            end=end,
            include_activity=include_recent_activity,
            include_partitions=include_partition_ranges,
            assets=assets,
        )


def _sync_lock(namespace: str) -> threading.Lock:
    with _SYNC_LOCKS_GUARD:
        return _SYNC_LOCKS.setdefault(namespace, threading.Lock())


def _required_id(namespace: DagsterCacheNamespace) -> int:
    if namespace.id is None:
        raise RuntimeError("Cache namespace was not persisted")
    return namespace.id


def _asset_key(path: tuple[str, ...]) -> str:
    return json.dumps(path, separators=(",", ":"))


def _asset_payload(asset: AssetOption) -> dict[str, object]:
    return {
        "label": asset.label,
        "path": list(asset.path),
        "is_partitioned": asset.is_partitioned,
        "partition_keys": list(asset.partition_keys),
    }


def _asset_from_payload(payload: dict[str, object]) -> AssetOption:
    path = payload.get("path")
    partition_keys = payload.get("partition_keys")
    is_partitioned = payload.get("is_partitioned")
    if not isinstance(path, list) or not isinstance(partition_keys, list):
        raise ValueError("Invalid cached asset definition")
    if is_partitioned is not None and not isinstance(is_partitioned, bool):
        raise ValueError("Invalid cached partitioned flag")
    return AssetOption(
        label=str(payload["label"]),
        path=tuple(map(str, path)),
        is_partitioned=is_partitioned,
        partition_keys=tuple(map(str, partition_keys)),
    )


def _parse_row_time(row: AssetStatusRow) -> datetime | None:
    value = row["update_timestamp"]
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC)


def _activity_key(row: AssetStatusRow, update_time: datetime) -> str:
    identity = row["event_id"] or (
        f"{row['attempt_id'] or row['row_id']}|{row['status']}|"
        f"{update_time.isoformat()}"
    )
    return hashlib.sha256(identity.encode()).hexdigest()


def _row_from_payload(
    payload: dict[str, object], *, updated_in_window: bool
) -> AssetStatusRow:
    row = dict(payload)
    row["updated_in_window"] = updated_in_window
    _refresh_links(row)
    return cast(AssetStatusRow, row)


def _refresh_links(row: dict[str, object]) -> None:
    asset_path = tuple(str(row["asset"]).split(" / "))
    base = dagster_ui_url()
    route = "/".join(quote(segment, safe="") for segment in asset_path)
    asset_url = f"{base}/assets/{route}"
    partition = str(row.get("partition", ""))
    query = {"view": "partitions"}
    if partition:
        query["partition"] = partition
    row["asset_url"] = f"{asset_url}?view=overview"
    row["partition_url"] = f"{asset_url}?{urlencode(query)}"
    if partition:
        query["showAllEvents"] = "true"
    row["status_url"] = f"{asset_url}?{urlencode(query)}"


def _keep_latest_row(
    rows: dict[str, AssetStatusRow], candidate: AssetStatusRow
) -> None:
    existing = rows.get(candidate["row_id"])
    candidate_time = _parse_row_time(candidate)
    existing_time = _parse_row_time(existing) if existing else None
    if (
        existing is None
        or existing_time is None
        or (candidate_time is not None and candidate_time >= existing_time)
    ):
        rows[candidate["row_id"]] = candidate


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
