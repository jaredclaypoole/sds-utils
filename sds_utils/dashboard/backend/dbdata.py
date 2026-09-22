"""Database-backed dataframe source for the dashboard."""

import datetime
import logging

import pandas as pd
from sqlalchemy import Engine
from sqlmodel import Session, col, select

from .data import DataSourceBase, QuerySpec
from .db.models import (
    CachedDagsterRun,
    CachedRunEvent,
    DagsterCacheNamespace,
    DerivedJobRun,
)

logger = logging.getLogger(__name__)

_DATETIME_COLUMNS = (
    "creation_time",
    "update_time",
    "start_time",
    "end_time",
    "run_start_time",
    "run_end_time",
    "start_date",
    "end_date",
)
_COUNT_COLUMNS = (
    "n_expected",
    "n_materialized",
    "n_skipped",
    "n_explicitly_skipped",
)
_JOB_NAME_PATTERN = (
    r"^(?P<instrument>[^_]+)_"
    r"(?P<data_level>[^_]+)_"
    r"(?P<descriptor>[^_]+)_.+$"
)
_STEP_KEY_PATTERN = (
    r"^(?P<instrument>[^_]+)_"
    r"(?P<data_level>[^_]+)_"
    r"(?P<descriptor>[^_]+)(?:_.+)?$"
)
_PARTITION_PATTERN = (
    r"^(?P<partition_spec>.+)_"
    r"(?P<start_time>\d{4}-\d{2}-\d{2}T[^_]+)_to_"
    r"(?P<end_time>\d{4}-\d{2}-\d{2}T.+)$"
)
_DAGSTER_STATUS_MAP = {
    "QUEUED": "materializing",
    "NOT_STARTED": "materializing",
    "MANAGED": "materializing",
    "STARTING": "materializing",
    "STARTED": "materializing",
    "SUCCESS": "unknown",
    "FAILURE": "failed",
    "CANCELING": "canceling",
    "CANCELED": "canceled",
}
_COLUMNS = (
    "run_id",
    "instrument",
    "data_level",
    "descriptor",
    "partition",
    "partition_label",
    "repoint",
    "job_name",
    "status",
    "dagster_status",
    "creation_time",
    "update_time",
    "start_time",
    "end_time",
    "run_start_time",
    "run_end_time",
    "start_date",
    "end_date",
    "duration_seconds",
    "parent_run_id",
    "root_run_id",
    "selected_assets",
    "tags",
    *_COUNT_COLUMNS,
    "skip_info",
    "skip_reason",
    "missing_files",
)


def _utc_naive(value: datetime.datetime) -> datetime.datetime:
    """Represent an input timestamp as naive UTC for SQLite comparisons."""
    if value.tzinfo is None or value.utcoffset() is None:
        return value
    return value.astimezone(datetime.UTC).replace(tzinfo=None)


def _status(run: CachedDagsterRun, derived: DerivedJobRun | None) -> str:
    if derived is not None:
        return derived.dashboard_status
    status = _DAGSTER_STATUS_MAP.get(run.dagster_status)
    if status is None:
        logger.warning(
            "Unknown Dagster run status %r for run %s; marking it unknown",
            run.dagster_status,
            run.run_id,
        )
        return "unknown"
    return status


def _skip_columns(
    derived: DerivedJobRun | None,
) -> tuple[dict[str, str] | None, str | None, str | None]:
    skip_info = derived.skip_info if derived is not None else None
    if skip_info is None:
        return None, None, None
    return (
        skip_info,
        skip_info.get("status") or skip_info.get("skip_reason"),
        skip_info.get("missing_files"),
    )


class DBDataSource(DataSourceBase):
    """Load cached and derived Dagster runs into dashboard dataframes."""

    def __init__(self, engine: Engine, dagster_namespace: str):
        self.engine = engine
        self.namespace = dagster_namespace

    def query(self, query: QuerySpec) -> pd.DataFrame:
        """Return dashboard-ready runs updated within the requested UTC window."""
        start_time = _utc_naive(query.start_time)
        end_time = _utc_naive(query.end_time)
        if start_time > end_time:
            raise ValueError("Query start_time must not be later than end_time")

        statement = (
            select(CachedDagsterRun, DerivedJobRun)
            .join(
                DagsterCacheNamespace,
                col(CachedDagsterRun.namespace_id) == DagsterCacheNamespace.id,
            )
            .outerjoin(
                DerivedJobRun,
                col(DerivedJobRun.cached_run_id) == CachedDagsterRun.id,
            )
            .where(
                DagsterCacheNamespace.name == self.namespace,
                col(CachedDagsterRun.update_time) >= start_time,
                col(CachedDagsterRun.update_time) <= end_time,
            )
            .order_by(col(CachedDagsterRun.update_time).desc())
        )
        with Session(self.engine) as session:
            results = list(session.exec(statement))
            planned_steps = list(
                session.exec(
                    select(CachedRunEvent.run_id, CachedRunEvent.step_key)
                    .join(
                        CachedDagsterRun,
                        (
                            col(CachedRunEvent.namespace_id)
                            == CachedDagsterRun.namespace_id
                        )
                        & (col(CachedRunEvent.run_id) == CachedDagsterRun.run_id),
                    )
                    .join(
                        DagsterCacheNamespace,
                        col(CachedDagsterRun.namespace_id) == DagsterCacheNamespace.id,
                    )
                    .where(
                        DagsterCacheNamespace.name == self.namespace,
                        col(CachedDagsterRun.update_time) >= start_time,
                        col(CachedDagsterRun.update_time) <= end_time,
                        CachedRunEvent.event_type == "AssetMaterializationPlannedEvent",
                        col(CachedRunEvent.step_key).is_not(None),
                    )
                    .distinct()
                )
            )

        steps_by_run: dict[str, set[str]] = {}
        for run_id, step_key in planned_steps:
            if step_key is not None:
                steps_by_run.setdefault(run_id, set()).add(step_key)

        records: list[dict[str, object]] = []
        for run, derived in results:
            skip_info, skip_reason, missing_files = _skip_columns(derived)
            records.append(
                {
                    "run_id": run.run_id,
                    "instrument": None,
                    "data_level": None,
                    "descriptor": None,
                    "job_name": run.job_name,
                    "step_key": next(iter(steps_by_run[run.run_id]))
                    if len(steps_by_run.get(run.run_id, ())) == 1
                    else None,
                    "partition": run.partition,
                    "partition_label": None,
                    "repoint": None,
                    "start_time": None,
                    "end_time": None,
                    "status": _status(run, derived),
                    "dagster_status": run.dagster_status,
                    "start_date": None,
                    "end_date": None,
                    "n_expected": derived.n_expected if derived is not None else None,
                    "n_materialized": (
                        derived.n_materialized if derived is not None else None
                    ),
                    "n_skipped": derived.n_skipped if derived is not None else None,
                    "n_explicitly_skipped": (
                        derived.n_explicitly_skipped if derived is not None else None
                    ),
                    "creation_time": run.creation_time,
                    "update_time": run.update_time,
                    "run_start_time": run.start_time,
                    "run_end_time": run.end_time,
                    "duration_seconds": (
                        (run.end_time - run.start_time).total_seconds()
                        if run.start_time is not None and run.end_time is not None
                        else None
                    ),
                    "parent_run_id": run.parent_run_id,
                    "root_run_id": run.root_run_id,
                    "selected_assets": run.selected_assets,
                    "tags": run.tags,
                    "skip_info": skip_info,
                    "skip_reason": skip_reason,
                    "missing_files": missing_files,
                }
            )

        if not records:
            return pd.DataFrame()

        data_df = pd.DataFrame.from_records(records)
        job_parts = data_df["job_name"].str.extract(_JOB_NAME_PATTERN)
        step_parts = data_df["step_key"].str.extract(_STEP_KEY_PATTERN)
        for column in ("instrument", "data_level", "descriptor"):
            data_df[column] = job_parts[column].fillna(step_parts[column])

        partition_parts = data_df["partition"].str.extract(_PARTITION_PATTERN)
        partition_spec = partition_parts["partition_spec"]
        repoint = partition_spec.str.extract(r"^repoint(?P<repoint>\d*)$")["repoint"]
        is_repoint = repoint.notna()
        data_df["partition_label"] = partition_spec.where(
            ~is_repoint,
            "repoint",
        )
        data_df["repoint"] = pd.to_numeric(
            repoint.where(repoint != ""),
            errors="coerce",
        ).astype("Int64")
        data_df["start_time"] = pd.to_datetime(
            partition_parts["start_time"],
            utc=True,
        )
        data_df["end_time"] = pd.to_datetime(
            partition_parts["end_time"],
            utc=True,
        )
        data_df["start_date"] = data_df["start_time"].dt.normalize()
        data_df["end_date"] = data_df["end_time"].dt.normalize()
        for column in _DATETIME_COLUMNS:
            data_df[column] = pd.to_datetime(data_df[column], utc=True)
        for column in _COUNT_COLUMNS:
            data_df[column] = data_df[column].astype("Int64")
        return data_df
