"""Stable local identifiers for Dagster asset attempts and events."""

import json
from collections.abc import Sequence
from uuid import NAMESPACE_URL, uuid5


def make_attempt_id(
    *,
    dagster_source: str,
    run_id: str,
    asset_path: Sequence[str],
    partition: str,
) -> str:
    """Identify one asset/partition execution within a Dagster run."""
    identity = _canonical(
        {
            "asset_path": list(asset_path),
            "dagster_source": dagster_source.rstrip("/"),
            "partition": partition,
            "run_id": run_id,
        }
    )
    return str(uuid5(NAMESPACE_URL, identity))


def make_event_id(
    *,
    attempt_id: str,
    event_type: str,
    timestamp: str,
    step_key: str | None,
) -> str:
    """Identify one exact Dagster event within an asset/partition attempt."""
    identity = _canonical(
        {
            "attempt_id": attempt_id,
            "event_type": event_type,
            "step_key": step_key,
            "timestamp": timestamp,
        }
    )
    return str(uuid5(NAMESPACE_URL, identity))


def make_run_event_id(
    *,
    dagster_source: str,
    run_id: str,
    event_type: str,
    timestamp: str,
    step_key: str | None,
) -> str:
    """Identify an exact run-scoped event independently of an asset attempt."""
    identity = _canonical(
        {
            "dagster_source": dagster_source.rstrip("/"),
            "event_type": event_type,
            "run_id": run_id,
            "step_key": step_key,
            "timestamp": timestamp,
        }
    )
    return str(uuid5(NAMESPACE_URL, identity))


def _canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
