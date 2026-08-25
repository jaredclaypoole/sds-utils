"""Classify Dagster asset partitions using generated GraphQL models."""

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from itertools import islice
from typing import Protocol, TypeVar

from sds_utils.dashboard.dagster_graphql_client import (
    AssetKeyInput,
    DagsterGraphQLClient,
)
from sds_utils.dashboard.dagster_graphql_client.asset_partition_history import (
    AssetPartitionHistoryAssetOrErrorAsset as HistoryAsset,
)
from sds_utils.dashboard.dagster_graphql_client.asset_partition_history import (
    AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsFailedToMaterializeEvent as FailedEvent,
)
from sds_utils.dashboard.dagster_graphql_client.asset_partition_history import (
    AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsObservationEvent as ObservationEvent,
)
from sds_utils.dashboard.dagster_graphql_client.asset_partition_history import (
    AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsObservationEventMetadataEntriesTextMetadataEntry as ObservationTextMetadata,
)
from sds_utils.dashboard.dagster_graphql_client.asset_partition_state import (
    AssetPartitionStateAssetNodeOrErrorAssetNode as AssetNode,
)
from sds_utils.dashboard.dagster_graphql_client.asset_partition_state import (
    AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatuses as MultiStatuses,
)
from sds_utils.dashboard.dagster_graphql_client.asset_partition_states import (
    AssetPartitionStatesAssetNodes as BatchAssetNode,
)
from sds_utils.dashboard.dagster_graphql_client.asset_partition_states import (
    AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatuses as BatchMultiStatuses,
)
from sds_utils.dashboard.dagster_graphql_client.enums import (
    AssetMaterializationFailureType,
    PartitionRangeStatus,
)

DEFAULT_HISTORY_PAGE_SIZE = 1_000
DEFAULT_HISTORY_PARTITION_BATCH_SIZE = 500
DEFAULT_ASSET_STATE_BATCH_SIZE = 500

logger = logging.getLogger(
    "uvicorn.error.sds_utils.dashboard.backend.partition_classifier"
)


class PartitionCategory(StrEnum):
    """Dashboard classification for a Dagster asset partition."""

    MATERIALIZED = "materialized"
    MATERIALIZING = "materializing"
    FAILED = "failed"
    SKIPPED = "skipped"
    NOT_RUN = "not-run"
    NOT_FOUND = "not-found"


@dataclass(frozen=True, slots=True)
class SkippedPartition:
    """Event details explaining why a partition was skipped."""

    partition: str
    run_id: str
    step_key: str | None
    event_type: str
    timestamp: str
    reason: str
    metadata: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class PartitionFailureDetails:
    """Latest failure or skip details for a partition."""

    partition: str
    run_id: str
    step_key: str | None
    event_type: str
    timestamp: str
    skipped: bool
    reason: str
    metadata: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class PartitionCategories:
    """Partition keys grouped by their resolved dashboard status."""

    asset_key: tuple[str, ...]
    materialized: tuple[str, ...]
    materializing: tuple[str, ...]
    failed: tuple[str, ...]
    skipped: tuple[str, ...]
    not_run: tuple[str, ...]
    not_found: tuple[str, ...]
    skipped_details: tuple[SkippedPartition, ...]

    def for_category(self, category: PartitionCategory) -> tuple[str, ...]:
        """Return partition keys belonging to a category."""
        return {
            PartitionCategory.MATERIALIZED: self.materialized,
            PartitionCategory.MATERIALIZING: self.materializing,
            PartitionCategory.FAILED: self.failed,
            PartitionCategory.SKIPPED: self.skipped,
            PartitionCategory.NOT_RUN: self.not_run,
            PartitionCategory.NOT_FOUND: self.not_found,
        }[category]


class PartitionClassificationError(RuntimeError):
    """Raised when Dagster returns an unusable partition response."""


class _DefaultStatuses(Protocol):
    materialized_partitions: list[str]
    materializing_partitions: list[str]
    failed_partitions: list[str]
    unmaterialized_partitions: list[str]


@dataclass(frozen=True, slots=True)
class _SkipMarker:
    run_id: str
    step_key: str | None
    event_type: str
    timestamp: str
    sort_key: float
    skipped: bool
    reason: str
    metadata: tuple[tuple[str, str], ...]


def classify_asset_partitions(
    client: DagsterGraphQLClient,
    asset_key: Sequence[str],
    *,
    history_page_size: int = DEFAULT_HISTORY_PAGE_SIZE,
    history_partition_batch_size: int = DEFAULT_HISTORY_PARTITION_BATCH_SIZE,
) -> PartitionCategories:
    """Sort every partition for an asset into one mutually exclusive category."""
    if history_page_size < 1 or history_partition_batch_size < 1:
        raise ValueError("History page and batch sizes must be positive")

    key_input = AssetKeyInput(path=list(asset_key))
    state_result = client.asset_partition_state(asset_key=key_input).asset_node_or_error
    if not isinstance(state_result, AssetNode):
        raise PartitionClassificationError(state_result.message)

    native = _native_partition_categories(state_result)
    partition_order = state_result.partition_keys
    native_keys = set().union(*native.values())
    missing = set(partition_order) - native_keys
    markers = _load_skip_markers(
        client,
        key_input,
        partition_order,
        missing,
        page_size=history_page_size,
        partition_batch_size=history_partition_batch_size,
    )
    skipped = {partition for partition, marker in markers.items() if marker.skipped}
    not_run = missing - skipped

    def ordered(keys: set[str]) -> tuple[str, ...]:
        return tuple(partition for partition in partition_order if partition in keys)

    skipped_details = tuple(
        SkippedPartition(
            partition=partition,
            run_id=markers[partition].run_id,
            step_key=markers[partition].step_key,
            event_type=markers[partition].event_type,
            timestamp=markers[partition].timestamp,
            reason=markers[partition].reason,
            metadata=markers[partition].metadata,
        )
        for partition in partition_order
        if partition in skipped
    )
    return PartitionCategories(
        asset_key=tuple(state_result.asset_key.path),
        materialized=ordered(native[PartitionCategory.MATERIALIZED]),
        materializing=ordered(native[PartitionCategory.MATERIALIZING]),
        failed=ordered(native[PartitionCategory.FAILED]),
        skipped=ordered(skipped),
        not_run=ordered(not_run),
        not_found=(),
        skipped_details=skipped_details,
    )


def get_native_partition_categories(
    client: DagsterGraphQLClient,
    asset_key: Sequence[str],
) -> dict[PartitionCategory, set[str]]:
    """Return Dagster's authoritative native partition-status sets."""
    result = client.asset_partition_state(
        asset_key=AssetKeyInput(path=list(asset_key))
    ).asset_node_or_error
    if not isinstance(result, AssetNode):
        raise PartitionClassificationError(result.message)
    return _native_partition_categories(result)


def get_native_partition_categories_batch(
    client: DagsterGraphQLClient,
    asset_keys: Iterable[Sequence[str]],
    *,
    batch_size: int = DEFAULT_ASSET_STATE_BATCH_SIZE,
) -> dict[tuple[str, ...], dict[PartitionCategory, set[str]]]:
    """Return native partition states using batched ``assetNodes`` queries."""
    if batch_size < 1:
        raise ValueError("Asset state batch size must be positive")
    result: dict[tuple[str, ...], dict[PartitionCategory, set[str]]] = {}
    for batch in _batched(asset_keys, batch_size):
        nodes = client.asset_partition_states(
            asset_keys=[AssetKeyInput(path=list(path)) for path in batch]
        ).asset_nodes
        result.update(
            {
                tuple(node.asset_key.path): _native_partition_categories(node)
                for node in nodes
            }
        )
    return result


def get_skipped_partition_details(
    client: DagsterGraphQLClient,
    asset_key: Sequence[str],
    partitions: Sequence[str],
    *,
    history_page_size: int = DEFAULT_HISTORY_PAGE_SIZE,
    history_partition_batch_size: int = DEFAULT_HISTORY_PARTITION_BATCH_SIZE,
) -> dict[str, SkippedPartition]:
    """Return latest skip details for only the requested partition keys."""
    if not partitions:
        return {}
    markers = _load_skip_markers(
        client,
        AssetKeyInput(path=list(asset_key)),
        partitions,
        set(partitions),
        page_size=history_page_size,
        partition_batch_size=history_partition_batch_size,
    )
    return {
        partition: SkippedPartition(
            partition=partition,
            run_id=marker.run_id,
            step_key=marker.step_key,
            event_type=marker.event_type,
            timestamp=marker.timestamp,
            reason=marker.reason,
            metadata=marker.metadata,
        )
        for partition, marker in markers.items()
        if marker.skipped
    }


def get_partition_failure_details(
    client: DagsterGraphQLClient,
    asset_key: Sequence[str],
    partitions: Sequence[str],
    *,
    history_page_size: int = DEFAULT_HISTORY_PAGE_SIZE,
    history_partition_batch_size: int = DEFAULT_HISTORY_PARTITION_BATCH_SIZE,
) -> dict[str, PartitionFailureDetails]:
    """Return latest failure or skip markers for requested partition keys."""
    if not partitions:
        return {}
    markers = _load_skip_markers(
        client,
        AssetKeyInput(path=list(asset_key)),
        partitions,
        set(partitions),
        page_size=history_page_size,
        partition_batch_size=history_partition_batch_size,
    )
    return {
        partition: PartitionFailureDetails(
            partition=partition,
            run_id=marker.run_id,
            step_key=marker.step_key,
            event_type=marker.event_type,
            timestamp=marker.timestamp,
            skipped=marker.skipped,
            reason=marker.reason,
            metadata=marker.metadata,
        )
        for partition, marker in markers.items()
        if marker.skipped or marker.event_type == "FailedToMaterializeEvent"
    }


def _native_partition_categories(
    node: AssetNode | BatchAssetNode,
) -> dict[PartitionCategory, set[str]]:
    categories = {
        PartitionCategory.MATERIALIZED: set[str](),
        PartitionCategory.MATERIALIZING: set[str](),
        PartitionCategory.FAILED: set[str](),
    }
    statuses = node.asset_partition_statuses

    if statuses.typename__ == "DefaultPartitionStatuses":
        _merge_default_statuses(categories, statuses)
    elif statuses.typename__ == "TimePartitionStatuses":
        for status_range in statuses.ranges:
            category = _category_for_range_status(status_range.status)
            categories[category].update(
                _keys_in_range(
                    node.partition_keys, status_range.start_key, status_range.end_key
                )
            )
    elif statuses.typename__ == "MultiPartitionStatuses":
        _merge_multi_statuses(categories, node, statuses)
    else:  # pragma: no cover - generated union makes this defensive only
        raise PartitionClassificationError(
            f"Unsupported partition status type: {type(statuses).__name__}"
        )

    overlaps = (
        categories[PartitionCategory.MATERIALIZED]
        & categories[PartitionCategory.MATERIALIZING]
        | categories[PartitionCategory.MATERIALIZED]
        & categories[PartitionCategory.FAILED]
        | categories[PartitionCategory.MATERIALIZING]
        & categories[PartitionCategory.FAILED]
    )
    if overlaps:
        logger.debug(
            "Normalizing %d overlapping native partition statuses for asset %s",
            len(overlaps),
            " / ".join(node.asset_key.path),
        )
        materializing = categories[PartitionCategory.MATERIALIZING]
        failed = categories[PartitionCategory.FAILED]
        materialized = categories[PartitionCategory.MATERIALIZED]
        failed.difference_update(materializing)
        materialized.difference_update(materializing | failed)
    return categories


def _merge_default_statuses(
    categories: dict[PartitionCategory, set[str]], statuses: _DefaultStatuses
) -> None:
    categories[PartitionCategory.MATERIALIZED].update(statuses.materialized_partitions)
    categories[PartitionCategory.MATERIALIZING].update(
        statuses.materializing_partitions
    )
    categories[PartitionCategory.FAILED].update(statuses.failed_partitions)


def _merge_multi_statuses(
    categories: dict[PartitionCategory, set[str]],
    node: AssetNode | BatchAssetNode,
    statuses: MultiStatuses | BatchMultiStatuses,
) -> None:
    dimensions = {
        dimension.name: dimension.partition_keys
        for dimension in node.partition_keys_by_dimension
    }
    dimension_order = (
        [dimension.name for dimension in node.partition_definition.dimension_types]
        if node.partition_definition
        else list(dimensions)
    )
    if (
        len(dimension_order) != len(("primary", "secondary"))
        or statuses.primary_dimension_name not in dimensions
    ):
        raise PartitionClassificationError(
            "Dagster multipartition status decoding requires exactly two dimensions"
        )

    primary_name = statuses.primary_dimension_name
    secondary_name = next(name for name in dimension_order if name != primary_name)
    primary_keys = dimensions[primary_name]
    secondary_keys = dimensions[secondary_name]
    known_composite_keys = set(node.partition_keys)

    def composite_key(primary: str, secondary: str) -> str:
        values = {primary_name: primary, secondary_name: secondary}
        key = "|".join(values[name] for name in dimension_order)
        if key not in known_composite_keys:
            raise PartitionClassificationError(
                f"Could not map multipartition dimensions to key {key!r}"
            )
        return key

    for primary_range in statuses.ranges:
        primary_values = _keys_in_range(
            primary_keys,
            primary_range.primary_dim_start_key,
            primary_range.primary_dim_end_key,
        )
        secondary_categories = {
            PartitionCategory.MATERIALIZED: set[str](),
            PartitionCategory.MATERIALIZING: set[str](),
            PartitionCategory.FAILED: set[str](),
        }
        secondary = primary_range.secondary_dim
        if secondary.typename__ == "DefaultPartitionStatuses":
            _merge_default_statuses(secondary_categories, secondary)
        else:
            for status_range in secondary.ranges:
                category = _category_for_range_status(status_range.status)
                secondary_categories[category].update(
                    _keys_in_range(
                        secondary_keys, status_range.start_key, status_range.end_key
                    )
                )

        for category, categorized_secondary_keys in secondary_categories.items():
            categories[category].update(
                composite_key(primary, secondary_key)
                for primary in primary_values
                for secondary_key in categorized_secondary_keys
            )


def _keys_in_range(keys: Sequence[str], start: str, end: str) -> list[str]:
    try:
        start_index = keys.index(start)
        end_index = keys.index(end)
    except ValueError as exc:
        raise PartitionClassificationError(
            f"Partition range endpoint is absent from returned keys: {start!r}..{end!r}"
        ) from exc
    if start_index > end_index:
        raise PartitionClassificationError(
            f"Reversed partition range: {start!r}..{end!r}"
        )
    return list(keys[start_index : end_index + 1])


def _category_for_range_status(status: PartitionRangeStatus) -> PartitionCategory:
    return {
        PartitionRangeStatus.MATERIALIZED: PartitionCategory.MATERIALIZED,
        PartitionRangeStatus.MATERIALIZING: PartitionCategory.MATERIALIZING,
        PartitionRangeStatus.FAILED: PartitionCategory.FAILED,
    }[status]


def _load_skip_markers(
    client: DagsterGraphQLClient,
    asset_key: AssetKeyInput,
    partition_order: Sequence[str],
    missing: set[str],
    *,
    page_size: int,
    partition_batch_size: int,
) -> dict[str, _SkipMarker]:
    markers: dict[str, _SkipMarker] = {}
    missing_in_order = (
        partition for partition in partition_order if partition in missing
    )

    for partition_batch in _batched(missing_in_order, partition_batch_size):
        cursor: str | None = None
        while True:
            result = client.asset_partition_history(
                asset_key=asset_key,
                partitions=list(partition_batch),
                cursor=cursor,
                limit=page_size,
            ).asset_or_error
            if not isinstance(result, HistoryAsset):
                raise PartitionClassificationError(result.message)

            history = result.asset_event_history
            for event in history.results:
                marker = _skip_marker(event)
                if marker is None or event.partition not in missing:
                    continue
                existing = markers.get(event.partition)
                if existing is None or marker.sort_key > existing.sort_key:
                    markers[event.partition] = marker

            next_cursor = history.cursor
            if not history.results or not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor

    return markers


def _skip_marker(event: object) -> _SkipMarker | None:
    if isinstance(event, FailedEvent):
        metadata = tuple(
            (entry.label, entry.text)
            for entry in event.metadata_entries
            if hasattr(entry, "text")
        )
        return _SkipMarker(
            run_id=event.run_id,
            step_key=event.step_key,
            event_type=event.typename__,
            timestamp=event.timestamp,
            sort_key=_timestamp_sort_key(event.timestamp),
            skipped=(
                event.materialization_failure_type
                is AssetMaterializationFailureType.SKIPPED
            ),
            reason=event.materialization_failure_reason.value,
            metadata=metadata,
        )

    if isinstance(event, ObservationEvent):
        status = next(
            (
                entry.text
                for entry in event.metadata_entries
                if isinstance(entry, ObservationTextMetadata)
                and entry.label.casefold() == "status"
            ),
            None,
        )
        if status is None:
            return None
        metadata = tuple(
            (entry.label, entry.text)
            for entry in event.metadata_entries
            if isinstance(entry, ObservationTextMetadata)
        )
        return _SkipMarker(
            run_id=event.run_id,
            step_key=event.step_key,
            event_type=event.typename__,
            timestamp=event.timestamp,
            sort_key=_timestamp_sort_key(event.timestamp),
            skipped=status.casefold().startswith("skipped"),
            reason=status,
            metadata=metadata,
        )
    return None


def _timestamp_sort_key(timestamp: str) -> float:
    try:
        return float(timestamp)
    except ValueError:
        return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()


_T = TypeVar("_T")


def _batched(values: Iterable[_T], size: int) -> Iterable[tuple[_T, ...]]:
    iterator = iter(values)
    while batch := tuple(islice(iterator, size)):
        yield batch
