import marimo

__generated_with = "0.24.0"
app = marimo.App()

with app.setup:
    from dataclasses import dataclass

    import marimo as mo
    from sqlmodel import Session, select

    from db import AssetPartitionHistory, engine
    from sds_utils.dashboard.backend.data import (
        DagsterAssetsDataSource,
        _dagster_timestamp_sort_key as dagster_timestamp_sort_key,
    )
    from sds_utils.dashboard.backend.dagster_client import asset_state_batch_size
    from sds_utils.dashboard.dagster_graphql_client.enums import RunStatus
    from sds_utils.dashboard.dagster_graphql_client.input_types import AssetKeyInput


@app.class_definition
@dataclass(frozen=True)
class CacheValidationMismatch:
    asset: str
    partition: str
    cached: bool
    targeted: bool


@app.class_definition
@dataclass(frozen=True)
class CacheValidationReport:
    checked: int
    cached_true: int
    targeted_true: int
    mismatches: tuple[CacheValidationMismatch, ...]


@app.function
def cache_key(
    row: AssetPartitionHistory,
) -> tuple[tuple[str, ...], str]:
    return ((row.asset,), row.partition)


@app.function
def load_pair_stale_successes(
    keys: set[tuple[tuple[str, ...], str]],
) -> dict[tuple[tuple[str, ...], str], bool]:
    """Classify pairs with sequential, partition-batched GraphQL requests."""
    assets_by_partition: dict[str, list[tuple[str, ...]]] = {}
    for asset_path, partition in keys:
        assets_by_partition.setdefault(partition, []).append(asset_path)

    source = DagsterAssetsDataSource()
    results: dict[tuple[tuple[str, ...], str], bool] = {}
    batch_size = asset_state_batch_size()
    batches = [
        (partition, asset_paths[start : start + batch_size])
        for partition, asset_paths in assets_by_partition.items()
        for start in range(0, len(asset_paths), batch_size)
    ]
    for partition, batch in mo.status.progress_bar(
        batches,
        title="Validating cached asset-partition pairs",
        completion_title="Cache validation queries complete",
    ):
        with source._client() as client:
            nodes = client.asset_partition_pair_states(
                asset_keys=[
                    AssetKeyInput(path=list(asset_path)) for asset_path in batch
                ],
                partitions=[partition],
                partition=partition,
            ).asset_nodes

        returned_paths: set[tuple[str, ...]] = set()
        for node in nodes:
            asset_path = tuple(node.asset_key.path)
            returned_paths.add(asset_path)
            materialization = next(iter(node.latest_materialization_by_partition), None)
            latest_run = node.latest_run_for_partition
            results[(asset_path, partition)] = bool(
                materialization is not None
                and latest_run is not None
                and latest_run.status is RunStatus.FAILURE
                and latest_run.update_time is not None
                and latest_run.update_time
                >= dagster_timestamp_sort_key(materialization.timestamp)
            )

        missing = set(batch) - returned_paths
        if missing:
            formatted = ", ".join(" / ".join(path) for path in sorted(missing))
            raise ValueError(f"Dagster did not return requested assets: {formatted}")
    return results


@app.function
def validate_cached_pairs_with_targeted_query() -> CacheValidationReport:
    """Compare existing cache rows without modifying the cache."""
    with Session(engine) as session:
        cached_rows = tuple(session.exec(select(AssetPartitionHistory)).all())

    keys = {cache_key(row) for row in cached_rows}
    targeted = load_pair_stale_successes(keys)
    mismatches = tuple(
        CacheValidationMismatch(
            asset=row.asset,
            partition=row.partition,
            cached=row.stale_success,
            targeted=targeted[cache_key(row)],
        )
        for row in cached_rows
        if row.stale_success != targeted[cache_key(row)]
    )
    return CacheValidationReport(
        checked=len(cached_rows),
        cached_true=sum(row.stale_success for row in cached_rows),
        targeted_true=sum(targeted.values()),
        mismatches=mismatches,
    )


@app.cell
def _():
    run_validation = True
    if run_validation:
        report = validate_cached_pairs_with_targeted_query()
        print(
            f"checked={report.checked}, cached_true={report.cached_true}, "
            f"targeted_true={report.targeted_true}, "
            f"mismatches={len(report.mismatches)}"
        )
        for mismatch in report.mismatches:
            print(mismatch)
    return


if __name__ == "__main__":
    app.run()
