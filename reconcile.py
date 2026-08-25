import time

from functools import cache

from sds_utils.dashboard.backend.data import DagsterAssetsDataSource
from db import AssetPartitionHistory, create_db_and_tables, engine
from sqlmodel import select, Session


_db_init_done = False


@cache
def latest_attempt_failed_after_previous_success_cached(asset: str, partition: str) -> bool:
    time.sleep(0.1)
    dg_source = DagsterAssetsDataSource()
    return dg_source.latest_attempt_failed_after_previous_success(
        [asset],
        partition
    )


def latest_attempt_failed_after_previous_success_db(asset: str, partition: str) -> bool:
    global _db_init_done
    if not _db_init_done:
        create_db_and_tables()
        _db_init_done = True

    with Session(engine) as session:
        elems = session.exec(
            select(AssetPartitionHistory).where(
                (AssetPartitionHistory.asset == asset) &
                (AssetPartitionHistory.partition == partition)
            )
        ).all()
        if elems:
            assert len(elems) == 1
            return elems[0].stale_success
        else:
            dg_source = DagsterAssetsDataSource()
            is_stale_success = dg_source.latest_attempt_failed_after_previous_success(
                [asset],
                partition
            )
            elem = AssetPartitionHistory(
                asset=asset,
                partition=partition,
                stale_success=is_stale_success,
            )
            session.add(elem)
            session.commit()
            return is_stale_success
