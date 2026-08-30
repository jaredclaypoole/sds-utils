from datetime import UTC, datetime, timedelta
from unittest import TestCase, mock

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from sds_utils.dashboard.backend.data import AssetOption
from sds_utils.dashboard.frontend.asset_cache import (
    AssetStatusCache,
    CachedDagsterAssetsDataSource,
    cache_namespace,
)
from sds_utils.dashboard.frontend.models import DagsterCacheNamespace


class AssetStatusCacheTests(TestCase):
    def test_namespace_defaults_to_database_suffix(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"QUERY_APP_DB_SUFFIX": "production"},
            clear=True,
        ):
            self.assertEqual(cache_namespace(), "production")

    def test_explicit_namespace_overrides_database_suffix(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "QUERY_DAGSTER_CACHE_NAMESPACE": "shared-production",
                "QUERY_APP_DB_SUFFIX": "production",
            },
            clear=True,
        ):
            self.assertEqual(cache_namespace(), "shared-production")

    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    @mock.patch(
        "sds_utils.dashboard.frontend.asset_cache.graphql_url",
        return_value="https://first.example/graphql",
    )
    def test_asset_definitions_round_trip(self, _graphql_url: mock.Mock) -> None:
        cache = AssetStatusCache(self.engine, "production")
        expected = [
            AssetOption(
                "mag_l1d_norm",
                ("mag_l1d_norm",),
                True,
                ("partition-one",),
            )
        ]

        cache.save_assets(expected)
        actual, refreshed_at = cache.load_assets()

        self.assertEqual(actual, expected)
        self.assertIsNotNone(refreshed_at)

    def test_url_change_does_not_invalidate_namespace(self) -> None:
        cache = AssetStatusCache(self.engine, "production")
        with mock.patch(
            "sds_utils.dashboard.frontend.asset_cache.graphql_url",
            return_value="https://first.example/graphql",
        ):
            cache.save_assets([AssetOption("mag", ("mag",), False, ())])
        with mock.patch(
            "sds_utils.dashboard.frontend.asset_cache.graphql_url",
            return_value="https://replacement.example/graphql",
        ):
            assets, _refreshed_at = cache.load_assets()

        self.assertEqual([asset.label for asset in assets], ["mag"])
        with Session(self.engine) as session:
            namespace = session.exec(select(DagsterCacheNamespace)).one()
        self.assertEqual(
            namespace.current_graphql_url,
            "https://replacement.example/graphql",
        )
        self.assertEqual(
            namespace.previous_graphql_urls,
            ["https://first.example/graphql"],
        )

    @mock.patch(
        "sds_utils.dashboard.frontend.asset_cache.graphql_url",
        return_value="https://dagster.example/graphql",
    )
    @mock.patch(
        "sds_utils.dashboard.frontend.asset_cache.dagster_ui_url",
        return_value="https://dagster.example",
    )
    def test_status_rows_and_coverage_round_trip(
        self, _ui_url: mock.Mock, _graphql_url: mock.Mock
    ) -> None:
        cache = AssetStatusCache(self.engine, "production")
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = start + timedelta(days=1)
        partition = "x_2026-08-01T00:00:00Z_to_2026-08-02T00:00:00Z"
        row = {
            "row_id": f"mag_l1d_norm\0{partition}",
            "asset": "mag_l1d_norm",
            "instrument": "mag",
            "data_level": "l1d",
            "descriptor": "norm",
            "partition": partition,
            "update_timestamp": "2026-08-01T12:00:00+00:00",
            "status": "materialized",
            "skip_reason": "",
            "missing_file": "",
            "missing_files": "",
            "asset_url": "old",
            "partition_url": "old",
            "status_url": "old",
            "updated_in_window": True,
            "attempt_id": "attempt",
            "event_id": "event",
            "run_id": "run",
            "event_type": "MaterializationEvent",
            "event_scope": "asset",
            "step_key": "step",
            "tags": "",
            "notes": "",
        }
        cache.save_result(
            [row],  # type: ignore[list-item]
            start=start,
            end=end,
            include_activity=True,
            include_partitions=True,
            mark_coverage=True,
        )

        rows = cache.project(
            start=start,
            end=end,
            include_activity=True,
            include_partitions=True,
            assets=(AssetOption("mag_l1d_norm", ("mag_l1d_norm",), True, (partition,)),),
        )

        self.assertTrue(
            cache.covers(
                start,
                end,
                include_activity=True,
                include_partitions=True,
            )
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "materialized")
        self.assertTrue(rows[0]["asset_url"].startswith("https://dagster.example"))

    @mock.patch(
        "sds_utils.dashboard.frontend.asset_cache.graphql_url",
        return_value="https://dagster.example/graphql",
    )
    @mock.patch(
        "sds_utils.dashboard.frontend.asset_cache.dagster_ui_url",
        return_value="https://dagster.example",
    )
    def test_covered_load_queries_only_incremental_activity(
        self, _ui_url: mock.Mock, _graphql_url: mock.Mock
    ) -> None:
        cache = AssetStatusCache(self.engine, "production")
        asset = AssetOption(
            "mag_l1d_norm",
            ("mag_l1d_norm",),
            True,
            ("x_2026-08-01T00:00:00Z_to_2026-08-02T00:00:00Z",),
        )
        cache.save_assets([asset])
        source = mock.Mock()
        source.load_recent_status_rows.return_value = []
        cached_source = CachedDagsterAssetsDataSource(  # type: ignore[arg-type]
            source=source,
            cache=cache,
        )
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = start + timedelta(days=1)

        cached_source.load_recent_status_rows(
            start=start,
            end=end,
            include_recent_activity=False,
            include_partition_ranges=True,
            assets=(asset,),
        )
        cached_source.load_recent_status_rows(
            start=start,
            end=end,
            include_recent_activity=False,
            include_partition_ranges=True,
            assets=(asset,),
        )

        self.assertEqual(source.load_recent_status_rows.call_count, 2)
        incremental_call = source.load_recent_status_rows.call_args_list[1]
        self.assertTrue(incremental_call.kwargs["include_recent_activity"])
        self.assertFalse(incremental_call.kwargs["include_partition_ranges"])

    @mock.patch(
        "sds_utils.dashboard.frontend.asset_cache.graphql_url",
        return_value="https://dagster.example/graphql",
    )
    def test_combined_coverage_satisfies_partition_only_request(
        self, _graphql_url: mock.Mock
    ) -> None:
        cache = AssetStatusCache(self.engine, "production")
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = start + timedelta(days=1)
        cache.save_result(
            [],
            start=start,
            end=end,
            include_activity=True,
            include_partitions=True,
            mark_coverage=True,
        )

        self.assertTrue(
            cache.covers(
                start,
                end,
                include_activity=False,
                include_partitions=True,
            )
        )
