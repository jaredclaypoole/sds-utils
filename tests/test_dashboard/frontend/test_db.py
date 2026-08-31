from unittest import TestCase

from pydantic import ValidationError
from sqlalchemy import inspect
from sqlmodel import Session, SQLModel, create_engine, select

from sds_utils.dashboard.frontend.models import (
    AppSettings,
    AppSettingsState,
    AttemptMetadata,
    BackfillTableSettingsState,
    ColumnFilterSettings,
    ColumnSortSettings,
)
from sds_utils.dashboard.frontend.metadata_store import AttemptMetadataStore
from sds_utils.dashboard.frontend.settings_store import (
    AppSettingsStore,
    BackfillTableSettingsStore,
)


class AttemptMetadataTests(TestCase):
    def test_tags_reject_whitespace_and_semicolons(self) -> None:
        for tag in ("needs review", "needs\treview", "review;later"):
            with self.subTest(tag=tag), self.assertRaises(ValidationError):
                AttemptMetadata.model_validate(
                    {"dg_atttempt_id": "attempt-1", "tags": [tag]}
                )

    def test_tags_allow_unambiguous_names(self) -> None:
        metadata = AttemptMetadata.model_validate(
            {
                "dg_atttempt_id": "attempt-1",
                "tags": ["needs-review", "reviewed_ok"],
            }
        )
        self.assertEqual(metadata.tags, ["needs-review", "reviewed_ok"])

    def test_tags_convert_to_and_from_semicolon_separated_text(self) -> None:
        metadata = AttemptMetadata(
            dg_atttempt_id="attempt-1",
            tags=["needs-review", "reviewed_ok"],
        )
        self.assertEqual(metadata.tags_str, "needs-review; reviewed_ok")
        self.assertEqual(
            AttemptMetadata.parse_tags_str("needs-review; reviewed_ok; "),
            ["needs-review", "reviewed_ok"],
        )

    def test_json_tags_round_trip_and_attempt_id_is_indexed(self) -> None:
        test_engine = create_engine("sqlite://")
        SQLModel.metadata.create_all(test_engine)

        with Session(test_engine) as session:
            metadata = AttemptMetadata(
                dg_atttempt_id="attempt-1",
                tags=["reviewed", "important"],
                notes="Check the missing source files.",
            )
            session.add(metadata)
            session.commit()
            session.refresh(metadata)
            self.assertIsNotNone(metadata.id)

        with Session(test_engine) as session:
            stored = session.exec(select(AttemptMetadata)).one()
            self.assertEqual(stored.tags, ["reviewed", "important"])
            self.assertEqual(stored.notes, "Check the missing source files.")

        indexes = inspect(test_engine).get_indexes(AttemptMetadata.__tablename__)
        self.assertIn(
            ["dg_atttempt_id"],
            [index["column_names"] for index in indexes],
        )

    def test_store_upserts_fields_without_overwriting_other_metadata(self) -> None:
        test_engine = create_engine("sqlite://")
        SQLModel.metadata.create_all(test_engine)
        store = AttemptMetadataStore(test_engine)

        store.set_tags("attempt-1", ["reviewed", "important"])
        store.set_notes("attempt-1", "Check source files.")

        stored = store.get_many(["attempt-1"])["attempt-1"]
        self.assertEqual(stored.tags, ["reviewed", "important"])
        self.assertEqual(stored.notes, "Check source files.")
        with Session(test_engine) as session:
            self.assertEqual(len(session.exec(select(AttemptMetadata)).all()), 1)

    def test_store_validates_tags_before_writing(self) -> None:
        test_engine = create_engine("sqlite://")
        SQLModel.metadata.create_all(test_engine)
        store = AttemptMetadataStore(test_engine)

        with self.assertRaises(ValidationError):
            store.set_tags("attempt-1", ["needs review"])
        self.assertEqual(store.get_many(["attempt-1"]), {})


class AppSettingsStoreTests(TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite://")
        SQLModel.metadata.create_all(self.engine)
        self.store = AppSettingsStore(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_missing_profile_loads_defaults(self) -> None:
        settings = self.store.load()
        self.assertEqual(settings.start_mode, "days_3")
        self.assertEqual(settings.end_mode, "now")
        self.assertEqual(settings.instrument, "all")
        self.assertEqual(settings.records_per_page, 25)
        self.assertNotIn("not-run", settings.visible_statuses)
        self.assertEqual(settings.visible_optional_columns, [])

    def test_settings_round_trip_as_one_profile(self) -> None:
        settings = AppSettingsState(
            instrument="mag",
            column_filters={
                "asset": ColumnFilterSettings(mode="pattern", value="^lo_l1b"),
                "status": ColumnFilterSettings(
                    mode="one_of", value=["failed", "skipped"]
                ),
            },
            sort_column="partition",
            sort_descending=True,
            start_mode="custom_days",
            custom_days_before=4.5,
            timestamp_filtering="partition_only",
            show_unpartitioned_assets=True,
            view_mode="dependency_graph",
            records_per_page=100,
            snapshot_partition_types=["daily", "idex10day"],
            snapshot_data_levels=["l1a", "ancillary"],
            snapshot_instruments=["mag", "spacecraft"],
            snapshot_instrument_aggregation="separate",
            snapshot_show_totals=True,
            dependency_graph_instrument="spacecraft",
            summary_column_filters={
                "instrument": ColumnFilterSettings(mode="pattern", value="^mag$")
            },
            summary_sort_column="failed",
            summary_sort_descending=True,
            sorting=[
                ColumnSortSettings(column="status", descending=True),
                ColumnSortSettings(column="instrument"),
            ],
            summary_group_dimensions=["instrument", "data_level"],
            summary_date_aggregation="days",
            summary_aggregation_days=3,
            visible_optional_columns=["tags"],
            export_main_csv=False,
            export_main_text=True,
            export_main_csv_partition_links=True,
            export_summary_csv=False,
            export_summary_text=True,
        )
        self.store.save(settings)
        self.store.save(settings.model_copy(update={"end_mode": "custom"}))

        loaded = self.store.load()
        self.assertEqual(loaded, settings.model_copy(update={"end_mode": "custom"}))
        with Session(self.engine) as session:
            self.assertEqual(len(session.exec(select(AppSettings)).all()), 1)

    def test_backfill_table_settings_use_a_separate_profile_row(self) -> None:
        self.store.save(AppSettingsState(instrument="mag"))
        backfill_store = BackfillTableSettingsStore(self.engine)
        backfill_store.save(
            BackfillTableSettingsState(
                visible_statuses=["FAILURE"],
                column_filters={
                    "instrument": ColumnFilterSettings(
                        mode="pattern", value="^swe$"
                    )
                },
            )
        )

        self.assertEqual(self.store.load().instrument, "mag")
        self.assertEqual(backfill_store.load().visible_statuses, ["FAILURE"])
        with Session(self.engine) as session:
            profiles = {
                row.profile for row in session.exec(select(AppSettings)).all()
            }
        self.assertEqual(profiles, {"default", "backfill_detail"})

    def test_default_sorting_matches_dashboard_precedence(self) -> None:
        settings = AppSettingsState()

        self.assertEqual(
            [(rule.column, rule.descending) for rule in settings.sorting],
            [
                ("status", False),
                ("instrument", False),
                ("descriptor", False),
                ("data_level", False),
                ("first_date", False),
                ("last_date", False),
                ("materialized", True),
                ("materializing", True),
                ("failed", True),
                ("skipped", True),
                ("not_run", True),
                ("not_found", True),
                ("partition", False),
                ("update_timestamp", False),
                ("missing_file", False),
                ("skip_reason", False),
                ("missing_files", False),
                ("tags", False),
                ("notes", False),
            ],
        )

    def test_default_timestamp_filtering_is_partition_only(self) -> None:
        self.assertEqual(AppSettingsState().timestamp_filtering, "partition_only")

    def test_invalid_saved_document_falls_back_to_defaults(self) -> None:
        with Session(self.engine) as session:
            session.add(
                AppSettings(
                    profile="default",
                    settings={"timestamp_filtering": "unknown"},
                )
            )
            session.commit()

        self.assertEqual(self.store.load().timestamp_filtering, "partition_only")
