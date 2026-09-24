"""Tests for persisted dashboard settings."""

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from sds_utils.dashboard.backend.aggbase import AggPreset
from sds_utils.dashboard.backend.db.models import PersistentSettings, UserProfile
from sds_utils.dashboard.backend.settings import DashboardSettings


def test_dashboard_settings_have_safe_defaults() -> None:
    settings = DashboardSettings()

    assert settings.agg.preset == AggPreset.RAW
    assert settings.agg.extra_columns == []
    assert settings.filtering == {}


def test_faulty_stored_field_falls_back_without_discarding_valid_fields() -> None:
    stored = PersistentSettings(
        user_id=1,
        settings={
            "agg": {"preset": "not-a-preset"},
            "filtering": {"status": {"excluded_values_regex": "failed"}},
        },
    )

    settings = stored.validated_settings()

    assert settings.agg == DashboardSettings().agg
    assert settings.filtering == {
        "status": {"excluded_values_regex": "failed"}
    }


def test_settings_tables_persist_valid_json() -> None:
    db_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db_engine)
    settings = DashboardSettings(
        filtering={"status": {"excluded_values_regex": "failed"}}
    )

    with Session(db_engine) as session:
        profile = UserProfile(username="dashboard-user")
        session.add(profile)
        session.flush()
        assert profile.id is not None
        session.add(
            PersistentSettings(
                user_id=profile.id,
                settings=settings.model_dump(mode="json"),
            )
        )
        session.commit()
        stored = session.get(PersistentSettings, profile.id)

    assert stored is not None
    assert stored.validated_settings() == settings
