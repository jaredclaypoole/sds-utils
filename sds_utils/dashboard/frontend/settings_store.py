"""Persistence for validated frontend settings profiles."""

import logging
from datetime import UTC, datetime

from pydantic import ValidationError
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from .db import engine
from .models import (
    AppSettings,
    AppSettingsState,
    BackfillTableSettingsState,
)

logger = logging.getLogger("uvicorn.error.sds_utils.dashboard.settings")


class AppSettingsStore:
    """Load and replace a named application-settings document."""

    def __init__(self, db_engine: Engine = engine, profile: str = "default") -> None:
        self.engine = db_engine
        self.profile = profile

    def load(self) -> AppSettingsState:
        """Load validated application settings or return defaults."""
        with Session(self.engine) as session:
            record = session.exec(
                select(AppSettings).where(AppSettings.profile == self.profile)
            ).first()
            if record is None:
                return AppSettingsState()
            try:
                return AppSettingsState.model_validate(record.settings)
            except ValidationError as exc:
                logger.warning(
                    "Saved application settings for profile %r are invalid; "
                    "using defaults. Validation error: %s. Saved document: %r",
                    self.profile,
                    exc,
                    record.settings,
                )
                return AppSettingsState()

    def save(self, settings: AppSettingsState) -> None:
        """Replace the persisted application settings document."""
        document = settings.model_dump(mode="json")
        with Session(self.engine) as session:
            record = session.exec(
                select(AppSettings).where(AppSettings.profile == self.profile)
            ).first()
            if record is None:
                record = AppSettings(profile=self.profile)
                session.add(record)
            record.settings = document
            record.schema_version = 1
            record.updated_at = datetime.now(UTC)
            session.commit()


class BackfillTableSettingsStore:
    """Persist backfill run-table settings in an independent profile row."""

    def __init__(
        self,
        db_engine: Engine = engine,
        profile: str = "backfill_detail",
    ) -> None:
        self.engine = db_engine
        self.profile = profile

    def load(self) -> BackfillTableSettingsState:
        """Load validated backfill-table settings or return defaults."""
        with Session(self.engine) as session:
            record = session.exec(
                select(AppSettings).where(AppSettings.profile == self.profile)
            ).first()
            if record is None:
                return BackfillTableSettingsState()
            try:
                return BackfillTableSettingsState.model_validate(record.settings)
            except ValidationError as exc:
                logger.warning(
                    "Saved backfill settings for profile %r are invalid; "
                    "using defaults. Validation error: %s. Saved document: %r",
                    self.profile,
                    exc,
                    record.settings,
                )
                return BackfillTableSettingsState()

    def save(self, settings: BackfillTableSettingsState) -> None:
        """Replace the persisted backfill-table settings document."""
        document = settings.model_dump(mode="json")
        with Session(self.engine) as session:
            record = session.exec(
                select(AppSettings).where(AppSettings.profile == self.profile)
            ).first()
            if record is None:
                record = AppSettings(profile=self.profile)
                session.add(record)
            record.settings = document
            record.schema_version = 1
            record.updated_at = datetime.now(UTC)
            session.commit()
