"""Database persistence for per-user dashboard settings."""

from sqlalchemy import Engine
from sqlmodel import Session

from .db.models import PersistentSettings
from .settings import DashboardSettings


class SettingsRepository:
    """Load and save typed settings for dashboard users."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def load(self, user_id: int) -> DashboardSettings:
        """Load validated settings, or defaults when no row exists."""
        with Session(self.engine) as session:
            stored = session.get(PersistentSettings, user_id)
        if stored is None:
            return DashboardSettings()
        return stored.validated_settings()

    def save(self, user_id: int, settings: DashboardSettings) -> None:
        """Create or replace one user's serialized settings document."""
        with Session(self.engine) as session:
            stored = session.get(PersistentSettings, user_id)
            settings_json = settings.model_dump(mode="json")
            if stored is None:
                stored = PersistentSettings(user_id=user_id, settings=settings_json)
            else:
                stored.settings = settings_json
            session.add(stored)
            session.commit()
