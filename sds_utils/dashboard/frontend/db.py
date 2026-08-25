"""Database configuration and initialization for frontend-owned state."""

import os
from pathlib import Path

from sqlmodel import SQLModel, create_engine

from . import models as _models  # noqa: F401

DB_SUFFIX = os.getenv("QUERY_APP_DB_SUFFIX", "")
suffix = f"_{DB_SUFFIX}" if DB_SUFFIX else ""
DEFAULT_DATABASE_PATH = Path(f"app/query_app{suffix}.db")
DEFAULT_DATABASE_URL = f"sqlite:///{DEFAULT_DATABASE_PATH.as_posix()}"
DATABASE_URL = os.getenv("QUERY_APP_DB_URL", DEFAULT_DATABASE_URL)

engine = create_engine(
    DATABASE_URL,
    connect_args=(
        {"check_same_thread": False} if DATABASE_URL.startswith("sqlite:") else {}
    ),
)


def create_db_and_tables() -> None:
    """Create all application tables that do not already exist."""
    if DATABASE_URL == DEFAULT_DATABASE_URL:
        DEFAULT_DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    SQLModel.metadata.create_all(engine)
