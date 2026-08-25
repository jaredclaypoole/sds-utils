"""Persistence operations for user-managed frontend attempt metadata."""

from collections.abc import Iterable

from sqlalchemy.engine import Engine
from sqlmodel import Session, col, select

from .db import engine
from .models import AttemptMetadata


class AttemptMetadataStore:
    """Read and upsert tags and notes keyed by Dagster attempt identity."""

    def __init__(self, db_engine: Engine = engine) -> None:
        self.engine = db_engine

    def get_many(self, attempt_ids: Iterable[str]) -> dict[str, AttemptMetadata]:
        """Load metadata keyed by each requested attempt ID."""
        unique_ids = tuple(dict.fromkeys(attempt_ids))
        if not unique_ids:
            return {}
        result: dict[str, AttemptMetadata] = {}
        with Session(self.engine) as session:
            for offset in range(0, len(unique_ids), 500):
                batch = unique_ids[offset : offset + 500]
                statement = select(AttemptMetadata).where(
                    col(AttemptMetadata.dg_atttempt_id).in_(batch)
                )
                for metadata in session.exec(statement):
                    result[metadata.dg_atttempt_id] = metadata
        return result

    def set_tags(self, attempt_id: str, tags: list[str]) -> AttemptMetadata:
        """Validate and persist tags for an attempt."""
        validated = AttemptMetadata.model_validate(
            {"dg_atttempt_id": attempt_id, "tags": tags}
        )
        return self._upsert(attempt_id, tags=validated.tags)

    def set_notes(self, attempt_id: str, notes: str) -> AttemptMetadata:
        """Persist notes for an attempt."""
        return self._upsert(attempt_id, notes=notes)

    def _upsert(
        self,
        attempt_id: str,
        *,
        tags: list[str] | None = None,
        notes: str | None = None,
    ) -> AttemptMetadata:
        with Session(self.engine) as session:
            statement = select(AttemptMetadata).where(
                AttemptMetadata.dg_atttempt_id == attempt_id
            )
            metadata = session.exec(statement).first()
            if metadata is None:
                metadata = AttemptMetadata(dg_atttempt_id=attempt_id)
                session.add(metadata)
            if tags is not None:
                metadata.tags = tags
            if notes is not None:
                metadata.notes = notes
            session.commit()
            session.refresh(metadata)
            session.expunge(metadata)
            return metadata
