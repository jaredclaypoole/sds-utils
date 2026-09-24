"""Typed persistent settings for the dashboard interface."""

from collections.abc import Mapping
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from .aggbase import AggPreset, AggSpec
from .filtersbase import FilterArguments


class DashboardSettings(BaseModel):
    """Aggregation and filtering choices persisted for one user."""

    model_config = ConfigDict(extra="forbid")

    agg: AggSpec = Field(
        default_factory=lambda: AggSpec(preset=AggPreset.RAW),
    )
    filtering: FilterArguments = Field(default_factory=dict)

    @classmethod
    def from_stored(cls, value: object) -> Self:
        """Validate stored fields independently, defaulting invalid fields."""
        defaults = cls()
        if not isinstance(value, Mapping):
            return defaults

        validated: dict[str, object] = {}
        for name, field in cls.model_fields.items():
            default = getattr(defaults, name)
            try:
                validated[name] = TypeAdapter(
                    field.rebuild_annotation()
                ).validate_python(value.get(name, default))
            except (TypeError, ValidationError, ValueError):
                validated[name] = default
        return cls.model_validate(validated)
