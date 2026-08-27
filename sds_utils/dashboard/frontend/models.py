"""SQLModel tables and validated documents for frontend-owned state."""

from datetime import UTC, datetime, timedelta
from typing import Literal

from pydantic import BaseModel, field_validator
from pydantic import Field as PydanticField
from sqlalchemy import JSON, Column
from sqlmodel import Field, SQLModel

FilterMode = Literal["pattern", "regex", "one_of", "empty", "not_empty"]


class ColumnFilterSettings(BaseModel):
    """Persisted state for one table-column filter."""

    mode: FilterMode
    value: str | list[str]


class ColumnSortSettings(BaseModel):
    """One column in the shared, precedence-ordered table sort."""

    column: str
    descending: bool = False


def _default_sorting() -> list[ColumnSortSettings]:
    """Return the dashboard's default multi-column sorting precedence."""
    return [
        ColumnSortSettings(column="status"),
        ColumnSortSettings(column="instrument"),
        ColumnSortSettings(column="descriptor"),
        ColumnSortSettings(column="data_level"),
        ColumnSortSettings(column="first_date"),
        ColumnSortSettings(column="last_date"),
        ColumnSortSettings(column="materialized", descending=True),
        ColumnSortSettings(column="materializing", descending=True),
        ColumnSortSettings(column="failed", descending=True),
        ColumnSortSettings(column="skipped", descending=True),
        ColumnSortSettings(column="not_run", descending=True),
        ColumnSortSettings(column="not_found", descending=True),
        ColumnSortSettings(column="partition"),
        ColumnSortSettings(column="update_timestamp"),
        ColumnSortSettings(column="missing_file"),
        ColumnSortSettings(column="skip_reason"),
        ColumnSortSettings(column="missing_files"),
        ColumnSortSettings(column="tags"),
        ColumnSortSettings(column="notes"),
    ]


StatusName = Literal[
    "materialized",
    "materializing",
    "failed",
    "skipped",
    "not-run",
    "not-found",
]
InstrumentName = Literal[
    "all",
    "codice",
    "hi",
    "hit",
    "idex",
    "glows",
    "mag",
    "lo",
    "swapi",
    "swe",
    "ultra",
    "spacecraft",
]
SortColumn = Literal[
    "",
    "asset",
    "instrument",
    "data_level",
    "descriptor",
    "partition",
    "update_timestamp",
    "status",
    "tags",
    "notes",
    "skip_reason",
    "missing_file",
    "missing_files",
]
StartMode = Literal[
    "days_1",
    "days_2",
    "days_3",
    "days_7",
    "days_14",
    "days_30",
    "custom_days",
    "custom_date",
]
EndMode = Literal["now", "custom"]
ViewMode = Literal["all_rows", "summary"]
OptionalColumn = Literal["tags", "notes"]
SummarySortColumn = Literal[
    "",
    "instrument",
    "data_level",
    "descriptor",
    "missing_file",
    "first_date",
    "last_date",
    "materialized",
    "materializing",
    "failed",
    "skipped",
    "not_run",
    "not_found",
]


def _default_visible_statuses() -> list[StatusName]:
    return ["materialized", "materializing", "failed", "skipped", "not-found"]


SummaryGroupDimension = Literal[
    "instrument", "data_level", "descriptor", "missing_file"
]
SummaryDateAggregation = Literal["all", "day", "week", "days"]


def _default_summary_group_dimensions() -> list[SummaryGroupDimension]:
    return ["instrument", "data_level", "descriptor", "missing_file"]


class AppSettingsState(BaseModel):
    """Validated, versionable dashboard settings document."""

    instrument: InstrumentName = "all"
    column_filters: dict[str, ColumnFilterSettings] = PydanticField(
        default_factory=dict
    )
    sort_column: SortColumn = "instrument"
    sort_descending: bool = False
    visible_statuses: list[StatusName] = PydanticField(
        default_factory=_default_visible_statuses
    )
    start_mode: StartMode = "days_3"
    end_mode: EndMode = "now"
    custom_days_before: float = 3.0
    custom_start: datetime = PydanticField(
        default_factory=lambda: datetime.now(UTC) - timedelta(days=3)
    )
    custom_end: datetime = PydanticField(default_factory=lambda: datetime.now(UTC))
    timestamp_filtering: Literal[
        "active_only", "active_or_partition", "partition_only"
    ] = "partition_only"
    show_unpartitioned_assets: bool = False
    view_mode: ViewMode = "all_rows"
    summary_column_filters: dict[str, ColumnFilterSettings] = PydanticField(
        default_factory=dict
    )
    summary_sort_column: SummarySortColumn = "instrument"
    summary_sort_descending: bool = False
    sorting: list[ColumnSortSettings] = PydanticField(default_factory=_default_sorting)
    summary_group_dimensions: list[SummaryGroupDimension] = PydanticField(
        default_factory=_default_summary_group_dimensions
    )
    summary_date_aggregation: SummaryDateAggregation = "all"
    summary_aggregation_days: int = PydanticField(default=2, ge=2)
    visible_optional_columns: list[OptionalColumn] = PydanticField(default_factory=list)
    export_main_csv: bool = True
    export_main_text: bool = False
    export_main_csv_partition_links: bool = False
    export_summary_csv: bool = True
    export_summary_text: bool = False


class BackfillTableSettingsState(BaseModel):
    """Persisted filters, statuses, and sorting for backfill run details."""

    column_filters: dict[str, ColumnFilterSettings] = PydanticField(
        default_factory=dict
    )
    sorting: list[ColumnSortSettings] = PydanticField(
        default_factory=lambda: [
            ColumnSortSettings(column="instrument"),
            ColumnSortSettings(column="data_level"),
            ColumnSortSettings(column="descriptor"),
            ColumnSortSettings(column="partition"),
            ColumnSortSettings(column="status"),
            ColumnSortSettings(column="run_status"),
        ]
    )
    visible_statuses: list[str] = PydanticField(default_factory=list)
    known_statuses: list[str] = PydanticField(default_factory=list)


class AppSettings(SQLModel, table=True):
    """Named dashboard settings profile stored as a JSON document."""

    id: int | None = Field(default=None, primary_key=True)
    profile: str = Field(index=True, unique=True)
    schema_version: int = 1
    settings: dict[str, object] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False),
    )
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class AttemptMetadata(SQLModel, table=True):
    """User-managed metadata attached to a Dagster asset attempt."""

    id: int | None = Field(default=None, primary_key=True)
    dg_atttempt_id: str = Field(index=True)
    tags: list[str] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False),
    )
    notes: str = ""

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, tags: list[str]) -> list[str]:
        """Reject delimiters that would make tags ambiguous in text controls."""
        invalid = [tag for tag in tags if ";" in tag or any(map(str.isspace, tag))]
        if invalid:
            raise ValueError("tags must not contain whitespace or semicolons")
        return tags

    @property
    def tags_str(self) -> str:
        """Return tags formatted for the dashboard text control."""
        return "; ".join(self.tags)

    @classmethod
    def parse_tags_str(cls, tags_str: str) -> list[str]:
        """Parse the dashboard text control value into individual tags."""
        return [tag.strip() for tag in tags_str.split(";") if tag.strip()]
