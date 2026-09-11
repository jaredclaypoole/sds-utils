from enum import StrEnum, auto

import pandas as pd
from pydantic import BaseModel, Field

from .status import StatusCounts


class AggPreset(StrEnum):
    RAW = auto()
    DATES_SUMMARY = auto()
    INSTRUMENT_SNAPSHOT = auto()
    FULL_SNAPSHOT = auto()


class AggSpec(BaseModel):
    preset: AggPreset
    extra_columns: list[str] = Field(default_factory=list)


class Aggregator:
    def apply(self, data_df: pd.DataFrame, agg_spec: AggSpec) -> pd.DataFrame:
        match agg_spec.preset:
            case AggPreset.RAW:
                return data_df
            case AggPreset.DATES_SUMMARY:
                horiz_col = None
            case AggPreset.INSTRUMENT_SNAPSHOT:
                horiz_col = "data_level"
            case AggPreset.FULL_SNAPSHOT:
                horiz_col = "instrument"
            case _:
                raise NotImplementedError(agg_spec.preset)

        horiz_cols = [horiz_col] if horiz_col is not None else []
        cols_to_keep = ["start_date", *horiz_cols, *agg_spec.extra_columns]
        cols_to_keep = list(dict.fromkeys(cols_to_keep))

        def _make_status_counts(statuses: pd.Series) -> StatusCounts:
            kwargs: dict[str, int] = statuses.value_counts().to_dict()
            kwargs = {k.replace("-", "_"): v for k, v in kwargs.items()}
            return StatusCounts(**kwargs)
        grouped_df = pd.DataFrame(
            data_df.groupby(cols_to_keep).status.apply(_make_status_counts).rename("status_counts")
        ).reset_index()

        if horiz_col is not None:
            if agg_spec.extra_columns:
                raise ValueError(
                    f"Extra columns are not allowed for this agg preset: {agg_spec.preset}"
                )
            snapshot_df = grouped_df.pivot_table(
                columns=horiz_col,
                index="start_date",
                values="status_counts",
                aggfunc="sum",
            )
            return snapshot_df
        else:
            return grouped_df


