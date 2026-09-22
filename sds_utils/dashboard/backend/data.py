import datetime
from typing import Literal

import pandas as pd
import pandera.pandas as pa
from pydantic import BaseModel


class DataSchema(pa.DataFrameModel):
    """Schema of normalized dashboard rows."""

    asset: str
    instrument: str
    data_level: str
    descriptor: str
    partition: str
    partition_label: str
    repoint: int = pa.Field(nullable=True)
    start_time: pd.DatetimeTZDtype = pa.Field(dtype_kwargs={"unit": "ns", "tz": "UTC"})
    end_time: pd.DatetimeTZDtype = pa.Field(dtype_kwargs={"unit": "ns", "tz": "UTC"})
    updated: pd.DatetimeTZDtype = pa.Field(
        dtype_kwargs={"unit": "ns", "tz": "UTC"},
        nullable=True,
    )
    status: str
    missing_file: str = pa.Field(nullable=True)
    skip_reason: str = pa.Field(nullable=True)
    missing_files: str = pa.Field(nullable=True)
    partition_link: str
    start_date: pd.DatetimeTZDtype = pa.Field(dtype_kwargs={"unit": "ns", "tz": "UTC"})
    end_date: pd.DatetimeTZDtype = pa.Field(dtype_kwargs={"unit": "ns", "tz": "UTC"})

    class Config:
        """Require normalized data to contain exactly the declared columns."""

        strict = True


class QuerySpec(BaseModel):
    start_time: datetime.datetime
    end_time: datetime.datetime
    date_mode: Literal["update_time", "partition"] = "update_time"


class DataSourceBase:
    def query(self, query: QuerySpec) -> pd.DataFrame:
        raise NotImplementedError
