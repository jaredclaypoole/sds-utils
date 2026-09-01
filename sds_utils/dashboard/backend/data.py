import datetime

from pydantic import BaseModel
import pandas as pd
import pandera.pandas as pa


class DataSchema(pa.DataFrameModel):
    """Schema of normalized dashboard rows."""

    asset: str
    instrument: str
    data_level: str
    descriptor: str
    partition: str
    partition_label: str
    start_date: pd.DatetimeTZDtype = pa.Field(
        dtype_kwargs={"unit": "ns", "tz": "UTC"}
    )
    end_date: pd.DatetimeTZDtype = pa.Field(
        dtype_kwargs={"unit": "ns", "tz": "UTC"}
    )
    repoint: int = pa.Field(nullable=True)
    updated: str = pa.Field(nullable=True)
    status: str
    missing_file: str = pa.Field(nullable=True)
    skip_reason: str = pa.Field(nullable=True)
    missing_files: str = pa.Field(nullable=True)
    partition_link: str

    class Config:
        """Require normalized data to contain exactly the declared columns."""

        strict = True


class QuerySpec(BaseModel):
    start_time: datetime.datetime
    end_time: datetime.datetime


class DataSourceBase:
    def query(self, query: QuerySpec) -> pd.DataFrame[DataSchema]:
        raise NotImplementedError
