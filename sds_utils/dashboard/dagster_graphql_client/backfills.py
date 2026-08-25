from typing import Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel
from .enums import BulkActionStatus


class Backfills(BaseModel):
    partition_backfills_or_error: Union[
        "BackfillsPartitionBackfillsOrErrorPartitionBackfills",
        "BackfillsPartitionBackfillsOrErrorPythonError",
    ] = Field(alias="partitionBackfillsOrError", discriminator="typename__")


class BackfillsPartitionBackfillsOrErrorPartitionBackfills(BaseModel):
    typename__: Literal["PartitionBackfills"] = Field(alias="__typename")
    results: list["BackfillsPartitionBackfillsOrErrorPartitionBackfillsResults"]


class BackfillsPartitionBackfillsOrErrorPartitionBackfillsResults(BaseModel):
    id: str
    status: BulkActionStatus
    title: Optional[str]
    description: Optional[str]
    creation_time: float = Field(alias="creationTime")
    end_time: Optional[float] = Field(alias="endTime")
    num_partitions: Optional[int] = Field(alias="numPartitions")
    is_asset_backfill: bool = Field(alias="isAssetBackfill")


class BackfillsPartitionBackfillsOrErrorPythonError(BaseModel):
    typename__: Literal["PythonError"] = Field(alias="__typename")
    message: str


Backfills.model_rebuild()
BackfillsPartitionBackfillsOrErrorPartitionBackfills.model_rebuild()
