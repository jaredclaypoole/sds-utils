from typing import Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel
from .enums import RunStatus


class BackfillRuns(BaseModel):
    partition_backfill_or_error: Union[
        "BackfillRunsPartitionBackfillOrErrorPartitionBackfill",
        "BackfillRunsPartitionBackfillOrErrorBackfillNotFoundError",
        "BackfillRunsPartitionBackfillOrErrorPythonError",
    ] = Field(alias="partitionBackfillOrError", discriminator="typename__")


class BackfillRunsPartitionBackfillOrErrorPartitionBackfill(BaseModel):
    typename__: Literal["PartitionBackfill"] = Field(alias="__typename")
    id: str
    runs: list["BackfillRunsPartitionBackfillOrErrorPartitionBackfillRuns"]


class BackfillRunsPartitionBackfillOrErrorPartitionBackfillRuns(BaseModel):
    run_id: str = Field(alias="runId")
    status: RunStatus
    asset_selection: Optional[
        list["BackfillRunsPartitionBackfillOrErrorPartitionBackfillRunsAssetSelection"]
    ] = Field(alias="assetSelection")
    tags: list["BackfillRunsPartitionBackfillOrErrorPartitionBackfillRunsTags"]


class BackfillRunsPartitionBackfillOrErrorPartitionBackfillRunsAssetSelection(
    BaseModel
):
    path: list[str]


class BackfillRunsPartitionBackfillOrErrorPartitionBackfillRunsTags(BaseModel):
    key: str
    value: str


class BackfillRunsPartitionBackfillOrErrorBackfillNotFoundError(BaseModel):
    typename__: Literal["BackfillNotFoundError"] = Field(alias="__typename")
    message: str


class BackfillRunsPartitionBackfillOrErrorPythonError(BaseModel):
    typename__: Literal["PythonError"] = Field(alias="__typename")
    message: str


BackfillRuns.model_rebuild()
BackfillRunsPartitionBackfillOrErrorPartitionBackfill.model_rebuild()
BackfillRunsPartitionBackfillOrErrorPartitionBackfillRuns.model_rebuild()
