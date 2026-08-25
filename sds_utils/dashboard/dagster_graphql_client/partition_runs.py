from typing import Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel
from .enums import RunStatus


class PartitionRuns(BaseModel):
    runs_or_error: Union[
        "PartitionRunsRunsOrErrorRuns",
        "PartitionRunsRunsOrErrorInvalidPipelineRunsFilterError",
        "PartitionRunsRunsOrErrorPythonError",
    ] = Field(alias="runsOrError", discriminator="typename__")


class PartitionRunsRunsOrErrorRuns(BaseModel):
    typename__: Literal["Runs"] = Field(alias="__typename")
    results: list["PartitionRunsRunsOrErrorRunsResults"]


class PartitionRunsRunsOrErrorRunsResults(BaseModel):
    run_id: str = Field(alias="runId")
    status: RunStatus
    update_time: Optional[float] = Field(alias="updateTime")
    asset_selection: Optional[
        list["PartitionRunsRunsOrErrorRunsResultsAssetSelection"]
    ] = Field(alias="assetSelection")


class PartitionRunsRunsOrErrorRunsResultsAssetSelection(BaseModel):
    path: list[str]


class PartitionRunsRunsOrErrorInvalidPipelineRunsFilterError(BaseModel):
    typename__: Literal["InvalidPipelineRunsFilterError"] = Field(alias="__typename")
    message: str


class PartitionRunsRunsOrErrorPythonError(BaseModel):
    typename__: Literal["PythonError"] = Field(alias="__typename")
    message: str
    stack: list[str]


PartitionRuns.model_rebuild()
PartitionRunsRunsOrErrorRuns.model_rebuild()
PartitionRunsRunsOrErrorRunsResults.model_rebuild()
