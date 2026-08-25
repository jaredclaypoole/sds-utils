from typing import Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel
from .enums import RunStatus


class RecentAssetActivity(BaseModel):
    runs_or_error: Union[
        "RecentAssetActivityRunsOrErrorRuns",
        "RecentAssetActivityRunsOrErrorInvalidPipelineRunsFilterError",
        "RecentAssetActivityRunsOrErrorPythonError",
    ] = Field(alias="runsOrError", discriminator="typename__")


class RecentAssetActivityRunsOrErrorRuns(BaseModel):
    typename__: Literal["Runs"] = Field(alias="__typename")
    results: list["RecentAssetActivityRunsOrErrorRunsResults"]


class RecentAssetActivityRunsOrErrorRunsResults(BaseModel):
    run_id: str = Field(alias="runId")
    status: RunStatus
    update_time: Optional[float] = Field(alias="updateTime")
    tags: list["RecentAssetActivityRunsOrErrorRunsResultsTags"]
    asset_selection: Optional[
        list["RecentAssetActivityRunsOrErrorRunsResultsAssetSelection"]
    ] = Field(alias="assetSelection")


class RecentAssetActivityRunsOrErrorRunsResultsTags(BaseModel):
    key: str
    value: str


class RecentAssetActivityRunsOrErrorRunsResultsAssetSelection(BaseModel):
    path: list[str]


class RecentAssetActivityRunsOrErrorInvalidPipelineRunsFilterError(BaseModel):
    typename__: Literal["InvalidPipelineRunsFilterError"] = Field(alias="__typename")
    message: str


class RecentAssetActivityRunsOrErrorPythonError(BaseModel):
    typename__: Literal["PythonError"] = Field(alias="__typename")
    message: str
    stack: list[str]


RecentAssetActivity.model_rebuild()
RecentAssetActivityRunsOrErrorRuns.model_rebuild()
RecentAssetActivityRunsOrErrorRunsResults.model_rebuild()
