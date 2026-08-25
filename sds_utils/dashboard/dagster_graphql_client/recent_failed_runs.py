from typing import Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel
from .enums import RunStatus


class RecentFailedRuns(BaseModel):
    runs_or_error: Union[
        "RecentFailedRunsRunsOrErrorRuns",
        "RecentFailedRunsRunsOrErrorInvalidPipelineRunsFilterError",
        "RecentFailedRunsRunsOrErrorPythonError",
    ] = Field(alias="runsOrError", discriminator="typename__")


class RecentFailedRunsRunsOrErrorRuns(BaseModel):
    typename__: Literal["Runs"] = Field(alias="__typename")
    results: list["RecentFailedRunsRunsOrErrorRunsResults"]


class RecentFailedRunsRunsOrErrorRunsResults(BaseModel):
    run_id: str = Field(alias="runId")
    job_name: str = Field(alias="jobName")
    status: RunStatus
    start_time: Optional[float] = Field(alias="startTime")
    end_time: Optional[float] = Field(alias="endTime")
    update_time: Optional[float] = Field(alias="updateTime")


class RecentFailedRunsRunsOrErrorInvalidPipelineRunsFilterError(BaseModel):
    typename__: Literal["InvalidPipelineRunsFilterError"] = Field(alias="__typename")
    message: str


class RecentFailedRunsRunsOrErrorPythonError(BaseModel):
    typename__: Literal["PythonError"] = Field(alias="__typename")
    message: str
    stack: list[str]


RecentFailedRuns.model_rebuild()
RecentFailedRunsRunsOrErrorRuns.model_rebuild()
