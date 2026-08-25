from typing import Annotated, Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel
from .enums import AssetMaterializationFailureReason, AssetMaterializationFailureType


class RunAssetActivity(BaseModel):
    run_or_error: Union[
        "RunAssetActivityRunOrErrorRun",
        "RunAssetActivityRunOrErrorRunNotFoundError",
        "RunAssetActivityRunOrErrorPythonError",
    ] = Field(alias="runOrError", discriminator="typename__")


class RunAssetActivityRunOrErrorRun(BaseModel):
    typename__: Literal["Run"] = Field(alias="__typename")
    event_connection: "RunAssetActivityRunOrErrorRunEventConnection" = Field(
        alias="eventConnection"
    )


class RunAssetActivityRunOrErrorRunEventConnection(BaseModel):
    events: list[
        Annotated[
            Union[
                "RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepFailureEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepInputEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepOutputEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepSkippedEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepStartEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepSuccessEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepUpForRetryEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepRestartEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsHealthChangedEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsLogMessageEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsResourceInitFailureEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsResourceInitStartedEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsResourceInitSuccessEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsRunFailureEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsRunStartEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsRunEnqueuedEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsRunDequeuedEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsRunStartingEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsRunCancelingEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsRunCanceledEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsRunSuccessEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsStepWorkerStartedEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsStepWorkerStartingEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsHandledOutputEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsLoadedInputEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsLogsCapturedEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsObjectStoreOperationEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsStepExpectationResultEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsMaterializationEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsObservationEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsFailedToMaterializeEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsEngineEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsHookCompletedEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsHookSkippedEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsHookErroredEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsAlertStartEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsAlertSuccessEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsAlertFailureEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsAssetMaterializationPlannedEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsAssetCheckEvaluationPlannedEvent",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsAssetCheckEvaluationEvent",
            ],
            Field(discriminator="typename__"),
        ]
    ]
    cursor: str
    has_more: bool = Field(alias="hasMore")


class RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepFailureEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepFailureEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepInputEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepInputEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepOutputEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepOutputEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepSkippedEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepSkippedEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepStartEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepStartEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepSuccessEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepSuccessEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepUpForRetryEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepUpForRetryEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsExecutionStepRestartEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepRestartEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsHealthChangedEvent(BaseModel):
    typename__: Literal["HealthChangedEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsLogMessageEvent(BaseModel):
    typename__: Literal["LogMessageEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    timestamp: str
    message: str


class RunAssetActivityRunOrErrorRunEventConnectionEventsResourceInitFailureEvent(
    BaseModel
):
    typename__: Literal["ResourceInitFailureEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsResourceInitStartedEvent(
    BaseModel
):
    typename__: Literal["ResourceInitStartedEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsResourceInitSuccessEvent(
    BaseModel
):
    typename__: Literal["ResourceInitSuccessEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsRunFailureEvent(BaseModel):
    typename__: Literal["RunFailureEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    timestamp: str


class RunAssetActivityRunOrErrorRunEventConnectionEventsRunStartEvent(BaseModel):
    typename__: Literal["RunStartEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsRunEnqueuedEvent(BaseModel):
    typename__: Literal["RunEnqueuedEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsRunDequeuedEvent(BaseModel):
    typename__: Literal["RunDequeuedEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsRunStartingEvent(BaseModel):
    typename__: Literal["RunStartingEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsRunCancelingEvent(BaseModel):
    typename__: Literal["RunCancelingEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsRunCanceledEvent(BaseModel):
    typename__: Literal["RunCanceledEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsRunSuccessEvent(BaseModel):
    typename__: Literal["RunSuccessEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsStepWorkerStartedEvent(
    BaseModel
):
    typename__: Literal["StepWorkerStartedEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsStepWorkerStartingEvent(
    BaseModel
):
    typename__: Literal["StepWorkerStartingEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsHandledOutputEvent(BaseModel):
    typename__: Literal["HandledOutputEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsLoadedInputEvent(BaseModel):
    typename__: Literal["LoadedInputEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsLogsCapturedEvent(BaseModel):
    typename__: Literal["LogsCapturedEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsObjectStoreOperationEvent(
    BaseModel
):
    typename__: Literal["ObjectStoreOperationEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsStepExpectationResultEvent(
    BaseModel
):
    typename__: Literal["StepExpectationResultEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsMaterializationEvent(BaseModel):
    typename__: Literal["MaterializationEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    timestamp: str
    partition: Optional[str]
    asset_key: Optional[
        "RunAssetActivityRunOrErrorRunEventConnectionEventsMaterializationEventAssetKey"
    ] = Field(alias="assetKey")


class RunAssetActivityRunOrErrorRunEventConnectionEventsMaterializationEventAssetKey(
    BaseModel
):
    path: list[str]


class RunAssetActivityRunOrErrorRunEventConnectionEventsObservationEvent(BaseModel):
    typename__: Literal["ObservationEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    timestamp: str
    partition: Optional[str]
    asset_key: Optional[
        "RunAssetActivityRunOrErrorRunEventConnectionEventsObservationEventAssetKey"
    ] = Field(alias="assetKey")
    metadata_entries: list[
        Annotated[
            Union[
                "RunAssetActivityRunOrErrorRunEventConnectionEventsObservationEventMetadataEntriesMetadataEntry",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsObservationEventMetadataEntriesTextMetadataEntry",
            ],
            Field(discriminator="typename__"),
        ]
    ] = Field(alias="metadataEntries")


class RunAssetActivityRunOrErrorRunEventConnectionEventsObservationEventAssetKey(
    BaseModel
):
    path: list[str]


class RunAssetActivityRunOrErrorRunEventConnectionEventsObservationEventMetadataEntriesMetadataEntry(
    BaseModel
):
    typename__: Literal[
        "AssetMetadataEntry",
        "BoolMetadataEntry",
        "CodeReferencesMetadataEntry",
        "FloatMetadataEntry",
        "IntMetadataEntry",
        "JobMetadataEntry",
        "JsonMetadataEntry",
        "MarkdownMetadataEntry",
        "MetadataEntry",
        "NotebookMetadataEntry",
        "NullMetadataEntry",
        "PathMetadataEntry",
        "PipelineRunMetadataEntry",
        "PoolMetadataEntry",
        "PythonArtifactMetadataEntry",
        "TableColumnLineageMetadataEntry",
        "TableMetadataEntry",
        "TableSchemaMetadataEntry",
        "TimestampMetadataEntry",
        "UrlMetadataEntry",
    ] = Field(alias="__typename")
    label: str


class RunAssetActivityRunOrErrorRunEventConnectionEventsObservationEventMetadataEntriesTextMetadataEntry(
    BaseModel
):
    typename__: Literal["TextMetadataEntry"] = Field(alias="__typename")
    label: str
    text: str


class RunAssetActivityRunOrErrorRunEventConnectionEventsFailedToMaterializeEvent(
    BaseModel
):
    typename__: Literal["FailedToMaterializeEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    timestamp: str
    partition: Optional[str]
    asset_key: Optional[
        "RunAssetActivityRunOrErrorRunEventConnectionEventsFailedToMaterializeEventAssetKey"
    ] = Field(alias="assetKey")
    materialization_failure_type: AssetMaterializationFailureType = Field(
        alias="materializationFailureType"
    )
    materialization_failure_reason: AssetMaterializationFailureReason = Field(
        alias="materializationFailureReason"
    )
    metadata_entries: list[
        Annotated[
            Union[
                "RunAssetActivityRunOrErrorRunEventConnectionEventsFailedToMaterializeEventMetadataEntriesMetadataEntry",
                "RunAssetActivityRunOrErrorRunEventConnectionEventsFailedToMaterializeEventMetadataEntriesTextMetadataEntry",
            ],
            Field(discriminator="typename__"),
        ]
    ] = Field(alias="metadataEntries")


class RunAssetActivityRunOrErrorRunEventConnectionEventsFailedToMaterializeEventAssetKey(
    BaseModel
):
    path: list[str]


class RunAssetActivityRunOrErrorRunEventConnectionEventsFailedToMaterializeEventMetadataEntriesMetadataEntry(
    BaseModel
):
    typename__: Literal[
        "AssetMetadataEntry",
        "BoolMetadataEntry",
        "CodeReferencesMetadataEntry",
        "FloatMetadataEntry",
        "IntMetadataEntry",
        "JobMetadataEntry",
        "JsonMetadataEntry",
        "MarkdownMetadataEntry",
        "MetadataEntry",
        "NotebookMetadataEntry",
        "NullMetadataEntry",
        "PathMetadataEntry",
        "PipelineRunMetadataEntry",
        "PoolMetadataEntry",
        "PythonArtifactMetadataEntry",
        "TableColumnLineageMetadataEntry",
        "TableMetadataEntry",
        "TableSchemaMetadataEntry",
        "TimestampMetadataEntry",
        "UrlMetadataEntry",
    ] = Field(alias="__typename")
    label: str


class RunAssetActivityRunOrErrorRunEventConnectionEventsFailedToMaterializeEventMetadataEntriesTextMetadataEntry(
    BaseModel
):
    typename__: Literal["TextMetadataEntry"] = Field(alias="__typename")
    label: str
    text: str


class RunAssetActivityRunOrErrorRunEventConnectionEventsEngineEvent(BaseModel):
    typename__: Literal["EngineEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsHookCompletedEvent(BaseModel):
    typename__: Literal["HookCompletedEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsHookSkippedEvent(BaseModel):
    typename__: Literal["HookSkippedEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsHookErroredEvent(BaseModel):
    typename__: Literal["HookErroredEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsAlertStartEvent(BaseModel):
    typename__: Literal["AlertStartEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsAlertSuccessEvent(BaseModel):
    typename__: Literal["AlertSuccessEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsAlertFailureEvent(BaseModel):
    typename__: Literal["AlertFailureEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsAssetMaterializationPlannedEvent(
    BaseModel
):
    typename__: Literal["AssetMaterializationPlannedEvent"] = Field(alias="__typename")
    timestamp: str
    asset_key: Optional[
        "RunAssetActivityRunOrErrorRunEventConnectionEventsAssetMaterializationPlannedEventAssetKey"
    ] = Field(alias="assetKey")


class RunAssetActivityRunOrErrorRunEventConnectionEventsAssetMaterializationPlannedEventAssetKey(
    BaseModel
):
    path: list[str]


class RunAssetActivityRunOrErrorRunEventConnectionEventsAssetCheckEvaluationPlannedEvent(
    BaseModel
):
    typename__: Literal["AssetCheckEvaluationPlannedEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunEventConnectionEventsAssetCheckEvaluationEvent(
    BaseModel
):
    typename__: Literal["AssetCheckEvaluationEvent"] = Field(alias="__typename")


class RunAssetActivityRunOrErrorRunNotFoundError(BaseModel):
    typename__: Literal["RunNotFoundError"] = Field(alias="__typename")
    message: str


class RunAssetActivityRunOrErrorPythonError(BaseModel):
    typename__: Literal["PythonError"] = Field(alias="__typename")
    message: str
    stack: list[str]


RunAssetActivity.model_rebuild()
RunAssetActivityRunOrErrorRun.model_rebuild()
RunAssetActivityRunOrErrorRunEventConnection.model_rebuild()
RunAssetActivityRunOrErrorRunEventConnectionEventsMaterializationEvent.model_rebuild()
RunAssetActivityRunOrErrorRunEventConnectionEventsObservationEvent.model_rebuild()
RunAssetActivityRunOrErrorRunEventConnectionEventsFailedToMaterializeEvent.model_rebuild()
RunAssetActivityRunOrErrorRunEventConnectionEventsAssetMaterializationPlannedEvent.model_rebuild()
