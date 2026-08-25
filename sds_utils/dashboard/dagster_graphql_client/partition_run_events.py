from typing import Annotated, Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel
from .enums import AssetMaterializationFailureReason, AssetMaterializationFailureType


class PartitionRunEvents(BaseModel):
    runs_or_error: Union[
        "PartitionRunEventsRunsOrErrorRuns",
        "PartitionRunEventsRunsOrErrorInvalidPipelineRunsFilterError",
        "PartitionRunEventsRunsOrErrorPythonError",
    ] = Field(alias="runsOrError", discriminator="typename__")


class PartitionRunEventsRunsOrErrorRuns(BaseModel):
    typename__: Literal["Runs"] = Field(alias="__typename")
    results: list["PartitionRunEventsRunsOrErrorRunsResults"]


class PartitionRunEventsRunsOrErrorRunsResults(BaseModel):
    run_id: str = Field(alias="runId")
    event_connection: "PartitionRunEventsRunsOrErrorRunsResultsEventConnection" = Field(
        alias="eventConnection"
    )


class PartitionRunEventsRunsOrErrorRunsResultsEventConnection(BaseModel):
    events: list[
        Annotated[
            Union[
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepFailureEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepInputEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepOutputEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepSkippedEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepStartEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepSuccessEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepUpForRetryEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepRestartEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsHealthChangedEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsLogMessageEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsResourceInitFailureEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsResourceInitStartedEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsResourceInitSuccessEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunFailureEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunStartEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunEnqueuedEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunDequeuedEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunStartingEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunCancelingEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunCanceledEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunSuccessEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsStepWorkerStartedEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsStepWorkerStartingEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsHandledOutputEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsLoadedInputEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsLogsCapturedEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsObjectStoreOperationEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsStepExpectationResultEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsMaterializationEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsObservationEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsFailedToMaterializeEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsEngineEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsHookCompletedEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsHookSkippedEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsHookErroredEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAlertStartEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAlertSuccessEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAlertFailureEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAssetMaterializationPlannedEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAssetCheckEvaluationPlannedEvent",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAssetCheckEvaluationEvent",
            ],
            Field(discriminator="typename__"),
        ]
    ]
    cursor: str
    has_more: bool = Field(alias="hasMore")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepFailureEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepFailureEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepInputEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepInputEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepOutputEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepOutputEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepSkippedEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepSkippedEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepStartEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepStartEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepSuccessEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepSuccessEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepUpForRetryEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepUpForRetryEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsExecutionStepRestartEvent(
    BaseModel
):
    typename__: Literal["ExecutionStepRestartEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsHealthChangedEvent(
    BaseModel
):
    typename__: Literal["HealthChangedEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsLogMessageEvent(
    BaseModel
):
    typename__: Literal["LogMessageEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    timestamp: str
    message: str


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsResourceInitFailureEvent(
    BaseModel
):
    typename__: Literal["ResourceInitFailureEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsResourceInitStartedEvent(
    BaseModel
):
    typename__: Literal["ResourceInitStartedEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsResourceInitSuccessEvent(
    BaseModel
):
    typename__: Literal["ResourceInitSuccessEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunFailureEvent(
    BaseModel
):
    typename__: Literal["RunFailureEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    timestamp: str


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunStartEvent(
    BaseModel
):
    typename__: Literal["RunStartEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunEnqueuedEvent(
    BaseModel
):
    typename__: Literal["RunEnqueuedEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunDequeuedEvent(
    BaseModel
):
    typename__: Literal["RunDequeuedEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunStartingEvent(
    BaseModel
):
    typename__: Literal["RunStartingEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunCancelingEvent(
    BaseModel
):
    typename__: Literal["RunCancelingEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunCanceledEvent(
    BaseModel
):
    typename__: Literal["RunCanceledEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsRunSuccessEvent(
    BaseModel
):
    typename__: Literal["RunSuccessEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsStepWorkerStartedEvent(
    BaseModel
):
    typename__: Literal["StepWorkerStartedEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsStepWorkerStartingEvent(
    BaseModel
):
    typename__: Literal["StepWorkerStartingEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsHandledOutputEvent(
    BaseModel
):
    typename__: Literal["HandledOutputEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsLoadedInputEvent(
    BaseModel
):
    typename__: Literal["LoadedInputEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsLogsCapturedEvent(
    BaseModel
):
    typename__: Literal["LogsCapturedEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsObjectStoreOperationEvent(
    BaseModel
):
    typename__: Literal["ObjectStoreOperationEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsStepExpectationResultEvent(
    BaseModel
):
    typename__: Literal["StepExpectationResultEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsMaterializationEvent(
    BaseModel
):
    typename__: Literal["MaterializationEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    timestamp: str
    partition: Optional[str]
    asset_key: Optional[
        "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsMaterializationEventAssetKey"
    ] = Field(alias="assetKey")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsMaterializationEventAssetKey(
    BaseModel
):
    path: list[str]


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsObservationEvent(
    BaseModel
):
    typename__: Literal["ObservationEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    timestamp: str
    partition: Optional[str]
    asset_key: Optional[
        "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsObservationEventAssetKey"
    ] = Field(alias="assetKey")
    metadata_entries: list[
        Annotated[
            Union[
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsObservationEventMetadataEntriesMetadataEntry",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsObservationEventMetadataEntriesTextMetadataEntry",
            ],
            Field(discriminator="typename__"),
        ]
    ] = Field(alias="metadataEntries")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsObservationEventAssetKey(
    BaseModel
):
    path: list[str]


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsObservationEventMetadataEntriesMetadataEntry(
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


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsObservationEventMetadataEntriesTextMetadataEntry(
    BaseModel
):
    typename__: Literal["TextMetadataEntry"] = Field(alias="__typename")
    label: str
    text: str


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsFailedToMaterializeEvent(
    BaseModel
):
    typename__: Literal["FailedToMaterializeEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    timestamp: str
    partition: Optional[str]
    asset_key: Optional[
        "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsFailedToMaterializeEventAssetKey"
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
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsFailedToMaterializeEventMetadataEntriesMetadataEntry",
                "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsFailedToMaterializeEventMetadataEntriesTextMetadataEntry",
            ],
            Field(discriminator="typename__"),
        ]
    ] = Field(alias="metadataEntries")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsFailedToMaterializeEventAssetKey(
    BaseModel
):
    path: list[str]


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsFailedToMaterializeEventMetadataEntriesMetadataEntry(
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


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsFailedToMaterializeEventMetadataEntriesTextMetadataEntry(
    BaseModel
):
    typename__: Literal["TextMetadataEntry"] = Field(alias="__typename")
    label: str
    text: str


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsEngineEvent(
    BaseModel
):
    typename__: Literal["EngineEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsHookCompletedEvent(
    BaseModel
):
    typename__: Literal["HookCompletedEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsHookSkippedEvent(
    BaseModel
):
    typename__: Literal["HookSkippedEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsHookErroredEvent(
    BaseModel
):
    typename__: Literal["HookErroredEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAlertStartEvent(
    BaseModel
):
    typename__: Literal["AlertStartEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAlertSuccessEvent(
    BaseModel
):
    typename__: Literal["AlertSuccessEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAlertFailureEvent(
    BaseModel
):
    typename__: Literal["AlertFailureEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAssetMaterializationPlannedEvent(
    BaseModel
):
    typename__: Literal["AssetMaterializationPlannedEvent"] = Field(alias="__typename")
    timestamp: str
    asset_key: Optional[
        "PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAssetMaterializationPlannedEventAssetKey"
    ] = Field(alias="assetKey")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAssetMaterializationPlannedEventAssetKey(
    BaseModel
):
    path: list[str]


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAssetCheckEvaluationPlannedEvent(
    BaseModel
):
    typename__: Literal["AssetCheckEvaluationPlannedEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAssetCheckEvaluationEvent(
    BaseModel
):
    typename__: Literal["AssetCheckEvaluationEvent"] = Field(alias="__typename")


class PartitionRunEventsRunsOrErrorInvalidPipelineRunsFilterError(BaseModel):
    typename__: Literal["InvalidPipelineRunsFilterError"] = Field(alias="__typename")
    message: str


class PartitionRunEventsRunsOrErrorPythonError(BaseModel):
    typename__: Literal["PythonError"] = Field(alias="__typename")
    message: str
    stack: list[str]


PartitionRunEvents.model_rebuild()
PartitionRunEventsRunsOrErrorRuns.model_rebuild()
PartitionRunEventsRunsOrErrorRunsResults.model_rebuild()
PartitionRunEventsRunsOrErrorRunsResultsEventConnection.model_rebuild()
PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsMaterializationEvent.model_rebuild()
PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsObservationEvent.model_rebuild()
PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsFailedToMaterializeEvent.model_rebuild()
PartitionRunEventsRunsOrErrorRunsResultsEventConnectionEventsAssetMaterializationPlannedEvent.model_rebuild()
