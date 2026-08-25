from typing import Annotated, Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel
from .enums import AssetMaterializationFailureReason, AssetMaterializationFailureType


class AssetPartitionHistory(BaseModel):
    asset_or_error: Union[
        "AssetPartitionHistoryAssetOrErrorAsset",
        "AssetPartitionHistoryAssetOrErrorAssetNotFoundError",
    ] = Field(alias="assetOrError", discriminator="typename__")


class AssetPartitionHistoryAssetOrErrorAsset(BaseModel):
    typename__: Literal["Asset"] = Field(alias="__typename")
    asset_event_history: "AssetPartitionHistoryAssetOrErrorAssetAssetEventHistory" = (
        Field(alias="assetEventHistory")
    )


class AssetPartitionHistoryAssetOrErrorAssetAssetEventHistory(BaseModel):
    results: list[
        Annotated[
            Union[
                "AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsFailedToMaterializeEvent",
                "AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsMaterializationEvent",
                "AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsObservationEvent",
            ],
            Field(discriminator="typename__"),
        ]
    ]
    cursor: str


class AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsFailedToMaterializeEvent(
    BaseModel
):
    typename__: Literal["FailedToMaterializeEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    partition: Optional[str]
    timestamp: str
    materialization_failure_type: AssetMaterializationFailureType = Field(
        alias="materializationFailureType"
    )
    materialization_failure_reason: AssetMaterializationFailureReason = Field(
        alias="materializationFailureReason"
    )
    metadata_entries: list[
        Annotated[
            Union[
                "AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsFailedToMaterializeEventMetadataEntriesMetadataEntry",
                "AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsFailedToMaterializeEventMetadataEntriesTextMetadataEntry",
            ],
            Field(discriminator="typename__"),
        ]
    ] = Field(alias="metadataEntries")


class AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsFailedToMaterializeEventMetadataEntriesMetadataEntry(
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


class AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsFailedToMaterializeEventMetadataEntriesTextMetadataEntry(
    BaseModel
):
    typename__: Literal["TextMetadataEntry"] = Field(alias="__typename")
    label: str
    text: str


class AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsMaterializationEvent(
    BaseModel
):
    typename__: Literal["MaterializationEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    partition: Optional[str]
    timestamp: str
    metadata_entries: list[
        Annotated[
            Union[
                "AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsMaterializationEventMetadataEntriesMetadataEntry",
                "AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsMaterializationEventMetadataEntriesTextMetadataEntry",
            ],
            Field(discriminator="typename__"),
        ]
    ] = Field(alias="metadataEntries")


class AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsMaterializationEventMetadataEntriesMetadataEntry(
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


class AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsMaterializationEventMetadataEntriesTextMetadataEntry(
    BaseModel
):
    typename__: Literal["TextMetadataEntry"] = Field(alias="__typename")
    label: str
    text: str


class AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsObservationEvent(
    BaseModel
):
    typename__: Literal["ObservationEvent"] = Field(alias="__typename")
    run_id: str = Field(alias="runId")
    step_key: Optional[str] = Field(alias="stepKey")
    partition: Optional[str]
    timestamp: str
    metadata_entries: list[
        Annotated[
            Union[
                "AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsObservationEventMetadataEntriesMetadataEntry",
                "AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsObservationEventMetadataEntriesTextMetadataEntry",
            ],
            Field(discriminator="typename__"),
        ]
    ] = Field(alias="metadataEntries")


class AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsObservationEventMetadataEntriesMetadataEntry(
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


class AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsObservationEventMetadataEntriesTextMetadataEntry(
    BaseModel
):
    typename__: Literal["TextMetadataEntry"] = Field(alias="__typename")
    label: str
    text: str


class AssetPartitionHistoryAssetOrErrorAssetNotFoundError(BaseModel):
    typename__: Literal["AssetNotFoundError"] = Field(alias="__typename")
    message: str


AssetPartitionHistory.model_rebuild()
AssetPartitionHistoryAssetOrErrorAsset.model_rebuild()
AssetPartitionHistoryAssetOrErrorAssetAssetEventHistory.model_rebuild()
AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsFailedToMaterializeEvent.model_rebuild()
AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsMaterializationEvent.model_rebuild()
AssetPartitionHistoryAssetOrErrorAssetAssetEventHistoryResultsObservationEvent.model_rebuild()
