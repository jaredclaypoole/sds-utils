from typing import Annotated, Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel
from .enums import BulkActionStatus


class BackfillDetails(BaseModel):
    partition_backfill_or_error: Union[
        "BackfillDetailsPartitionBackfillOrErrorPartitionBackfill",
        "BackfillDetailsPartitionBackfillOrErrorBackfillNotFoundError",
        "BackfillDetailsPartitionBackfillOrErrorPythonError",
    ] = Field(alias="partitionBackfillOrError", discriminator="typename__")


class BackfillDetailsPartitionBackfillOrErrorPartitionBackfill(BaseModel):
    typename__: Literal["PartitionBackfill"] = Field(alias="__typename")
    id: str
    status: BulkActionStatus
    title: Optional[str]
    description: Optional[str]
    creation_time: float = Field(alias="creationTime")
    end_time: Optional[float] = Field(alias="endTime")
    num_partitions: Optional[int] = Field(alias="numPartitions")
    partition_names: Optional[list[str]] = Field(alias="partitionNames")
    is_asset_backfill: bool = Field(alias="isAssetBackfill")
    asset_backfill_data: Optional[
        "BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillData"
    ] = Field(alias="assetBackfillData")


class BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillData(
    BaseModel
):
    asset_backfill_statuses: list[
        Annotated[
            Union[
                "BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillDataAssetBackfillStatusesAssetPartitionsStatusCounts",
                "BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillDataAssetBackfillStatusesUnpartitionedAssetStatus",
            ],
            Field(discriminator="typename__"),
        ]
    ] = Field(alias="assetBackfillStatuses")


class BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillDataAssetBackfillStatusesAssetPartitionsStatusCounts(
    BaseModel
):
    typename__: Literal["AssetPartitionsStatusCounts"] = Field(alias="__typename")
    asset_key: "BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillDataAssetBackfillStatusesAssetPartitionsStatusCountsAssetKey" = Field(
        alias="assetKey"
    )
    num_partitions_targeted: int = Field(alias="numPartitionsTargeted")
    num_partitions_in_progress: int = Field(alias="numPartitionsInProgress")
    num_partitions_materialized: int = Field(alias="numPartitionsMaterialized")
    num_partitions_failed: int = Field(alias="numPartitionsFailed")


class BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillDataAssetBackfillStatusesAssetPartitionsStatusCountsAssetKey(
    BaseModel
):
    path: list[str]


class BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillDataAssetBackfillStatusesUnpartitionedAssetStatus(
    BaseModel
):
    typename__: Literal["UnpartitionedAssetStatus"] = Field(alias="__typename")
    asset_key: "BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillDataAssetBackfillStatusesUnpartitionedAssetStatusAssetKey" = Field(
        alias="assetKey"
    )
    in_progress: bool = Field(alias="inProgress")
    materialized: bool
    failed: bool


class BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillDataAssetBackfillStatusesUnpartitionedAssetStatusAssetKey(
    BaseModel
):
    path: list[str]


class BackfillDetailsPartitionBackfillOrErrorBackfillNotFoundError(BaseModel):
    typename__: Literal["BackfillNotFoundError"] = Field(alias="__typename")
    message: str


class BackfillDetailsPartitionBackfillOrErrorPythonError(BaseModel):
    typename__: Literal["PythonError"] = Field(alias="__typename")
    message: str


BackfillDetails.model_rebuild()
BackfillDetailsPartitionBackfillOrErrorPartitionBackfill.model_rebuild()
BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillData.model_rebuild()
BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillDataAssetBackfillStatusesAssetPartitionsStatusCounts.model_rebuild()
BackfillDetailsPartitionBackfillOrErrorPartitionBackfillAssetBackfillDataAssetBackfillStatusesUnpartitionedAssetStatus.model_rebuild()
