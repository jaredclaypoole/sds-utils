from typing import Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel
from .enums import PartitionRangeStatus


class AssetPartitionState(BaseModel):
    asset_node_or_error: Union[
        "AssetPartitionStateAssetNodeOrErrorAssetNode",
        "AssetPartitionStateAssetNodeOrErrorAssetNotFoundError",
    ] = Field(alias="assetNodeOrError", discriminator="typename__")


class AssetPartitionStateAssetNodeOrErrorAssetNode(BaseModel):
    typename__: Literal["AssetNode"] = Field(alias="__typename")
    asset_key: "AssetPartitionStateAssetNodeOrErrorAssetNodeAssetKey" = Field(
        alias="assetKey"
    )
    partition_keys: list[str] = Field(alias="partitionKeys")
    partition_definition: Optional[
        "AssetPartitionStateAssetNodeOrErrorAssetNodePartitionDefinition"
    ] = Field(alias="partitionDefinition")
    partition_keys_by_dimension: list[
        "AssetPartitionStateAssetNodeOrErrorAssetNodePartitionKeysByDimension"
    ] = Field(alias="partitionKeysByDimension")
    asset_partition_statuses: Union[
        "AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesDefaultPartitionStatuses",
        "AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatuses",
        "AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesTimePartitionStatuses",
    ] = Field(alias="assetPartitionStatuses", discriminator="typename__")


class AssetPartitionStateAssetNodeOrErrorAssetNodeAssetKey(BaseModel):
    path: list[str]


class AssetPartitionStateAssetNodeOrErrorAssetNodePartitionDefinition(BaseModel):
    dimension_types: list[
        "AssetPartitionStateAssetNodeOrErrorAssetNodePartitionDefinitionDimensionTypes"
    ] = Field(alias="dimensionTypes")


class AssetPartitionStateAssetNodeOrErrorAssetNodePartitionDefinitionDimensionTypes(
    BaseModel
):
    name: str
    is_primary_dimension: bool = Field(alias="isPrimaryDimension")


class AssetPartitionStateAssetNodeOrErrorAssetNodePartitionKeysByDimension(BaseModel):
    name: str
    partition_keys: list[str] = Field(alias="partitionKeys")


class AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesDefaultPartitionStatuses(
    BaseModel
):
    typename__: Literal["DefaultPartitionStatuses"] = Field(alias="__typename")
    materialized_partitions: list[str] = Field(alias="materializedPartitions")
    materializing_partitions: list[str] = Field(alias="materializingPartitions")
    failed_partitions: list[str] = Field(alias="failedPartitions")
    unmaterialized_partitions: list[str] = Field(alias="unmaterializedPartitions")


class AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatuses(
    BaseModel
):
    typename__: Literal["MultiPartitionStatuses"] = Field(alias="__typename")
    primary_dimension_name: str = Field(alias="primaryDimensionName")
    ranges: list[
        "AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatusesRanges"
    ]


class AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatusesRanges(
    BaseModel
):
    primary_dim_start_key: str = Field(alias="primaryDimStartKey")
    primary_dim_end_key: str = Field(alias="primaryDimEndKey")
    secondary_dim: Union[
        "AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimTimePartitionStatuses",
        "AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimDefaultPartitionStatuses",
    ] = Field(alias="secondaryDim", discriminator="typename__")


class AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimTimePartitionStatuses(
    BaseModel
):
    typename__: Literal["TimePartitionStatuses"] = Field(alias="__typename")
    ranges: list[
        "AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimTimePartitionStatusesRanges"
    ]


class AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimTimePartitionStatusesRanges(
    BaseModel
):
    start_key: str = Field(alias="startKey")
    end_key: str = Field(alias="endKey")
    status: PartitionRangeStatus


class AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimDefaultPartitionStatuses(
    BaseModel
):
    typename__: Literal["DefaultPartitionStatuses"] = Field(alias="__typename")
    materialized_partitions: list[str] = Field(alias="materializedPartitions")
    materializing_partitions: list[str] = Field(alias="materializingPartitions")
    failed_partitions: list[str] = Field(alias="failedPartitions")
    unmaterialized_partitions: list[str] = Field(alias="unmaterializedPartitions")


class AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesTimePartitionStatuses(
    BaseModel
):
    typename__: Literal["TimePartitionStatuses"] = Field(alias="__typename")
    ranges: list[
        "AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesTimePartitionStatusesRanges"
    ]


class AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesTimePartitionStatusesRanges(
    BaseModel
):
    start_key: str = Field(alias="startKey")
    end_key: str = Field(alias="endKey")
    status: PartitionRangeStatus


class AssetPartitionStateAssetNodeOrErrorAssetNotFoundError(BaseModel):
    typename__: Literal["AssetNotFoundError"] = Field(alias="__typename")
    message: str


AssetPartitionState.model_rebuild()
AssetPartitionStateAssetNodeOrErrorAssetNode.model_rebuild()
AssetPartitionStateAssetNodeOrErrorAssetNodePartitionDefinition.model_rebuild()
AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatuses.model_rebuild()
AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatusesRanges.model_rebuild()
AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimTimePartitionStatuses.model_rebuild()
AssetPartitionStateAssetNodeOrErrorAssetNodeAssetPartitionStatusesTimePartitionStatuses.model_rebuild()
