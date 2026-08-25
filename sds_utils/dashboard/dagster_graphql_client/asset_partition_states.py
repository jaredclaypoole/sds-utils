from typing import Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel
from .enums import PartitionRangeStatus


class AssetPartitionStates(BaseModel):
    asset_nodes: list["AssetPartitionStatesAssetNodes"] = Field(alias="assetNodes")


class AssetPartitionStatesAssetNodes(BaseModel):
    asset_key: "AssetPartitionStatesAssetNodesAssetKey" = Field(alias="assetKey")
    partition_keys: list[str] = Field(alias="partitionKeys")
    partition_definition: Optional[
        "AssetPartitionStatesAssetNodesPartitionDefinition"
    ] = Field(alias="partitionDefinition")
    partition_keys_by_dimension: list[
        "AssetPartitionStatesAssetNodesPartitionKeysByDimension"
    ] = Field(alias="partitionKeysByDimension")
    asset_partition_statuses: Union[
        "AssetPartitionStatesAssetNodesAssetPartitionStatusesDefaultPartitionStatuses",
        "AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatuses",
        "AssetPartitionStatesAssetNodesAssetPartitionStatusesTimePartitionStatuses",
    ] = Field(alias="assetPartitionStatuses", discriminator="typename__")


class AssetPartitionStatesAssetNodesAssetKey(BaseModel):
    path: list[str]


class AssetPartitionStatesAssetNodesPartitionDefinition(BaseModel):
    dimension_types: list[
        "AssetPartitionStatesAssetNodesPartitionDefinitionDimensionTypes"
    ] = Field(alias="dimensionTypes")


class AssetPartitionStatesAssetNodesPartitionDefinitionDimensionTypes(BaseModel):
    name: str
    is_primary_dimension: bool = Field(alias="isPrimaryDimension")


class AssetPartitionStatesAssetNodesPartitionKeysByDimension(BaseModel):
    name: str
    partition_keys: list[str] = Field(alias="partitionKeys")


class AssetPartitionStatesAssetNodesAssetPartitionStatusesDefaultPartitionStatuses(
    BaseModel
):
    typename__: Literal["DefaultPartitionStatuses"] = Field(alias="__typename")
    materialized_partitions: list[str] = Field(alias="materializedPartitions")
    materializing_partitions: list[str] = Field(alias="materializingPartitions")
    failed_partitions: list[str] = Field(alias="failedPartitions")
    unmaterialized_partitions: list[str] = Field(alias="unmaterializedPartitions")


class AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatuses(
    BaseModel
):
    typename__: Literal["MultiPartitionStatuses"] = Field(alias="__typename")
    primary_dimension_name: str = Field(alias="primaryDimensionName")
    ranges: list[
        "AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatusesRanges"
    ]


class AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatusesRanges(
    BaseModel
):
    primary_dim_start_key: str = Field(alias="primaryDimStartKey")
    primary_dim_end_key: str = Field(alias="primaryDimEndKey")
    secondary_dim: Union[
        "AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimTimePartitionStatuses",
        "AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimDefaultPartitionStatuses",
    ] = Field(alias="secondaryDim", discriminator="typename__")


class AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimTimePartitionStatuses(
    BaseModel
):
    typename__: Literal["TimePartitionStatuses"] = Field(alias="__typename")
    ranges: list[
        "AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimTimePartitionStatusesRanges"
    ]


class AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimTimePartitionStatusesRanges(
    BaseModel
):
    start_key: str = Field(alias="startKey")
    end_key: str = Field(alias="endKey")
    status: PartitionRangeStatus


class AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimDefaultPartitionStatuses(
    BaseModel
):
    typename__: Literal["DefaultPartitionStatuses"] = Field(alias="__typename")
    materialized_partitions: list[str] = Field(alias="materializedPartitions")
    materializing_partitions: list[str] = Field(alias="materializingPartitions")
    failed_partitions: list[str] = Field(alias="failedPartitions")
    unmaterialized_partitions: list[str] = Field(alias="unmaterializedPartitions")


class AssetPartitionStatesAssetNodesAssetPartitionStatusesTimePartitionStatuses(
    BaseModel
):
    typename__: Literal["TimePartitionStatuses"] = Field(alias="__typename")
    ranges: list[
        "AssetPartitionStatesAssetNodesAssetPartitionStatusesTimePartitionStatusesRanges"
    ]


class AssetPartitionStatesAssetNodesAssetPartitionStatusesTimePartitionStatusesRanges(
    BaseModel
):
    start_key: str = Field(alias="startKey")
    end_key: str = Field(alias="endKey")
    status: PartitionRangeStatus


AssetPartitionStates.model_rebuild()
AssetPartitionStatesAssetNodes.model_rebuild()
AssetPartitionStatesAssetNodesPartitionDefinition.model_rebuild()
AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatuses.model_rebuild()
AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatusesRanges.model_rebuild()
AssetPartitionStatesAssetNodesAssetPartitionStatusesMultiPartitionStatusesRangesSecondaryDimTimePartitionStatuses.model_rebuild()
AssetPartitionStatesAssetNodesAssetPartitionStatusesTimePartitionStatuses.model_rebuild()
