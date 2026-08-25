from typing import Optional

from pydantic import Field

from .base_model import BaseModel


class AssetStatus(BaseModel):
    asset_nodes: list["AssetStatusAssetNodes"] = Field(alias="assetNodes")


class AssetStatusAssetNodes(BaseModel):
    asset_key: "AssetStatusAssetNodesAssetKey" = Field(alias="assetKey")
    asset_materializations: list["AssetStatusAssetNodesAssetMaterializations"] = Field(
        alias="assetMaterializations"
    )
    partition_stats: Optional["AssetStatusAssetNodesPartitionStats"] = Field(
        alias="partitionStats"
    )


class AssetStatusAssetNodesAssetKey(BaseModel):
    path: list[str]


class AssetStatusAssetNodesAssetMaterializations(BaseModel):
    timestamp: str
    run_id: str = Field(alias="runId")


class AssetStatusAssetNodesPartitionStats(BaseModel):
    num_partitions: int = Field(alias="numPartitions")
    num_materialized: int = Field(alias="numMaterialized")
    num_failed: int = Field(alias="numFailed")
    num_materializing: int = Field(alias="numMaterializing")


AssetStatus.model_rebuild()
AssetStatusAssetNodes.model_rebuild()
