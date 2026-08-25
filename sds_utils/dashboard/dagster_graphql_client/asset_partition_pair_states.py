from typing import Optional

from pydantic import Field

from .base_model import BaseModel
from .enums import RunStatus


class AssetPartitionPairStates(BaseModel):
    asset_nodes: list["AssetPartitionPairStatesAssetNodes"] = Field(alias="assetNodes")


class AssetPartitionPairStatesAssetNodes(BaseModel):
    asset_key: "AssetPartitionPairStatesAssetNodesAssetKey" = Field(alias="assetKey")
    latest_materialization_by_partition: list[
        Optional["AssetPartitionPairStatesAssetNodesLatestMaterializationByPartition"]
    ] = Field(alias="latestMaterializationByPartition")
    latest_run_for_partition: Optional[
        "AssetPartitionPairStatesAssetNodesLatestRunForPartition"
    ] = Field(alias="latestRunForPartition")


class AssetPartitionPairStatesAssetNodesAssetKey(BaseModel):
    path: list[str]


class AssetPartitionPairStatesAssetNodesLatestMaterializationByPartition(BaseModel):
    run_id: str = Field(alias="runId")
    timestamp: str
    partition: Optional[str]
    asset_key: Optional[
        "AssetPartitionPairStatesAssetNodesLatestMaterializationByPartitionAssetKey"
    ] = Field(alias="assetKey")


class AssetPartitionPairStatesAssetNodesLatestMaterializationByPartitionAssetKey(
    BaseModel
):
    path: list[str]


class AssetPartitionPairStatesAssetNodesLatestRunForPartition(BaseModel):
    run_id: str = Field(alias="runId")
    status: RunStatus
    update_time: Optional[float] = Field(alias="updateTime")
    asset_selection: Optional[
        list["AssetPartitionPairStatesAssetNodesLatestRunForPartitionAssetSelection"]
    ] = Field(alias="assetSelection")


class AssetPartitionPairStatesAssetNodesLatestRunForPartitionAssetSelection(BaseModel):
    path: list[str]


AssetPartitionPairStates.model_rebuild()
AssetPartitionPairStatesAssetNodes.model_rebuild()
AssetPartitionPairStatesAssetNodesLatestMaterializationByPartition.model_rebuild()
AssetPartitionPairStatesAssetNodesLatestRunForPartition.model_rebuild()
