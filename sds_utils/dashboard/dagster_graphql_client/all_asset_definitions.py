from pydantic import Field

from .base_model import BaseModel


class AllAssetDefinitions(BaseModel):
    asset_nodes: list["AllAssetDefinitionsAssetNodes"] = Field(alias="assetNodes")


class AllAssetDefinitionsAssetNodes(BaseModel):
    asset_key: "AllAssetDefinitionsAssetNodesAssetKey" = Field(alias="assetKey")
    is_partitioned: bool = Field(alias="isPartitioned")
    partition_keys: list[str] = Field(alias="partitionKeys")


class AllAssetDefinitionsAssetNodesAssetKey(BaseModel):
    path: list[str]


AllAssetDefinitions.model_rebuild()
AllAssetDefinitionsAssetNodes.model_rebuild()
