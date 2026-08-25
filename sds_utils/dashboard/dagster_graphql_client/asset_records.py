from typing import Literal, Optional, Union

from pydantic import Field

from .base_model import BaseModel


class AssetRecords(BaseModel):
    asset_records_or_error: Union[
        "AssetRecordsAssetRecordsOrErrorAssetRecordConnection",
        "AssetRecordsAssetRecordsOrErrorPythonError",
    ] = Field(alias="assetRecordsOrError", discriminator="typename__")


class AssetRecordsAssetRecordsOrErrorAssetRecordConnection(BaseModel):
    typename__: Literal["AssetRecordConnection"] = Field(alias="__typename")
    assets: list["AssetRecordsAssetRecordsOrErrorAssetRecordConnectionAssets"]
    cursor: Optional[str]


class AssetRecordsAssetRecordsOrErrorAssetRecordConnectionAssets(BaseModel):
    key: "AssetRecordsAssetRecordsOrErrorAssetRecordConnectionAssetsKey"


class AssetRecordsAssetRecordsOrErrorAssetRecordConnectionAssetsKey(BaseModel):
    path: list[str]


class AssetRecordsAssetRecordsOrErrorPythonError(BaseModel):
    typename__: Literal["PythonError"] = Field(alias="__typename")
    message: str
    stack: list[str]


AssetRecords.model_rebuild()
AssetRecordsAssetRecordsOrErrorAssetRecordConnection.model_rebuild()
AssetRecordsAssetRecordsOrErrorAssetRecordConnectionAssets.model_rebuild()
