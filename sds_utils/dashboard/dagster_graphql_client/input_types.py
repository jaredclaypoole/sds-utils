from typing import Optional

from pydantic import Field

from .base_model import BaseModel
from .enums import BulkActionStatus


class AssetKeyInput(BaseModel):
    path: list[str]


class BulkActionsFilter(BaseModel):
    statuses: Optional[list[BulkActionStatus]] = None
    created_before: Optional[float] = Field(alias="createdBefore", default=None)
    created_after: Optional[float] = Field(alias="createdAfter", default=None)
