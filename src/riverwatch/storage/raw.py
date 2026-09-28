from datetime import datetime

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.models import (
    StoredObject,
)


class RawPageMetadata(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    provider: DataProvider

    endpoint: BipadEndpoint

    run_id: str = Field(
        min_length=1,
    )

    page_index: int = Field(
        ge=0,
    )

    captured_at: datetime

    record_count: int = Field(
        ge=0,
    )

    payload_key: str = Field(
        min_length=1,
    )

    payload_size_bytes: int = Field(
        ge=0,
    )

    payload_sha256: str = Field(
        pattern=r"^[0-9a-f]{64}$",
    )


class RawPageWriteResult(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    payload: StoredObject
    metadata: StoredObject

    page_metadata: RawPageMetadata