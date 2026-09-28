from datetime import datetime
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.metrics import (
    IngestionMetrics,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.models import (
    StoredObject,
)


class RawPageLineage(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    page_index: int = Field(
        ge=0,
    )

    record_count: int = Field(
        ge=0,
    )

    payload_key: str = Field(
        min_length=1,
    )

    metadata_key: str = Field(
        min_length=1,
    )

    payload_size_bytes: int = Field(
        ge=0,
    )

    payload_sha256: str = Field(
        pattern=r"^[0-9a-f]{64}$",
    )


class QuarantineLineage(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    page_index: int = Field(
        ge=0,
    )

    record_index: int = Field(
        ge=0,
    )

    object_key: str = Field(
        min_length=1,
    )


class BipadRunManifest(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    manifest_version: int = 1

    status: Literal[
        "completed"
    ] = "completed"

    provider: DataProvider

    endpoint: BipadEndpoint

    run_id: str = Field(
        min_length=1,
    )

    captured_at: datetime

    started_at: datetime

    finished_at: datetime

    duration_ms: float = Field(
        ge=0,
    )

    metrics: IngestionMetrics

    raw_pages: tuple[
        RawPageLineage,
        ...,
    ]

    quarantine_objects: tuple[
        QuarantineLineage,
        ...,
    ]


class ManifestWriteResult(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    object: StoredObject

    manifest: BipadRunManifest