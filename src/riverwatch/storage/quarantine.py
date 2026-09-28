from datetime import datetime
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.validation import (
    ValidationIssue,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.models import (
    StoredObject,
)


class QuarantineRecordEnvelope(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    provider: DataProvider

    endpoint: BipadEndpoint

    run_id: str = Field(
        min_length=1,
    )

    captured_at: datetime

    page_index: int = Field(
        ge=0,
    )

    record_index: int = Field(
        ge=0,
    )

    raw_record: Any

    issues: tuple[
        ValidationIssue,
        ...,
    ]


class QuarantineWriteResult(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    object: StoredObject

    envelope: QuarantineRecordEnvelope