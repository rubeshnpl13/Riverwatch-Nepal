from datetime import datetime
from enum import StrEnum
from pathlib import Path

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)


class ProcessedDataset(StrEnum):
    STATIONS = "stations"
    OBSERVATIONS = "observations"


class ProcessingInputPage(BaseModel):
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

    payload_path: str | Path

    payload_sha256: str = Field(
        pattern=r"^[0-9a-f]{64}$",
    )


class ProcessingInputRun(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    run_id: str = Field(
        min_length=1,
    )

    endpoint: BipadEndpoint

    captured_at: datetime

    manifest_path: str | Path

    pages: tuple[
        ProcessingInputPage,
        ...,
    ]

class ProcessedOutput(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    dataset: ProcessedDataset
    path: Path
    row_count: int = Field(
        ge=0,
    )


class ProcessingRunResult(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    run_id: str = Field(
        min_length=1,
    )

    endpoint: BipadEndpoint

    outputs: tuple[
        ProcessedOutput,
        ...,
    ]
    quality_report_path: Path