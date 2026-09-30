from datetime import UTC, datetime

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.processing.models import (
    ProcessedDataset,
)
from riverwatch.storage.errors import (
    InvalidObjectKeyError,
)


def _validate_segment(
    name: str,
    value: str,
) -> str:
    cleaned = value.strip()

    if not cleaned:
        raise InvalidObjectKeyError(
            f"{name} cannot be empty"
        )

    if "/" in cleaned:
        raise InvalidObjectKeyError(
            f"{name} cannot contain '/'"
        )

    if cleaned in {
        ".",
        "..",
    }:
        raise InvalidObjectKeyError(
            f"Invalid {name}: {cleaned}"
        )

    return cleaned


def build_raw_page_prefix(
    *,
    provider: DataProvider,
    endpoint: BipadEndpoint,
    captured_at: datetime,
    run_id: str,
    page_index: int,
) -> str:
    if page_index < 0:
        raise ValueError(
            "page_index must be greater than "
            "or equal to zero"
        )

    safe_run_id = _validate_segment(
        "run_id",
        run_id,
    )

    timestamp = captured_at.astimezone(
        UTC
    )

    return (
        f"raw/"
        f"provider={provider.value}/"
        f"endpoint={endpoint.value}/"
        f"year={timestamp:%Y}/"
        f"month={timestamp:%m}/"
        f"day={timestamp:%d}/"
        f"hour={timestamp:%H}/"
        f"run_id={safe_run_id}/"
        f"page={page_index:06d}"
    )


def build_raw_payload_key(
    *,
    provider: DataProvider,
    endpoint: BipadEndpoint,
    captured_at: datetime,
    run_id: str,
    page_index: int,
) -> str:
    prefix = build_raw_page_prefix(
        provider=provider,
        endpoint=endpoint,
        captured_at=captured_at,
        run_id=run_id,
        page_index=page_index,
    )

    return f"{prefix}/payload.json"


def build_raw_metadata_key(
    *,
    provider: DataProvider,
    endpoint: BipadEndpoint,
    captured_at: datetime,
    run_id: str,
    page_index: int,
) -> str:
    prefix = build_raw_page_prefix(
        provider=provider,
        endpoint=endpoint,
        captured_at=captured_at,
        run_id=run_id,
        page_index=page_index,
    )

    return f"{prefix}/metadata.json"

def build_quarantine_record_key(
    *,
    provider: DataProvider,
    endpoint: BipadEndpoint,
    captured_at: datetime,
    run_id: str,
    page_index: int,
    record_index: int,
) -> str:
    if page_index < 0:
        raise ValueError(
            "page_index must be greater than "
            "or equal to zero"
        )

    if record_index < 0:
        raise ValueError(
            "record_index must be greater than "
            "or equal to zero"
        )

    safe_run_id = _validate_segment(
        "run_id",
        run_id,
    )

    timestamp = captured_at.astimezone(
        UTC
    )

    return (
        f"quarantine/"
        f"provider={provider.value}/"
        f"endpoint={endpoint.value}/"
        f"year={timestamp:%Y}/"
        f"month={timestamp:%m}/"
        f"day={timestamp:%d}/"
        f"hour={timestamp:%H}/"
        f"run_id={safe_run_id}/"
        f"page={page_index:06d}/"
        f"record={record_index:09d}.json"
    )

def build_run_manifest_key(
    *,
    provider: DataProvider,
    endpoint: BipadEndpoint,
    captured_at: datetime,
    run_id: str,
) -> str:
    safe_run_id = _validate_segment(
        "run_id",
        run_id,
    )

    timestamp = captured_at.astimezone(
        UTC
    )

    return (
        f"manifests/"
        f"provider={provider.value}/"
        f"endpoint={endpoint.value}/"
        f"year={timestamp:%Y}/"
        f"month={timestamp:%m}/"
        f"day={timestamp:%d}/"
        f"hour={timestamp:%H}/"
        f"run_id={safe_run_id}/"
        f"manifest.json"
    )

def build_processed_run_prefix(
    *,
    dataset: ProcessedDataset,
    endpoint: BipadEndpoint,
    captured_at: datetime,
    run_id: str,
) -> str:
    safe_run_id = _validate_segment(
        "run_id",
        run_id,
    )

    timestamp = captured_at.astimezone(
        UTC
    )

    return (
        f"processed/"
        f"dataset={dataset.value}/"
        f"endpoint={endpoint.value}/"
        f"year={timestamp:%Y}/"
        f"month={timestamp:%m}/"
        f"day={timestamp:%d}/"
        f"hour={timestamp:%H}/"
        f"run-{safe_run_id}"
    )

def build_quality_report_key(
    *,
    endpoint: BipadEndpoint,
    captured_at: datetime,
    run_id: str,
) -> str:
    safe_run_id = _validate_segment(
        "run_id",
        run_id,
    )

    timestamp = captured_at.astimezone(
        UTC
    )

    return (
        f"quality/"
        f"endpoint={endpoint.value}/"
        f"year={timestamp:%Y}/"
        f"month={timestamp:%m}/"
        f"day={timestamp:%d}/"
        f"hour={timestamp:%H}/"
        f"run-{safe_run_id}/"
        f"report.json"
    )