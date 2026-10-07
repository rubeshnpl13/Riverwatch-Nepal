from __future__ import annotations

import argparse
import base64
import binascii
from collections.abc import (
    Callable,
    Sequence,
)
from dataclasses import dataclass
from datetime import (
    UTC,
    datetime,
)
from typing import cast
from uuid import (
    NAMESPACE_URL,
    UUID,
    uuid5,
)

from pyspark.sql import SparkSession

from riverwatch.cloud.processing_output import (
    ProcessingCloudStore,
)
from riverwatch.cloud.processing_pipeline import (
    CloudProcessingRunResult,
    process_gcs_manifest,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.serialization import (
    decode_event,
    encode_event,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.models import (
    ProcessedDataset,
)
from riverwatch.storage.errors import (
    ObjectAlreadyExistsError,
)
from riverwatch.storage.gcs import (
    GcsObjectStore,
)

SUPPORTED_PROVIDER = "bipad"

Clock = Callable[
    [],
    datetime,
]


class ProcessingJobError(
    RuntimeError,
):
    """Managed Spark processing job failed validation."""


@dataclass(
    frozen=True,
    slots=True,
)
class ProcessingJobArguments:
    event_json_base64: str
    lake_bucket: str



def utc_now() -> datetime:
    return datetime.now(
        UTC
    )


def processing_completion_event_id(
    ingestion_event_id: UUID,
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        (
            "https://riverwatch.nepal/"
            "events/processing-completed/"
            f"{ingestion_event_id}"
        ),
    )


def parse_processing_job_args(
    argv: Sequence[str] | None = None,
) -> ProcessingJobArguments:
    parser = argparse.ArgumentParser(
        description=(
            "Run one RiverWatch Managed "
            "Spark processing job."
        )
    )

    parser.add_argument(
        "--event-json-base64",
        required=True,
    )

    parser.add_argument(
        "--lake-bucket",
        required=True,
    )

    namespace = parser.parse_args(
        argv
    )
    return ProcessingJobArguments(
        event_json_base64=(
            _required_text(
                cast(
                    str,
                    namespace.event_json_base64,
                ),
                "event_json_base64",
            )
        ),
        lake_bucket=_required_text(
            cast(
                str,
                namespace.lake_bucket,
            ),
            "lake_bucket",
        ),
    )

def decode_submitted_event(
    encoded_event: str,
) -> EventEnvelope:
    normalized = _required_text(
        encoded_event,
        "event_json_base64",
    )

    try:
        payload = base64.b64decode(
            normalized,
            validate=True,
        )

    except (
        binascii.Error,
        ValueError,
    ) as exc:
        raise ProcessingJobError(
            "event_json_base64 is not "
            "valid base64"
        ) from exc

    try:
        return decode_event(
            payload
        )

    except Exception as exc:
        raise ProcessingJobError(
            "Unable to decode submitted "
            "event"
        ) from exc


def run_processing_job(
    *,
    spark: SparkSession,
    store: ProcessingCloudStore,
    ingestion_event: EventEnvelope,
    clock: Clock = utc_now,
) -> EventEnvelope:
    request = (
        _validate_ingestion_event(
            ingestion_event
        )
    )

    result = process_gcs_manifest(
        spark=spark,
        store=store,
        manifest_key=(
            request.manifest_path
        ),
    )

    _validate_processing_result(
        result=result,
        expected_run_id=request.run_id,
        expected_endpoint=(
            request.endpoint
        ),
    )

    completion_event = (
        _load_or_create_completion_event(

            store=store,
            ingestion_event=(
                ingestion_event
            ),
            request=request,
            result=result,
            clock=clock,
        )
    )

    # Publishing happens only after:
    #
    # 1. manifest/raw validation,
    # 2. Spark transformations,
    # 3. processed output completion,
    # 4. quality report persistence,
    # 5. durable completion-event receipt.

    return completion_event


@dataclass(
    frozen=True,
    slots=True,
)
class _ValidatedIngestionRequest:
    provider: str
    endpoint: BipadEndpoint
    run_id: str
    manifest_path: str
    request_event_id: UUID


def _validate_ingestion_event(
    event: EventEnvelope,
) -> _ValidatedIngestionRequest:
    if (
        event.event_type
        is not EventType.INGESTION_COMPLETED
    ):
        raise ProcessingJobError(
            "Processing job requires an "
            "ingestion.completed event"
        )

    provider = _event_text(
        event,
        "provider",
    )

    if provider != SUPPORTED_PROVIDER:
        raise ProcessingJobError(
            "Unsupported processing "
            f"provider: {provider}"
        )

    endpoint_value = _event_text(
        event,
        "endpoint",
    )

    try:
        endpoint = BipadEndpoint(
            endpoint_value
        )

    except ValueError as exc:
        raise ProcessingJobError(
            "Unsupported processing "
            f"endpoint: {endpoint_value}"
        ) from exc

    if endpoint not in {
        BipadEndpoint.RIVER,
        BipadEndpoint.RIVER_STATIONS,
    }:
        raise ProcessingJobError(
            "Unsupported processing "
            f"endpoint: {endpoint_value}"
        )

    run_id = _event_text(
        event,
        "run_id",
    )

    manifest_path = _event_text(
        event,
        "manifest_path",
    )

    request_event_id_value = (
        _event_text(
            event,
            "request_event_id",
        )
    )

    try:
        request_event_id = UUID(
            request_event_id_value
        )

    except ValueError as exc:
        raise ProcessingJobError(
            "request_event_id must be "
            "a UUID"
        ) from exc

    return _ValidatedIngestionRequest(
        provider=provider,
        endpoint=endpoint,
        run_id=run_id,
        manifest_path=manifest_path,
        request_event_id=(
            request_event_id
        ),
    )


def _validate_processing_result(
    *,
    result: CloudProcessingRunResult,
    expected_run_id: str,
    expected_endpoint: BipadEndpoint,
) -> None:
    if result.run_id != expected_run_id:
        raise ProcessingJobError(
            "Processing result run_id "
            "does not match the "
            "ingestion event"
        )

    if (
        result.endpoint
        is not expected_endpoint
    ):
        raise ProcessingJobError(
            "Processing result endpoint "
            "does not match the "
            "ingestion event"
        )

    datasets = {
        output.dataset
        for output in result.outputs
    }

    if (
        ProcessedDataset.OBSERVATIONS
        not in datasets
    ):
        raise ProcessingJobError(
            "Processing result did not "
            "contain observations"
        )

    if (
        expected_endpoint
        is BipadEndpoint.RIVER_STATIONS
        and ProcessedDataset.STATIONS
        not in datasets
    ):
        raise ProcessingJobError(
            "Station processing result "
            "did not contain stations"
        )


def _load_or_create_completion_event(
    *,
    store: ProcessingCloudStore,
    ingestion_event: EventEnvelope,
    request: _ValidatedIngestionRequest,
    result: CloudProcessingRunResult,
    clock: Clock,
) -> EventEnvelope:
    receipt_key = (
        _completion_receipt_key(
            result
        )
    )

    if store.object_exists(
        key=receipt_key
    ):
        existing = _read_completion_event(
            store=store,
            key=receipt_key,
        )

        _validate_existing_completion_event(
            event=existing,
            ingestion_event=(
                ingestion_event
            ),
            request=request,
            result=result,
        )

        return existing

    candidate = _build_completion_event(
        ingestion_event=(
            ingestion_event
        ),
        request=request,
        result=result,
        occurred_at=clock(),
    )

    data = encode_event(
        candidate
    )

    try:
        store.create_bytes(
            key=receipt_key,
            data=data,
            content_type=(
                "application/json"
            ),
        )

        return candidate

    except ObjectAlreadyExistsError:
        # A concurrent/retried execution may
        # have created the deterministic
        # receipt after our existence check.
        existing = _read_completion_event(
            store=store,
            key=receipt_key,
        )

        _validate_existing_completion_event(
            event=existing,
            ingestion_event=(
                ingestion_event
            ),
            request=request,
            result=result,
        )

        return existing


def _build_completion_event(
    *,
    ingestion_event: EventEnvelope,
    request: _ValidatedIngestionRequest,
    result: CloudProcessingRunResult,
    occurred_at: datetime,
) -> EventEnvelope:
    station_output_path: (
        str | None
    ) = None

    observation_output_path: (
        str | None
    ) = None

    for output in result.outputs:
        if (
            output.dataset
            is ProcessedDataset.STATIONS
        ):
            station_output_path = (
                output.key
            )

        elif (
            output.dataset
            is ProcessedDataset.OBSERVATIONS
        ):
            observation_output_path = (
                output.key
            )

    if observation_output_path is None:
        raise ProcessingJobError(
            "Processing result did not "
            "contain observations"
        )

    return EventEnvelope(
        event_id=(
            processing_completion_event_id(
                ingestion_event.event_id
            )
        ),
        event_type=(
            EventType.PROCESSING_COMPLETED
        ),
        occurred_at=occurred_at,
        correlation_id=(
            ingestion_event
            .correlation_id
        ),
        data={
            "provider": request.provider,
            "endpoint": (
                request.endpoint.value
            ),
            "run_id": request.run_id,
            "manifest_path": (
                request.manifest_path
            ),
            "quality_report_path": (
                result.quality_report_key
            ),
            "station_output_path": (
                station_output_path
            ),
            "observation_output_path": (
                observation_output_path
            ),
            "ingestion_event_id": str(
                ingestion_event.event_id
            ),
            "request_event_id": str(
                request.request_event_id
            ),
        },
    )


def _completion_receipt_key(
    result: CloudProcessingRunResult,
) -> str:
    try:
        parent, _ = (
            result
            .quality_report_key
            .rsplit(
                "/",
                1,
            )
        )

    except ValueError as exc:
        raise ProcessingJobError(
            "quality_report_key must "
            "contain a parent prefix"
        ) from exc

    return (
        f"{parent}/"
        "processing-completed.json"
    )


def _read_completion_event(
    *,
    store: ProcessingCloudStore,
    key: str,
) -> EventEnvelope:
    try:
        return decode_event(
            store.read_bytes(
                key=key
            )
        )

    except Exception as exc:
        raise ProcessingJobError(
            "Unable to read durable "
            "processing completion event"
        ) from exc


def _validate_existing_completion_event(
    *,
    event: EventEnvelope,
    ingestion_event: EventEnvelope,
    request: _ValidatedIngestionRequest,
    result: CloudProcessingRunResult,
) -> None:
    expected = _build_completion_event(
        ingestion_event=(
            ingestion_event
        ),
        request=request,
        result=result,
        occurred_at=event.occurred_at,
    )

    if event != expected:
        raise ProcessingJobError(
            "Existing processing completion "
            "receipt conflicts with the "
            "current processing result"
        )


def _event_text(
    event: EventEnvelope,
    key: str,
) -> str:
    value = event.data.get(
        key
    )

    if not isinstance(
        value,
        str,
    ):
        raise ProcessingJobError(
            f"{key} must be a string"
        )

    return _required_text(
        value,
        key,
    )


def _required_text(
    value: str,
    name: str,
) -> str:
    normalized = value.strip()

    if not normalized:
        raise ProcessingJobError(
            f"{name} must not be blank"
        )

    return normalized


def main(
    argv: Sequence[str] | None = None,
) -> int:
    arguments = (
        parse_processing_job_args(
            argv
        )
    )

    ingestion_event = (
        decode_submitted_event(
            arguments.event_json_base64
        )
    )

    spark = (
        SparkSession
        .builder
        .appName(
            "RiverWatch Cloud Processing"
        )
        .getOrCreate()
    )

    try:
        store = GcsObjectStore(
            bucket_name=(
                arguments.lake_bucket
            ),
        )

        run_processing_job(
            spark=spark,
            store=store,
            ingestion_event=(
                ingestion_event
            ),
        )

    finally:
        spark.stop()

    return 0