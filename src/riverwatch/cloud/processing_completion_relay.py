from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from riverwatch.events.bus import (
    EventPublisher,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.serialization import (
    decode_event,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)

GCS_FINALIZE_EVENT_TYPE = (
    "OBJECT_FINALIZE"
)

PROCESSING_COMPLETION_RECEIPT_SUFFIX = (
    "/processing-completed.json"
)

SUPPORTED_PROVIDER = "bipad"


class ProcessingCompletionRelayError(
    RuntimeError
):
    """Processing completion relay state is invalid."""


class CompletionReceiptStore(
    Protocol
):
    @property
    def bucket_name(
        self,
    ) -> str:
        ...

    def read_bytes(
        self,
        *,
        key: str,
    ) -> bytes:
        ...


@dataclass(
    frozen=True,
    slots=True,
)
class GcsObjectFinalizeNotification:
    bucket_name: str
    object_name: str
    object_generation: str


def decode_gcs_finalize_push(
    payload: Mapping[str, object],
) -> GcsObjectFinalizeNotification:
    message = payload.get(
        "message"
    )

    if not isinstance(
        message,
        Mapping,
    ):
        raise ProcessingCompletionRelayError(
            "Pub/Sub push payload must "
            "contain a message object"
        )

    attributes = message.get(
        "attributes"
    )

    if not isinstance(
        attributes,
        Mapping,
    ):
        raise ProcessingCompletionRelayError(
            "Pub/Sub message must contain "
            "attributes"
        )

    event_type = _required_attribute(
        attributes,
        "eventType",
    )

    if (
        event_type
        != GCS_FINALIZE_EVENT_TYPE
    ):
        raise ProcessingCompletionRelayError(
            "Cloud Storage notification "
            "must be OBJECT_FINALIZE"
        )

    bucket_name = _required_attribute(
        attributes,
        "bucketId",
    )

    object_name = _required_attribute(
        attributes,
        "objectId",
    )

    object_generation = (
        _required_attribute(
            attributes,
            "objectGeneration",
        )
    )

    if not object_generation.isdigit():
        raise ProcessingCompletionRelayError(
            "objectGeneration must be "
            "numeric"
        )

    return GcsObjectFinalizeNotification(
        bucket_name=bucket_name,
        object_name=object_name,
        object_generation=(
            object_generation
        ),
    )


def relay_processing_completion(
    *,
    notification: GcsObjectFinalizeNotification,
    store: CompletionReceiptStore,
    publisher: EventPublisher,
) -> EventEnvelope | None:
    if (
        notification.bucket_name
        != store.bucket_name
    ):
        raise ProcessingCompletionRelayError(
            "Cloud Storage notification "
            "bucket does not match the "
            "configured lake bucket"
        )

    if not notification.object_name.endswith(
        PROCESSING_COMPLETION_RECEIPT_SUFFIX
    ):
        return None

    try:
        receipt_bytes = store.read_bytes(
            key=notification.object_name
        )

    except Exception as exc:
        raise ProcessingCompletionRelayError(
            "Unable to read processing "
            "completion receipt"
        ) from exc

    try:
        event = decode_event(
            receipt_bytes
        )

    except Exception as exc:
        raise ProcessingCompletionRelayError(
            "Unable to decode processing "
            "completion receipt"
        ) from exc

    _validate_completion_event(
        event=event,
        receipt_key=(
            notification.object_name
        ),
    )
    try:
        publisher.publish(
            event
        )

    except Exception as exc:
        raise ProcessingCompletionRelayError(
            "Unable to publish processing "
            "completion event"
        ) from exc

    return event

def _validate_completion_event(
    *,
    event: EventEnvelope,
    receipt_key: str,
) -> None:
    if (
        event.event_type
        is not EventType.PROCESSING_COMPLETED
    ):
        raise ProcessingCompletionRelayError(
            "Completion receipt must contain "
            "a processing.completed event"
        )

    provider = _event_text(
        event,
        "provider",
    )

    if provider != SUPPORTED_PROVIDER:
        raise ProcessingCompletionRelayError(
            "Completion receipt has an "
            "unsupported provider"
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
        raise ProcessingCompletionRelayError(
            "Completion receipt has an "
            "unsupported endpoint"
        ) from exc

    if endpoint not in {
        BipadEndpoint.RIVER,
        BipadEndpoint.RIVER_STATIONS,
    }:
        raise ProcessingCompletionRelayError(
            "Completion receipt has an "
            "unsupported endpoint"
        )

    _event_text(
        event,
        "run_id",
    )

    _event_text(
        event,
        "manifest_path",
    )

    quality_report_path = (
        _event_text(
            event,
            "quality_report_path",
        )
    )

    observation_output_path = (
        _event_text(
            event,
            "observation_output_path",
        )
    )

    if not observation_output_path:
        raise ProcessingCompletionRelayError(
            "Completion receipt must contain "
            "an observation output"
        )

    if (
        endpoint
        is BipadEndpoint.RIVER_STATIONS
    ):
        _event_text(
            event,
            "station_output_path",
        )

    _event_uuid(
        event,
        "ingestion_event_id",
    )

    _event_uuid(
        event,
        "request_event_id",
    )

    try:
        quality_parent, _ = (
            quality_report_path.rsplit(
                "/",
                1,
            )
        )

    except ValueError as exc:
        raise ProcessingCompletionRelayError(
            "quality_report_path must "
            "contain a parent prefix"
        ) from exc

    expected_receipt_key = (
        f"{quality_parent}"
        f"{PROCESSING_COMPLETION_RECEIPT_SUFFIX}"
    )

    if receipt_key != expected_receipt_key:
        raise ProcessingCompletionRelayError(
            "Completion receipt object path "
            "does not match its quality "
            "report path"
        )


def _required_attribute(
    attributes: Mapping[
        object,
        object,
    ],
    name: str,
) -> str:
    value = attributes.get(
        name
    )

    if not isinstance(
        value,
        str,
    ):
        raise ProcessingCompletionRelayError(
            f"{name} must be a string"
        )

    normalized = value.strip()

    if not normalized:
        raise ProcessingCompletionRelayError(
            f"{name} must not be blank"
        )

    return normalized


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
        raise ProcessingCompletionRelayError(
            f"{key} must be a string"
        )

    normalized = value.strip()

    if not normalized:
        raise ProcessingCompletionRelayError(
            f"{key} must not be blank"
        )

    return normalized


def _event_uuid(
    event: EventEnvelope,
    key: str,
) -> UUID:
    value = _event_text(
        event,
        key,
    )

    try:
        return UUID(
            value
        )

    except ValueError as exc:
        raise ProcessingCompletionRelayError(
            f"{key} must be a UUID"
        ) from exc