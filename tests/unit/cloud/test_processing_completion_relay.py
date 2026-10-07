from __future__ import annotations

from datetime import (
    UTC,
    datetime,
)
from uuid import UUID

import pytest

from riverwatch.cloud.processing_completion_relay import (
    GcsObjectFinalizeNotification,
    ProcessingCompletionRelayError,
    decode_gcs_finalize_push,
    relay_processing_completion,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.serialization import (
    encode_event,
)

BUCKET_NAME = "riverwatch-test-lake"

QUALITY_REPORT_KEY = (
    "quality/"
    "provider=bipad/"
    "endpoint=river-stations/"
    "date=2026-10-07/"
    "run=run-123/"
    "report.json"
)

RECEIPT_KEY = (
    "quality/"
    "provider=bipad/"
    "endpoint=river-stations/"
    "date=2026-10-07/"
    "run=run-123/"
    "processing-completed.json"
)

INGESTION_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

REQUEST_EVENT_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

COMPLETION_EVENT_ID = UUID(
    "33333333-3333-3333-3333-333333333333"
)

CORRELATION_ID = UUID(
    "44444444-4444-4444-4444-444444444444"
)


class FakeReceiptStore:
    def __init__(
        self,
        *,
        bucket_name: str,
        objects: dict[str, bytes] | None = None,
    ) -> None:
        self._bucket_name = (
            bucket_name
        )

        self.objects = (
            {}
            if objects is None
            else dict(objects)
        )

        self.read_keys: list[str] = []

    @property
    def bucket_name(
        self,
    ) -> str:
        return self._bucket_name

    def read_bytes(
        self,
        *,
        key: str,
    ) -> bytes:
        self.read_keys.append(
            key
        )

        return self.objects[
            key
        ]


class RecordingPublisher:
    def __init__(
        self,
    ) -> None:
        self.events: list[
            EventEnvelope
        ] = []

    def publish(
        self,
        event: EventEnvelope,
    ) -> None:
        self.events.append(
            event
        )


class FailingPublisher:
    def publish(
        self,
        event: EventEnvelope,
    ) -> None:
        del event

        raise RuntimeError(
            "publish failed"
        )


def _completion_event(
    *,
    quality_report_path: str = (
        QUALITY_REPORT_KEY
    ),
    event_type: EventType = (
        EventType.PROCESSING_COMPLETED
    ),
) -> EventEnvelope:
    return EventEnvelope(
        event_id=COMPLETION_EVENT_ID,
        event_type=event_type,
        occurred_at=datetime(
            2026,
            10,
            7,
            1,
            0,
            tzinfo=UTC,
        ),
        correlation_id=(
            CORRELATION_ID
        ),
        data={
            "provider": "bipad",
            "endpoint": (
                "river-stations"
            ),
            "run_id": "run-123",
            "manifest_path": (
                "manifests/"
                "provider=bipad/"
                "endpoint=river-stations/"
                "run_id=run-123/"
                "manifest.json"
            ),
            "quality_report_path": (
                quality_report_path
            ),
            "station_output_path": (
                "processed/"
                "dataset=stations/"
                "run-run-123"
            ),
            "observation_output_path": (
                "processed/"
                "dataset=observations/"
                "run-run-123"
            ),
            "ingestion_event_id": str(
                INGESTION_EVENT_ID
            ),
            "request_event_id": str(
                REQUEST_EVENT_ID
            ),
        },
    )


def _notification(
    *,
    object_name: str = RECEIPT_KEY,
    bucket_name: str = BUCKET_NAME,
) -> GcsObjectFinalizeNotification:
    return GcsObjectFinalizeNotification(
        bucket_name=bucket_name,
        object_name=object_name,
        object_generation="123456789",
    )


def test_decodes_gcs_finalize_push() -> None:
    notification = (
        decode_gcs_finalize_push(
            {
                "message": {
                    "attributes": {
                        "eventType": (
                            "OBJECT_FINALIZE"
                        ),
                        "bucketId": (
                            BUCKET_NAME
                        ),
                        "objectId": (
                            RECEIPT_KEY
                        ),
                        "objectGeneration": (
                            "123456789"
                        ),
                    },
                },
                "subscription": (
                    "projects/test/"
                    "subscriptions/test"
                ),
            }
        )
    )

    assert (
        notification.bucket_name
        == BUCKET_NAME
    )

    assert (
        notification.object_name
        == RECEIPT_KEY
    )

    assert (
        notification.object_generation
        == "123456789"
    )


def test_rejects_non_finalize_event() -> None:
    with pytest.raises(
        ProcessingCompletionRelayError,
        match="OBJECT_FINALIZE",
    ):
        decode_gcs_finalize_push(
            {
                "message": {
                    "attributes": {
                        "eventType": (
                            "OBJECT_DELETE"
                        ),
                        "bucketId": (
                            BUCKET_NAME
                        ),
                        "objectId": (
                            RECEIPT_KEY
                        ),
                        "objectGeneration": (
                            "123456789"
                        ),
                    },
                },
            }
        )


def test_ignores_non_receipt_object() -> None:
    store = FakeReceiptStore(
        bucket_name=BUCKET_NAME,
    )

    publisher = RecordingPublisher()

    result = relay_processing_completion(
        notification=_notification(
            object_name=(
                QUALITY_REPORT_KEY
            ),
        ),
        store=store,
        publisher=publisher,
    )

    assert result is None
    assert store.read_keys == []
    assert publisher.events == []


def test_reads_and_publishes_receipt() -> None:
    completion_event = (
        _completion_event()
    )

    store = FakeReceiptStore(
        bucket_name=BUCKET_NAME,
        objects={
            RECEIPT_KEY: encode_event(
                completion_event
            ),
        },
    )

    publisher = RecordingPublisher()

    result = relay_processing_completion(
        notification=_notification(),
        store=store,
        publisher=publisher,
    )

    assert result == completion_event

    assert store.read_keys == [
        RECEIPT_KEY,
    ]

    assert publisher.events == [
        completion_event,
    ]


def test_rejects_wrong_bucket() -> None:
    store = FakeReceiptStore(
        bucket_name=BUCKET_NAME,
    )

    publisher = RecordingPublisher()

    with pytest.raises(
        ProcessingCompletionRelayError,
        match="bucket",
    ):
        relay_processing_completion(
            notification=_notification(
                bucket_name=(
                    "other-bucket"
                ),
            ),
            store=store,
            publisher=publisher,
        )

    assert store.read_keys == []
    assert publisher.events == []


def test_rejects_wrong_event_type() -> None:
    store = FakeReceiptStore(
        bucket_name=BUCKET_NAME,
        objects={
            RECEIPT_KEY: encode_event(
                _completion_event(
                    event_type=(
                        EventType
                        .INGESTION_COMPLETED
                    )
                )
            ),
        },
    )

    publisher = RecordingPublisher()

    with pytest.raises(
        ProcessingCompletionRelayError,
        match="processing.completed",
    ):
        relay_processing_completion(
            notification=_notification(),
            store=store,
            publisher=publisher,
        )

    assert publisher.events == []


def test_rejects_receipt_path_mismatch() -> None:
    store = FakeReceiptStore(
        bucket_name=BUCKET_NAME,
        objects={
            RECEIPT_KEY: encode_event(
                _completion_event(
                    quality_report_path=(
                        "quality/"
                        "other-run/"
                        "report.json"
                    )
                )
            ),
        },
    )

    publisher = RecordingPublisher()

    with pytest.raises(
        ProcessingCompletionRelayError,
        match="object path",
    ):
        relay_processing_completion(
            notification=_notification(),
            store=store,
            publisher=publisher,
        )

    assert publisher.events == []


def test_publish_failure_is_wrapped_for_retry(
) -> None:
    completion_event = (
        _completion_event()
    )

    store = FakeReceiptStore(
        bucket_name=BUCKET_NAME,
        objects={
            RECEIPT_KEY: encode_event(
                completion_event
            ),
        },
    )

    with pytest.raises(
        ProcessingCompletionRelayError,
        match=(
            "Unable to publish processing "
            "completion event"
        ),
    ) as exc_info:
        relay_processing_completion(
            notification=_notification(),
            store=store,
            publisher=FailingPublisher(),
        )

    assert isinstance(
        exc_info.value.__cause__,
        RuntimeError,
    )

    assert str(
        exc_info.value.__cause__
    ) == "publish failed"