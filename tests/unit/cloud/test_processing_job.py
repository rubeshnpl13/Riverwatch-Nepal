from __future__ import annotations

import base64
from datetime import (
    UTC,
    datetime,
    timedelta,
)
from typing import cast
from uuid import UUID

import pytest
from pyspark.sql import SparkSession

import riverwatch.cloud.processing_job as job
from riverwatch.cloud.processing_pipeline import (
    CloudProcessingDatasetResult,
    CloudProcessingRunResult,
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
    StorageError,
)
from riverwatch.storage.models import (
    StoredObject,
)
from riverwatch.storage.serialization import (
    sha256_hex,
)

INGESTION_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

REQUEST_EVENT_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

CORRELATION_ID = UUID(
    "33333333-3333-3333-3333-333333333333"
)

INGESTION_TIME = datetime(
    2026,
    10,
    6,
    12,
    0,
    tzinfo=UTC,
)

PROCESSING_TIME = datetime(
    2026,
    10,
    6,
    12,
    5,
    tzinfo=UTC,
)
COMPLETION_RECEIPT_KEY = (
    "quality/provider=bipad/"
    "endpoint=river-stations/"
    "run-run-123/"
    "processing-completed.json"
)

class FakeStore:
    def __init__(
        self,
    ) -> None:
        self.bucket_name = (
            "riverwatch-test-lake"
        )

        self.objects: dict[
            str,
            bytes,
        ] = {}

    def object_exists(
        self,
        *,
        key: str,
    ) -> bool:
        return key in self.objects

    def prefix_exists(
        self,
        *,
        prefix: str,
    ) -> bool:
        normalized = (
            f"{prefix.rstrip('/')}/"
        )

        return any(
            key.startswith(
                normalized
            )
            for key in self.objects
        )

    def read_bytes(
        self,
        *,
        key: str,
    ) -> bytes:
        try:
            return self.objects[
                key
            ]

        except KeyError as exc:
            raise StorageError(
                f"missing: {key}"
            ) from exc

    def create_bytes(
        self,
        *,
        key: str,
        data: bytes,
        content_type: str,
    ) -> StoredObject:
        if key in self.objects:
            raise ObjectAlreadyExistsError(
                f"Object already exists: {key}"
            )

        self.objects[
            key
        ] = data

        return StoredObject(
            key=key,
            size_bytes=len(data),
            content_type=content_type,
            sha256=sha256_hex(
                data
            ),
        )


def ingestion_event(
) -> EventEnvelope:
    return EventEnvelope(
        event_id=INGESTION_EVENT_ID,
        event_type=(
            EventType.INGESTION_COMPLETED
        ),
        occurred_at=INGESTION_TIME,
        correlation_id=CORRELATION_ID,
        data={
            "provider": "bipad",
            "endpoint": "river-stations",
            "run_id": "run-123",
            "manifest_path": (
                "manifests/provider=bipad/"
                "endpoint=river-stations/"
                "run_id=run-123/"
                "manifest.json"
            ),
            "request_event_id": str(
                REQUEST_EVENT_ID
            ),
        },
    )


def processing_result(
) -> CloudProcessingRunResult:
    return CloudProcessingRunResult(
        run_id="run-123",
        endpoint=(
            BipadEndpoint
            .RIVER_STATIONS
        ),
        outputs=(
            CloudProcessingDatasetResult(
                dataset=(
                    ProcessedDataset
                    .STATIONS
                ),
                key=(
                    "processed/"
                    "dataset=stations/"
                    "run-run-123"
                ),
                uri=(
                    "gs://riverwatch-test-lake/"
                    "processed/"
                    "dataset=stations/"
                    "run-run-123"
                ),
                row_count=1,
            ),
            CloudProcessingDatasetResult(
                dataset=(
                    ProcessedDataset
                    .OBSERVATIONS
                ),
                key=(
                    "processed/"
                    "dataset=observations/"
                    "run-run-123"
                ),
                uri=(
                    "gs://riverwatch-test-lake/"
                    "processed/"
                    "dataset=observations/"
                    "run-run-123"
                ),
                row_count=1,
            ),
        ),
        quality_report_key=(
            "quality/provider=bipad/"
            "endpoint=river-stations/"
            "run-run-123/"
            "report.json"
        ),
        quality_report_uri=(
            "gs://riverwatch-test-lake/"
            "quality/provider=bipad/"
            "endpoint=river-stations/"
            "run-run-123/"
            "report.json"
        ),
    )


def install_fake_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_process(
        *,
        spark: SparkSession,
        store: object,
        manifest_key: str,
    ) -> CloudProcessingRunResult:
        _ = (
            spark,
            store,
        )

        assert manifest_key.endswith(
            "manifest.json"
        )

        return processing_result()

    monkeypatch.setattr(
        job,
        "process_gcs_manifest",
        fake_process,
    )


def test_decodes_submitted_event(
) -> None:
    event = ingestion_event()

    encoded = (
        base64.b64encode(
            encode_event(
                event
            )
        )
        .decode(
            "ascii"
        )
    )

    assert (
        job.decode_submitted_event(
            encoded
        )
        == event
    )


def test_completion_event_id_is_deterministic(
) -> None:
    first = (
        job.processing_completion_event_id(
            INGESTION_EVENT_ID
        )
    )

    second = (
        job.processing_completion_event_id(
            INGESTION_EVENT_ID
        )
    )

    assert first == second

    assert first != (
        job.processing_completion_event_id(
            REQUEST_EVENT_ID
        )
    )

def test_processes_then_persists_completion_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_fake_pipeline(
        monkeypatch
    )

    store = FakeStore()

    event = job.run_processing_job(
        spark=cast(
            SparkSession,
            object(),
        ),
        store=store,
        ingestion_event=(
            ingestion_event()
        ),
        clock=lambda: PROCESSING_TIME,
    )

    assert (
        event.event_type
        is EventType.PROCESSING_COMPLETED
    )

    assert (
        event.correlation_id
        == CORRELATION_ID
    )

    assert (
        event.occurred_at
        == PROCESSING_TIME
    )

    assert dict(
        event.data
    ) == {
        "provider": "bipad",
        "endpoint": "river-stations",
        "run_id": "run-123",
        "manifest_path": (
            "manifests/provider=bipad/"
            "endpoint=river-stations/"
            "run_id=run-123/"
            "manifest.json"
        ),
        "quality_report_path": (
            "quality/provider=bipad/"
            "endpoint=river-stations/"
            "run-run-123/"
            "report.json"
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
    }

    assert (
        COMPLETION_RECEIPT_KEY
        in store.objects
    )

    receipt_event = decode_event(
        store.objects[
            COMPLETION_RECEIPT_KEY
        ]
    )

    assert receipt_event == event


def test_retry_reuses_durable_completion_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_fake_pipeline(
        monkeypatch
    )

    store = FakeStore()

    first = job.run_processing_job(
        spark=cast(
            SparkSession,
            object(),
        ),
        store=store,
        ingestion_event=(
            ingestion_event()
        ),
        clock=lambda: PROCESSING_TIME,
    )

    first_receipt_bytes = (
        store.objects[
            COMPLETION_RECEIPT_KEY
        ]
    )

    later = (
        PROCESSING_TIME
        + timedelta(
            hours=1
        )
    )

    second = job.run_processing_job(
        spark=cast(
            SparkSession,
            object(),
        ),
        store=store,
        ingestion_event=(
            ingestion_event()
        ),
        clock=lambda: later,
    )

    assert second == first

    assert (
        second.occurred_at
        == PROCESSING_TIME
    )

    assert (
        store.objects[
            COMPLETION_RECEIPT_KEY
        ]
        == first_receipt_bytes
    )

    assert (
        decode_event(
            first_receipt_bytes
        )
        == first
    )


def test_rejects_wrong_event_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def fake_process(
        **kwargs: object,
    ) -> CloudProcessingRunResult:
        nonlocal called

        _ = kwargs
        called = True

        return processing_result()

    monkeypatch.setattr(
        job,
        "process_gcs_manifest",
        fake_process,
    )

    event = EventEnvelope(
        event_id=INGESTION_EVENT_ID,
        event_type=(
            EventType.PROCESSING_COMPLETED
        ),
        occurred_at=INGESTION_TIME,
        correlation_id=CORRELATION_ID,
        data={},
    )

    with pytest.raises(
        job.ProcessingJobError,
        match=(
            "requires an "
            "ingestion.completed"
        ),
    ):
        job.run_processing_job(
            spark=cast(
                SparkSession,
                object(),
            ),
            store=FakeStore(),
            ingestion_event=event,
        )

    assert not called


def test_rejects_result_run_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wrong_result = (
        CloudProcessingRunResult(
            run_id="different-run",
            endpoint=(
                BipadEndpoint
                .RIVER_STATIONS
            ),
            outputs=(
                processing_result()
                .outputs
            ),
            quality_report_key=(
                processing_result()
                .quality_report_key
            ),
            quality_report_uri=(
                processing_result()
                .quality_report_uri
            ),
        )
    )

    monkeypatch.setattr(
        job,
        "process_gcs_manifest",
        lambda **kwargs: (
            wrong_result
        ),
    )

    with pytest.raises(
        job.ProcessingJobError,
        match="run_id",
    ):
        job.run_processing_job(
            spark=cast(
                SparkSession,
                object(),
            ),
            store=FakeStore(),
            ingestion_event=(
                ingestion_event()
            ),
        )