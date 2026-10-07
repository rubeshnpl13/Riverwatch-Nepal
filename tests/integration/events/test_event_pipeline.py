from __future__ import annotations

import json
from datetime import (
    UTC,
    datetime,
)
from pathlib import Path
from uuid import UUID

import httpx
import pytest
import respx
from pyspark.sql import SparkSession

import riverwatch.cloud.processing_job as processing_job
from riverwatch.cloud.processing_completion_relay import (
    decode_gcs_finalize_push,
    relay_processing_completion,
)
from riverwatch.cloud.processing_pipeline import (
    CloudProcessingDatasetResult,
    CloudProcessingRunResult,
)
from riverwatch.config import (
    Settings,
)
from riverwatch.events.idempotency import (
    LocalIngestionExecutionStore,
)
from riverwatch.events.ingestion import (
    IngestionRequestPublisher,
)
from riverwatch.events.local import (
    LocalEventBus,
)
from riverwatch.events.model import (
    EventType,
)
from riverwatch.events.retry import (
    LocalIngestionRetryStore,
)
from riverwatch.events.serialization import (
    decode_event,
)
from riverwatch.events.worker import (
    IngestionWorker,
)
from riverwatch.ingestion.bipad.event_runner import (
    BipadEventIngestionRunner,
)
from riverwatch.processing.models import (
    ProcessedDataset,
)
from riverwatch.processing.spark.event_runner import (
    SparkProcessingEventRunner,
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

BASE_URL = (
    "https://bipadportal.gov.np"
)

STATION_URL = (
    "https://bipadportal.gov.np/"
    "api/v1/river-stations/"
)

STATION_FIXTURE = Path(
    "tests/fixtures/bipad/"
    "river_station.json"
)


REQUEST_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

INGESTION_COMPLETED_EVENT_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

CORRELATION_ID = UUID(
    "44444444-4444-4444-4444-444444444444"
)


REQUEST_TIME = datetime(
    2026,
    10,
    3,
    0,
    0,
    tzinfo=UTC,
)

INGESTION_COMPLETED_TIME = datetime(
    2026,
    10,
    3,
    0,
    1,
    tzinfo=UTC,
)

PROCESSING_COMPLETED_TIME = datetime(
    2026,
    10,
    3,
    0,
    2,
    tzinfo=UTC,
)


class InMemoryProcessingCloudStore:
    def __init__(
        self,
    ) -> None:
        self.bucket_name = (
            "riverwatch-integration-lake"
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
                f"missing object: {key}"
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
                "Object already exists: "
                f"{key}"
            )

        self.objects[
            key
        ] = data

        return StoredObject(
            key=key,
            size_bytes=len(
                data
            ),
            content_type=(
                content_type
            ),
            sha256=sha256_hex(
                data
            ),
        )


def load_station_fixture(
) -> dict[str, object]:
    return json.loads(
        STATION_FIXTURE.read_text(
            encoding="utf-8"
        )
    )


def make_settings() -> Settings:
    return Settings(
        bipad_base_url=BASE_URL,
        bipad_api_version="v1",
        http_timeout_seconds=10,
        http_max_retries=0,
    )


@respx.mock
def test_event_pipeline_persists_then_relays_processing_completion(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events_root = (
        tmp_path
        / "events"
    )

    lake_root = (
        tmp_path
        / "lake"
    )

    route = respx.get(
        STATION_URL
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 1,
                "next": None,
                "previous": None,
                "results": [
                    load_station_fixture()
                ],
            },
        )
    )

    queue = LocalEventBus(
        events_root=events_root,
    )

    request_publisher = (
        IngestionRequestPublisher(
            publisher=queue,
            clock=lambda: (
                REQUEST_TIME
            ),
            uuid_factory=lambda: (
                REQUEST_EVENT_ID
            ),
        )
    )

    request = (
        request_publisher.request(
            provider="bipad",
            endpoint="river-stations",
            correlation_id=(
                CORRELATION_ID
            ),
        )
    )

    assert (
        request.event_id
        == REQUEST_EVENT_ID
    )

    assert (
        request.event_type
        is EventType.INGESTION_REQUESTED
    )

    assert (
        request.correlation_id
        == CORRELATION_ID
    )

    assert (
        queue.list_pending()
        == [request]
    )

    ingestion_execution_store = (
        LocalIngestionExecutionStore(
            events_root=events_root,
        )
    )

    ingestion_retry_store = (
        LocalIngestionRetryStore(
            events_root=events_root,
        )
    )

    ingestion_runner = (
        BipadEventIngestionRunner(
            lake_root=lake_root,
            settings=make_settings(),
        )
    )

    ingestion_worker = (
        IngestionWorker(
            queue=queue,
            runner=ingestion_runner,
            execution_store=(
                ingestion_execution_store
            ),
            retry_store=(
                ingestion_retry_store
            ),
            clock=lambda: (
                INGESTION_COMPLETED_TIME
            ),
            uuid_factory=lambda: (
                INGESTION_COMPLETED_EVENT_ID
            ),
        )
    )

    assert (
        ingestion_worker.run_once()
        == 1
    )

    assert len(
        route.calls
    ) == 1

    assert (
        ingestion_retry_store.get(
            REQUEST_EVENT_ID
        )
        is None
    )

    ingestion_execution = (
        ingestion_execution_store.get(
            REQUEST_EVENT_ID
        )
    )

    assert (
        ingestion_execution
        is not None
    )

    expected_run_id = (
        "event-"
        f"{REQUEST_EVENT_ID}"
        "-attempt-0001"
    )

    assert (
        ingestion_execution.run_id
        == expected_run_id
    )

    assert (
        ingestion_execution
        .completion_event_id
        == INGESTION_COMPLETED_EVENT_ID
    )

    pending = (
        queue.list_pending()
    )

    assert len(
        pending
    ) == 1

    ingestion_completed = (
        pending[0]
    )

    assert (
        ingestion_completed.event_id
        == INGESTION_COMPLETED_EVENT_ID
    )

    assert (
        ingestion_completed.event_type
        is EventType.INGESTION_COMPLETED
    )

    assert (
        ingestion_completed
        .correlation_id
        == CORRELATION_ID
    )

    assert (
        ingestion_completed.data[
            "run_id"
        ]
        == expected_run_id
    )

    assert (
        ingestion_completed.data[
            "request_event_id"
        ]
        == str(
            REQUEST_EVENT_ID
        )
    )

    manifest_path_value = (
        ingestion_completed.data[
            "manifest_path"
        ]
    )

    assert isinstance(
        manifest_path_value,
        str,
    )

    manifest_path = (
        lake_root
        / manifest_path_value
    )

    assert (
        manifest_path.is_file()
    )

    raw_payloads = list(
        (
            lake_root
            / "raw"
        ).rglob(
            "payload.json"
        )
    )

    assert len(
        raw_payloads
    ) == 1

    #
    # Perform the real local Spark work.
    #
    # This gives us actual processed
    # Parquet datasets and an actual
    # quality report.
    #
    local_processing_runner = (
        SparkProcessingEventRunner(
            spark=spark,
            lake_root=lake_root,
        )
    )

    processing_reference = (
        local_processing_runner.run(
            provider="bipad",
            endpoint="river-stations",
            run_id=expected_run_id,
            manifest_path=(
                manifest_path_value
            ),
            idempotency_key=(
                INGESTION_COMPLETED_EVENT_ID
            ),
        )
    )

    assert (
        processing_reference.run_id
        == expected_run_id
    )

    station_path_value = (
        processing_reference
        .station_output_path
    )

    observation_path_value = (
        processing_reference
        .observation_output_path
    )

    quality_path_value = (
        processing_reference
        .quality_report_path
    )

    assert isinstance(
        station_path_value,
        str,
    )

    assert isinstance(
        observation_path_value,
        str,
    )

    station_path = (
        lake_root
        / station_path_value
    )

    observation_path = (
        lake_root
        / observation_path_value
    )

    quality_path = (
        lake_root
        / quality_path_value
    )

    assert (
        station_path.is_dir()
    )

    assert (
        observation_path.is_dir()
    )

    assert (
        quality_path.is_file()
    )

    assert (
        station_path
        / "_SUCCESS"
    ).is_file()

    assert (
        observation_path
        / "_SUCCESS"
    ).is_file()

    stations = (
        spark.read.parquet(
            str(
                station_path
            )
        )
    )

    observations = (
        spark.read.parquet(
            str(
                observation_path
            )
        )
    )

    assert (
        stations.count()
        == 1
    )

    assert (
        observations.count()
        == 1
    )

    report_text = (
        quality_path.read_text(
            encoding="utf-8"
        )
    )

    assert (
        f'"run_id":"{expected_run_id}"'
        in report_text
    )

    assert (
        '"endpoint":"river-stations"'
        in report_text
    )

    assert (
        '"total_stations":1'
        in report_text
    )

    #
    # Adapt the real local processing
    # result into the cloud-processing
    # result contract.
    #
    # We are testing the cloud job's
    # completion boundary here; the
    # Spark transformation itself has
    # already run for real above.
    #
    cloud_store = (
        InMemoryProcessingCloudStore()
    )

    cloud_result = (
        CloudProcessingRunResult(
            run_id=expected_run_id,
            endpoint=(
                processing_job
                .BipadEndpoint
                .RIVER_STATIONS
            ),
            outputs=(
                CloudProcessingDatasetResult(
                    dataset=(
                        ProcessedDataset
                        .STATIONS
                    ),
                    key=(
                        station_path_value
                    ),
                    uri=(
                        f"gs://"
                        f"{cloud_store.bucket_name}/"
                        f"{station_path_value}"
                    ),
                    row_count=(
                        stations.count()
                    ),
                ),
                CloudProcessingDatasetResult(
                    dataset=(
                        ProcessedDataset
                        .OBSERVATIONS
                    ),
                    key=(
                        observation_path_value
                    ),
                    uri=(
                        f"gs://"
                        f"{cloud_store.bucket_name}/"
                        f"{observation_path_value}"
                    ),
                    row_count=(
                        observations.count()
                    ),
                ),
            ),
            quality_report_key=(
                quality_path_value
            ),
            quality_report_uri=(
                f"gs://"
                f"{cloud_store.bucket_name}/"
                f"{quality_path_value}"
            ),
        )
    )

    def fake_process_gcs_manifest(
        *,
        spark: SparkSession,
        store: object,
        manifest_key: str,
    ) -> CloudProcessingRunResult:
        _ = (
            spark,
            store,
        )

        assert (
            manifest_key
            == manifest_path_value
        )

        return cloud_result

    monkeypatch.setattr(
        processing_job,
        "process_gcs_manifest",
        fake_process_gcs_manifest,
    )

    #
    # Managed Spark job boundary:
    #
    # This must persist a durable receipt
    # but MUST NOT publish processing.completed.
    #
    completion_event = (
        processing_job.run_processing_job(
            spark=spark,
            store=cloud_store,
            ingestion_event=(
                ingestion_completed
            ),
            clock=lambda: (
                PROCESSING_COMPLETED_TIME
            ),
        )
    )

    assert (
        completion_event.event_type
        is EventType.PROCESSING_COMPLETED
    )

    assert (
        completion_event.event_id
        == processing_job
        .processing_completion_event_id(
            INGESTION_COMPLETED_EVENT_ID
        )
    )

    assert (
        completion_event.occurred_at
        == PROCESSING_COMPLETED_TIME
    )

    assert (
        completion_event.correlation_id
        == CORRELATION_ID
    )

    assert (
        completion_event.data[
            "run_id"
        ]
        == expected_run_id
    )

    assert (
        completion_event.data[
            "ingestion_event_id"
        ]
        == str(
            INGESTION_COMPLETED_EVENT_ID
        )
    )

    assert (
        completion_event.data[
            "request_event_id"
        ]
        == str(
            REQUEST_EVENT_ID
        )
    )

    assert (
        completion_event.data[
            "manifest_path"
        ]
        == manifest_path_value
    )

    assert (
        completion_event.data[
            "quality_report_path"
        ]
        == quality_path_value
    )

    assert (
        completion_event.data[
            "station_output_path"
        ]
        == station_path_value
    )

    assert (
        completion_event.data[
            "observation_output_path"
        ]
        == observation_path_value
    )

    quality_parent = (
        quality_path_value
        .rsplit(
            "/",
            1,
        )[0]
    )

    receipt_key = (
        f"{quality_parent}/"
        "processing-completed.json"
    )

    assert (
        cloud_store.object_exists(
            key=receipt_key
        )
    )

    persisted_receipt = (
        decode_event(
            cloud_store.read_bytes(
                key=receipt_key
            )
        )
    )

    assert (
        persisted_receipt
        == completion_event
    )

    receipt_keys = [
        key
        for key in cloud_store.objects
        if key.endswith(
            "/processing-completed.json"
        )
    ]

    assert receipt_keys == [
        receipt_key
    ]

    #
    # Critical 3C architecture assertion:
    #
    # processing_job did not publish anything.
    # The only pending event is still the
    # ingestion.completed delivery.
    #
    assert (
        queue.list_pending()
        == [ingestion_completed]
    )

    #
    # The processing request has now completed.
    # This represents the successful Cloud Run /
    # Pub/Sub processing delivery being ACKed.
    #
    queue.mark_processed(
        INGESTION_COMPLETED_EVENT_ID
    )

    assert (
        queue.list_pending()
        == []
    )

    #
    # Simulate the real Cloud Storage
    # OBJECT_FINALIZE Pub/Sub notification
    # generated when the durable receipt
    # is created.
    #
    notification = (
        decode_gcs_finalize_push(
            {
                "message": {
                    "attributes": {
                        "eventType": (
                            "OBJECT_FINALIZE"
                        ),
                        "bucketId": (
                            cloud_store
                            .bucket_name
                        ),
                        "objectId": (
                            receipt_key
                        ),
                        "objectGeneration": (
                            "1"
                        ),
                    }
                }
            }
        )
    )

    relayed_event = (
        relay_processing_completion(
            notification=notification,
            store=cloud_store,
            publisher=queue,
        )
    )

    assert (
        relayed_event
        == completion_event
    )

    pending = (
        queue.list_pending()
    )

    assert pending == [
        completion_event
    ]

    processing_completed = (
        pending[0]
    )

    assert (
        processing_completed.event_type
        is EventType.PROCESSING_COMPLETED
    )

    assert (
        processing_completed.event_id
        == completion_event.event_id
    )

    assert (
        processing_completed.occurred_at
        == PROCESSING_COMPLETED_TIME
    )

    assert (
        processing_completed.correlation_id
        == CORRELATION_ID
    )

    #
    # The request and ingestion event
    # deliveries must both be durable
    # processed records now.
    #
    request_processed_path = (
        events_root
        / "processed"
        / f"{REQUEST_EVENT_ID}.json"
    )

    ingestion_processed_path = (
        events_root
        / "processed"
        / (
            f"{INGESTION_COMPLETED_EVENT_ID}"
            ".json"
        )
    )

    assert (
        request_processed_path.is_file()
    )

    assert (
        ingestion_processed_path.is_file()
    )

    assert (
        events_root
        / "idempotency"
        / "ingestion"
        / f"{REQUEST_EVENT_ID}.json"
    ).is_file()

    failed_path = (
        events_root
        / "failed"
    )

    assert (
        not failed_path.is_dir()
        or not any(
            failed_path.iterdir()
        )
    )

    #
    # Nothing should cause ingestion to
    # run again. processing.completed is
    # intentionally irrelevant to the
    # ingestion worker.
    #
    assert (
        ingestion_worker.run_once()
        == 0
    )

    assert len(
        route.calls
    ) == 1