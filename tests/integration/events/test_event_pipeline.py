import json
from datetime import (
    UTC,
    datetime,
)
from pathlib import Path
from uuid import UUID

import httpx
import respx
from pyspark.sql import SparkSession

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
from riverwatch.events.processing import (
    ProcessingWorker,
)
from riverwatch.events.processing_idempotency import (
    LocalProcessingExecutionStore,
)
from riverwatch.events.processing_retry import (
    LocalProcessingRetryStore,
)
from riverwatch.events.retry import (
    LocalIngestionRetryStore,
)
from riverwatch.events.worker import (
    IngestionWorker,
)
from riverwatch.ingestion.bipad.event_runner import (
    BipadEventIngestionRunner,
)
from riverwatch.processing.spark.event_runner import (
    SparkProcessingEventRunner,
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

PROCESSING_COMPLETED_EVENT_ID = UUID(
    "33333333-3333-3333-3333-333333333333"
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
def test_event_pipeline_ingests_processes_and_publishes_completion(
    spark: SparkSession,
    tmp_path: Path,
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
            clock=lambda: REQUEST_TIME,
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

    pending = (
        queue.list_pending()
    )

    assert pending == [
        request
    ]

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

    processing_execution_store = (
        LocalProcessingExecutionStore(
            events_root=events_root,
        )
    )

    processing_retry_store = (
        LocalProcessingRetryStore(
            events_root=events_root,
        )
    )

    processing_runner = (
        SparkProcessingEventRunner(
            spark=spark,
            lake_root=lake_root,
        )
    )

    processing_worker = (
        ProcessingWorker(
            queue=queue,
            runner=processing_runner,
            execution_store=(
                processing_execution_store
            ),
            retry_store=(
                processing_retry_store
            ),
            clock=lambda: (
                PROCESSING_COMPLETED_TIME
            ),
            uuid_factory=lambda: (
                PROCESSING_COMPLETED_EVENT_ID
            ),
        )
    )

    assert (
        processing_worker.run_once()
        == 1
    )

    assert (
        processing_retry_store.get(
            INGESTION_COMPLETED_EVENT_ID
        )
        is None
    )

    processing_execution = (
        processing_execution_store.get(
            INGESTION_COMPLETED_EVENT_ID
        )
    )

    assert (
        processing_execution
        is not None
    )

    assert (
        processing_execution.run_id
        == expected_run_id
    )

    assert (
        processing_execution
        .completion_event_id
        == PROCESSING_COMPLETED_EVENT_ID
    )

    pending = (
        queue.list_pending()
    )

    assert len(
        pending
    ) == 1

    processing_completed = (
        pending[0]
    )

    assert (
        processing_completed.event_id
        == PROCESSING_COMPLETED_EVENT_ID
    )

    assert (
        processing_completed.event_type
        is EventType.PROCESSING_COMPLETED
    )

    assert (
        processing_completed
        .correlation_id
        == CORRELATION_ID
    )

    assert (
        processing_completed.data[
            "run_id"
        ]
        == expected_run_id
    )

    assert (
        processing_completed.data[
            "ingestion_event_id"
        ]
        == str(
            INGESTION_COMPLETED_EVENT_ID
        )
    )

    assert (
        processing_completed.data[
            "request_event_id"
        ]
        == str(
            REQUEST_EVENT_ID
        )
    )

    station_path_value = (
        processing_completed.data[
            "station_output_path"
        ]
    )

    observation_path_value = (
        processing_completed.data[
            "observation_output_path"
        ]
    )

    quality_path_value = (
        processing_completed.data[
            "quality_report_path"
        ]
    )

    assert isinstance(
        station_path_value,
        str,
    )

    assert isinstance(
        observation_path_value,
        str,
    )

    assert isinstance(
        quality_path_value,
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

    assert (
        events_root
        / "idempotency"
        / "processing"
        / (
            f"{INGESTION_COMPLETED_EVENT_ID}"
            ".json"
        )
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

    # No stage should rerun work now.
    # The only pending delivery is
    # processing.completed, which both
    # workers intentionally ignore.
    assert (
        ingestion_worker.run_once()
        == 0
    )

    assert (
        processing_worker.run_once()
        == 0
    )

    assert len(
        route.calls
    ) == 1