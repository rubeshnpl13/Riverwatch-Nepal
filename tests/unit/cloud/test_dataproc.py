from __future__ import annotations

import base64
from collections.abc import Mapping
from datetime import (
    UTC,
    datetime,
)
from uuid import UUID

import pytest
from google.api_core.exceptions import (
    AlreadyExists,
    ServiceUnavailable,
)

from riverwatch.cloud.dataproc import (
    DataprocBatchSubmission,
    ManagedSparkBatchSubmitter,
    ManagedSparkSubmissionError,
    processing_batch_id,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.serialization import (
    decode_event,
)

EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

CORRELATION_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

REQUEST_EVENT_ID = UUID(
    "33333333-3333-3333-3333-333333333333"
)
LAKE_BUCKET = (
    "riverwatch-test-lake"
)



def make_event(
    *,
    event_id: UUID = EVENT_ID,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id,
        event_type=(
            EventType
            .INGESTION_COMPLETED
        ),
        occurred_at=datetime(
            2026,
            10,
            5,
            0,
            0,
            tzinfo=UTC,
        ),
        correlation_id=(
            CORRELATION_ID
        ),
        data={
            "provider": "bipad",
            "endpoint": "river-stations",
            "run_id": "run-123",
            "manifest_path": (
                "manifests/run-123/"
                "manifest.json"
            ),
            "request_event_id": str(
                REQUEST_EVENT_ID
            ),
        },
    )


class FakeBatchClient:
    def __init__(
        self,
        *,
        error: Exception | None = None,
    ) -> None:
        self.error = error

        self.requests: list[
            Mapping[str, object]
        ] = []

    def create_batch(
        self,
        *,
        request: Mapping[str, object],
    ) -> object:
        self.requests.append(
            request
        )

        if self.error is not None:
            raise self.error

        return object()


def make_submitter(
    client: FakeBatchClient,
) -> ManagedSparkBatchSubmitter:
    return ManagedSparkBatchSubmitter(
        project_id="riverwatch-test",
        region="asia-south1",
        runtime_version="3.0",
        workload_service_account=(
            "processing@riverwatch-test."
            "iam.gserviceaccount.com"
        ),
        container_image=(
            "asia-south1-docker.pkg.dev/"
            "riverwatch-test/riverwatch/"
            "processing-spark:test"
        ),

        client=client,
        lake_bucket=LAKE_BUCKET,
    )


def test_submits_managed_spark_batch() -> None:
    client = FakeBatchClient()

    submitter = make_submitter(
        client
    )

    result = submitter.submit(
        make_event()
    )

    assert result == (
        DataprocBatchSubmission(
            batch_id=(
                "rw-processing-"
                f"{EVENT_ID}"
            ),
            resource_name=(
                "projects/riverwatch-test/"
                "locations/asia-south1/"
                "batches/"
                "rw-processing-"
                f"{EVENT_ID}"
            ),
        )
    )

    assert len(
        client.requests
    ) == 1

    request = client.requests[0]

    assert request[
        "parent"
    ] == (
        "projects/riverwatch-test/"
        "locations/asia-south1"
    )

    assert request[
        "batch_id"
    ] == (
        "rw-processing-"
        f"{EVENT_ID}"
    )


def test_batch_contains_runtime_contract() -> None:
    client = FakeBatchClient()

    make_submitter(
        client
    ).submit(
        make_event()
    )

    batch = client.requests[0][
        "batch"
    ]

    assert isinstance(
        batch,
        dict,
    )

    assert batch[
        "runtime_config"
    ] == {
        "version": "3.0",
        "container_image": (
            "asia-south1-docker.pkg.dev/"
            "riverwatch-test/riverwatch/"
            "processing-spark:test"
        ),
    }

    assert batch[
        "environment_config"
    ] == {
        "execution_config": {
            "service_account": (
                "processing@"
                "riverwatch-test."
                "iam.gserviceaccount.com"
            ),
        }
    }


def test_batch_passes_exact_event_to_driver() -> None:
    client = FakeBatchClient()

    event = make_event()

    make_submitter(
        client
    ).submit(
        event
    )

    batch = client.requests[0][
        "batch"
    ]

    assert isinstance(
        batch,
        dict,
    )

    pyspark_batch = batch[

        "pyspark_batch"
    ]

    assert isinstance(
        pyspark_batch,
        dict,
    )

    assert (
        pyspark_batch[
            "main_python_file_uri"
        ]
        == (
            "file:///opt/riverwatch/"
            "processing_job.py"
        )
    )

    args = pyspark_batch[
        "args"
    ]

    assert isinstance(
        args,
        list,
    )

    args = pyspark_batch[
        "args"
    ]

    assert isinstance(
        args,
        list,
    )

    encoded = args[1]

    assert isinstance(
        encoded,
        str,
    )

    expected_encoded_event = encoded

    assert args == [
        "--event-json-base64",
        expected_encoded_event,
        "--lake-bucket",
        LAKE_BUCKET,
    ]

    decoded = decode_event(
        base64.b64decode(
            expected_encoded_event
        )
    )

    assert decoded == event




def test_batch_id_is_deterministic() -> None:
    event = make_event()

    assert (
        processing_batch_id(
            event
        )
        == processing_batch_id(
            event
        )
    )


def test_different_event_has_different_batch_id() -> None:
    other_id = UUID(
        "44444444-4444-4444-4444-444444444444"
    )

    assert (
        processing_batch_id(
            make_event()
        )
        != processing_batch_id(
            make_event(
                event_id=other_id
            )
        )
    )


def test_existing_batch_is_idempotent_success() -> None:
    error = AlreadyExists(  # type: ignore[no-untyped-call]
        "already exists"
    )

    submitter = make_submitter(
        FakeBatchClient(
            error=error
        )
    )

    result = submitter.submit(
        make_event()
    )

    assert result.batch_id == (
        "rw-processing-"
        f"{EVENT_ID}"
    )


def test_google_api_failure_is_mapped() -> None:
    error = ServiceUnavailable(  # type: ignore[no-untyped-call]
        "Dataproc unavailable"
    )

    submitter = make_submitter(
        FakeBatchClient(
            error=error
        )
    )

    with pytest.raises(
        ManagedSparkSubmissionError,
        match="Unable to submit",
    ):
        submitter.submit(
            make_event()
        )


def test_rejects_wrong_event_type() -> None:
    event = EventEnvelope(
        event_id=EVENT_ID,
        event_type=(
            EventType
            .INGESTION_REQUESTED
        ),
        occurred_at=datetime(
            2026,
            10,
            5,
            tzinfo=UTC,
        ),
        correlation_id=(
            CORRELATION_ID
        ),
        data={
            "provider": "bipad",
            "endpoint": "river-stations",
        },
    )

    with pytest.raises(
        ValueError,
        match="ingestion.completed",
    ):
        make_submitter(
            FakeBatchClient()
        ).submit(
            event
        )


@pytest.mark.parametrize(
    (
        "field",
        "value",
    ),
    [
        ("project_id", " "),
        ("region", ""),
        ("runtime_version", " "),
        (
            "workload_service_account",
            "",
        ),
        ("container_image", " "),
        ("main_python_file_uri", ""),
    ],
)
def test_rejects_blank_configuration(
    field: str,
    value: str,
) -> None:
    values = {
        "project_id":
            "riverwatch-test",
        "region":
            "asia-south1",
        "runtime_version":
            "3.0",
        "workload_service_account": (
            "processing@example."
            "iam.gserviceaccount.com"
        ),
        "container_image":
            "example/image:test",
        "main_python_file_uri": (
            "file:///opt/riverwatch/"
            "processing_job.py"
        ),
    }

    values[field] = value

    with pytest.raises(
        ValueError,
        match=(
            f"{field} must not be blank"
        ),
    ):
        ManagedSparkBatchSubmitter(
            **values,
            client=FakeBatchClient(),
            lake_bucket=LAKE_BUCKET,
        )