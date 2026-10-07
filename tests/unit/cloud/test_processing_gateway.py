from __future__ import annotations

from datetime import (
    UTC,
    datetime,
)
from uuid import UUID

import pytest

import riverwatch.cloud.processing_gateway as processing_gateway
from riverwatch.cloud.config import (
    ProcessingGatewayCloudConfig,
)
from riverwatch.cloud.dataproc import (
    DataprocBatchSubmission,
)
from riverwatch.cloud.processing_gateway import (
    ProcessingGatewayHandler,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)

INGESTION_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

CORRELATION_ID = UUID(
    "22222222-2222-2222-2222-222222222222"
)

REQUEST_EVENT_ID = UUID(
    "33333333-3333-3333-3333-333333333333"
)


def make_event(
    *,
    data: dict[str, str] | None = None,
    event_type: EventType = (
        EventType.INGESTION_COMPLETED
    ),
) -> EventEnvelope:
    resolved_data = (
        {
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
        }
        if data is None
        else data
    )

    return EventEnvelope(
        event_id=INGESTION_EVENT_ID,
        event_type=event_type,
        occurred_at=datetime(
            2026,
            10,
            5,
            0,
            0,
            tzinfo=UTC,
        ),
        correlation_id=CORRELATION_ID,
        data=resolved_data,
    )


class RecordingSubmitter:
    def __init__(
        self,
    ) -> None:
        self.events: list[
            EventEnvelope
        ] = []

        self.result = DataprocBatchSubmission(
            batch_id=(
                "rw-processing-"
                f"{INGESTION_EVENT_ID}"
            ),
            resource_name=(
                "projects/test/"
                "locations/asia-south1/"
                "batches/"
                "rw-processing-"
                f"{INGESTION_EVENT_ID}"
            ),
        )

    def submit(
        self,
        event: EventEnvelope,
    ) -> DataprocBatchSubmission:
        self.events.append(event)

        return self.result


class FailingSubmitter:
    @staticmethod
    def submit(
        event: EventEnvelope,
    ) -> DataprocBatchSubmission:
        del event

        raise RuntimeError(
            "submission failed"
        )


def test_submits_ingestion_completed_event() -> None:
    submitter = RecordingSubmitter()

    handler = ProcessingGatewayHandler(
        submitter=submitter
    )

    event = make_event()

    result = handler.handle(event)

    assert result == submitter.result

    assert submitter.events == [event]


def test_submission_failure_propagates() -> None:
    handler = ProcessingGatewayHandler(
        submitter=FailingSubmitter()
    )

    with pytest.raises(
        RuntimeError,
        match="submission failed",
    ):
        handler.handle(make_event())


def test_rejects_wrong_event_type() -> None:
    handler = ProcessingGatewayHandler(
        submitter=RecordingSubmitter()
    )

    with pytest.raises(
        ValueError,
        match="ingestion.completed",
    ):
        handler.handle(
            make_event(
                event_type=(
                    EventType.INGESTION_REQUESTED
                )
            )
        )


@pytest.mark.parametrize(
    (
        "field",
        "value",
        "message",
    ),
    [
        (
            "provider",
            "",
            "provider must not be blank",
        ),
        (
            "endpoint",
            "   ",
            "endpoint must not be blank",
        ),
        (
            "run_id",
            "",
            "run_id must not be blank",
        ),
        (
            "manifest_path",
            "   ",
            (
                "manifest_path must "
                "not be blank"
            ),
        ),
        (
            "request_event_id",
            "",
            (
                "request_event_id must "
                "not be blank"
            ),
        ),
    ],
)
def test_rejects_blank_required_data(
    field: str,
    value: str,
    message: str,
) -> None:
    data = {
        "provider": "bipad",
        "endpoint": "river-stations",
        "run_id": "run-123",
        "manifest_path": "manifest.json",
        "request_event_id": str(
            REQUEST_EVENT_ID
        ),
    }

    data[field] = value

    handler = ProcessingGatewayHandler(
        submitter=RecordingSubmitter()
    )

    with pytest.raises(
        ValueError,
        match=message,
    ):
        handler.handle(
            make_event(data=data)
        )

def test_build_handler_uses_cloud_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    class FakeManagedSparkBatchSubmitter:
        def __init__(
            self,
            *,
            project_id: str,
            region: str,
            runtime_version: str,
            workload_service_account: str,
            container_image: str,
            lake_bucket: str,
        ) -> None:
            captured["project_id"] = project_id
            captured["region"] = region
            captured["runtime_version"] = (
                runtime_version
            )
            captured["workload_service_account"] = (
                workload_service_account
            )
            captured["container_image"] = (
                container_image
            )
            captured["lake_bucket"] = (
                lake_bucket
            )
        @staticmethod
        def submit(
            event: EventEnvelope,
        ) -> DataprocBatchSubmission:
            del event

            return DataprocBatchSubmission(
                batch_id="batch-test",
                resource_name=(
                    "projects/test/"
                    "locations/test/"
                    "batches/batch-test"
                ),
            )

    monkeypatch.setattr(
        processing_gateway,
        "ManagedSparkBatchSubmitter",
        FakeManagedSparkBatchSubmitter,
    )

    config = ProcessingGatewayCloudConfig(
        environment="dev",
        project_id="riverwatch-test",
        region="asia-south1",
        runtime_version="3.0",
        workload_service_account=(
            "processing@"
            "riverwatch-test."
            "iam.gserviceaccount.com"
        ),
        container_image=(
            "example/"
            "processing-spark:test"
        ),
        lake_bucket=(
            "riverwatch-test-lake"
        )
    )

    processing_gateway.build_processing_gateway_handler(
        config=config
    )

    assert captured == {
        "project_id": "riverwatch-test",
        "region": "asia-south1",
        "runtime_version": "3.0",
        "workload_service_account": (
            "processing@"
            "riverwatch-test."
            "iam.gserviceaccount.com"
        ),
        "container_image": (
            "example/"
            "processing-spark:test"
        ),
        "lake_bucket": (
            "riverwatch-test-lake"
        ),
    }
