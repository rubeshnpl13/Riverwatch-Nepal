from __future__ import annotations

from typing import Protocol

from riverwatch.cloud.config import (
    ProcessingGatewayCloudConfig,
)
from riverwatch.cloud.dataproc import (
    DataprocBatchSubmission,
    ManagedSparkBatchSubmitter,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)


class ProcessingBatchSubmitter(
    Protocol,
):
    def submit(
        self,
        event: EventEnvelope,
    ) -> DataprocBatchSubmission:
        ...


class ProcessingGatewayHandler:
    """Submit one ingestion.completed event to Managed Spark."""

    def __init__(
        self,
        *,
        submitter: ProcessingBatchSubmitter,
    ) -> None:
        self._submitter = submitter

    def handle(
        self,
        event: EventEnvelope,
    ) -> DataprocBatchSubmission:
        if (
            event.event_type
            is not EventType
            .INGESTION_COMPLETED
        ):
            raise ValueError(
                "Processing gateway requires "
                "an ingestion.completed event"
            )

        self._required_text(
            event,
            "provider",
        )

        self._required_text(
            event,
            "endpoint",
        )

        self._required_text(
            event,
            "run_id",
        )

        self._required_text(
            event,
            "manifest_path",
        )

        self._required_text(
            event,
            "request_event_id",
        )

        return self._submitter.submit(
            event
        )

    @staticmethod
    def _required_text(
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
            raise ValueError(
                f"{key} must be a string"
            )

        normalized = value.strip()

        if not normalized:
            raise ValueError(
                f"{key} must not be blank"
            )

        return normalized


def build_processing_gateway_handler(
    *,
    config: (
        ProcessingGatewayCloudConfig
        | None
    ) = None,
) -> ProcessingGatewayHandler:
    resolved_config = (
        ProcessingGatewayCloudConfig
        .from_environment()
        if config is None
        else config
    )

    submitter = (
        ManagedSparkBatchSubmitter(
            project_id=(
                resolved_config
                .project_id
            ),
            region=(
                resolved_config
                .region
            ),
            runtime_version=(
                resolved_config
                .runtime_version
            ),
            workload_service_account=(
                resolved_config
                .workload_service_account
            ),
            container_image=(
                resolved_config
                .container_image
            ),
            lake_bucket=(
                resolved_config.lake_bucket
            ),
        )
    )

    return ProcessingGatewayHandler(
        submitter=submitter
    )