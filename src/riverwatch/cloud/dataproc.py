from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, cast

from google.api_core.exceptions import (
    AlreadyExists,
    GoogleAPIError,
)
from google.cloud import dataproc_v1

from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.serialization import (
    encode_event,
)

DEFAULT_MAIN_PYTHON_FILE_URI = (
    "file:///opt/riverwatch/"
    "processing_job.py"
)


class ManagedSparkSubmissionError(
    RuntimeError
):
    """Managed Spark batch submission failed."""


@dataclass(
    frozen=True,
    slots=True,
)
class DataprocBatchSubmission:
    batch_id: str
    resource_name: str

    def __post_init__(
        self,
    ) -> None:
        if not self.batch_id.strip():
            raise ValueError(
                "batch_id must not be blank"
            )

        if not self.resource_name.strip():
            raise ValueError(
                "resource_name must not be blank"
            )


class _BatchControllerClient(
    Protocol,
):
    def create_batch(
        self,
        *,
        request: Mapping[str, object],
    ) -> object:
        ...


def processing_batch_id(
    event: EventEnvelope,
) -> str:
    return (
        "rw-processing-"
        f"{event.event_id}"
    )


class ManagedSparkBatchSubmitter:
    """Submit RiverWatch processing to Managed Spark."""

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        runtime_version: str,
        workload_service_account: str,
        container_image: str,
        lake_bucket: str,
        main_python_file_uri: str = (
            DEFAULT_MAIN_PYTHON_FILE_URI
        ),
        client: (
            _BatchControllerClient | None
        ) = None,
    ) -> None:
        self._project_id = (
            self._required_value(
                project_id,
                "project_id",
            )
        )

        self._region = (
            self._required_value(
                region,
                "region",
            )
        )

        self._runtime_version = (
            self._required_value(
                runtime_version,
                "runtime_version",
            )
        )

        self._workload_service_account = (
            self._required_value(
                workload_service_account,
                "workload_service_account",
            )
        )

        self._container_image = (
            self._required_value(
                container_image,
                "container_image",
            )
        )
        self._lake_bucket = (
            self._required_value(
                lake_bucket,
                "lake_bucket",
            )
        )

        self._main_python_file_uri = (
            self._required_value(
                main_python_file_uri,
                "main_python_file_uri",
            )
        )

        if client is None:
            resolved_client = (
                dataproc_v1
                .BatchControllerClient(
                    client_options={
                        "api_endpoint": (
                            f"{self._region}-"
                            "dataproc.googleapis.com:"
                            "443"
                        )
                    }
                )
            )

            self._client = cast(
                _BatchControllerClient,
                resolved_client,
            )
        else:
            self._client = client

    def submit(
        self,
        event: EventEnvelope,
    ) -> DataprocBatchSubmission:
        if (
            event.event_type
            is not EventType
            .INGESTION_COMPLETED
        ):
            raise ValueError(
                "Managed Spark submitter "
                "requires an "
                "ingestion.completed event"
            )

        batch_id = processing_batch_id(
            event
        )

        parent = (
            f"projects/{self._project_id}/"
            f"locations/{self._region}"
        )

        resource_name = (
            f"{parent}/batches/{batch_id}"
        )

        encoded_event = (
            base64.b64encode(
                encode_event(
                    event
                )
            ).decode(
                "ascii"
            )
        )

        request: dict[
            str,
            object,
        ] = {
            "parent": parent,
            "batch_id": batch_id,
            "batch": {
                "pyspark_batch": {
                    "main_python_file_uri": (
                        self
                        ._main_python_file_uri
                    ),
                    "args": [
                        "--event-json-base64",
                        encoded_event,
                        "--lake-bucket",
                        self._lake_bucket,
                    ],
                },
                "runtime_config": {
                    "version": (
                        self
                        ._runtime_version
                    ),
                    "container_image": (
                        self
                        ._container_image
                    ),
                },
                "environment_config": {
                    "execution_config": {
                        "service_account": (
                            self
                            ._workload_service_account
                        ),
                    }
                },
            },
        }

        try:
            self._client.create_batch(
                request=request,
            )

        except AlreadyExists:
            # The batch ID is deterministic.
            # A retry after a successful
            # submission therefore resolves
            # to the same batch.
            pass

        except GoogleAPIError as exc:
            raise (
                ManagedSparkSubmissionError(
                    "Unable to submit "
                    "Managed Spark batch "
                    f"{batch_id}"
                )
            ) from exc

        return DataprocBatchSubmission(
            batch_id=batch_id,
            resource_name=resource_name,
        )

    @staticmethod
    def _required_value(
        value: str,
        name: str,
    ) -> str:
        normalized = value.strip()

        if not normalized:
            raise ValueError(
                f"{name} must not be blank"
            )

        return normalized