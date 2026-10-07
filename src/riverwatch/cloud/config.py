from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


class CloudConfigurationError(ValueError):
    """Raised when required cloud runtime configuration is invalid."""


def _required_value(
    environment: Mapping[str, str],
    name: str,
) -> str:
    value = environment.get(name)

    if value is None:
        raise CloudConfigurationError(
            f"{name} must be set."
        )

    normalized = value.strip()

    if not normalized:
        raise CloudConfigurationError(
            f"{name} must not be blank."
        )

    return normalized


def _comma_separated_values(
    environment: Mapping[str, str],
    name: str,
) -> tuple[str, ...]:
    raw_value = _required_value(
        environment,
        name,
    )

    values = tuple(
        value.strip()
        for value in raw_value.split(",")
        if value.strip()
    )

    if not values:
        raise CloudConfigurationError(
            f"{name} must contain at least one value."
        )

    return values


def _environment_source(
    environment: Mapping[str, str] | None,
) -> Mapping[str, str]:
    if environment is None:
        return os.environ

    return environment


@dataclass(
    frozen=True,
    slots=True,
)
class IngestionCloudConfig:
    environment: str
    lake_bucket: str
    ingestion_completed_topic: str

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> IngestionCloudConfig:
        source = _environment_source(
            environment
        )

        return cls(
            environment=_required_value(
                source,
                "RIVERWATCH_ENVIRONMENT",
            ),
            lake_bucket=_required_value(
                source,
                "RIVERWATCH_LAKE_BUCKET",
            ),
            ingestion_completed_topic=_required_value(
                source,
                "RIVERWATCH_INGESTION_COMPLETED_TOPIC",
            ),
        )

@dataclass(
    frozen=True,
    slots=True,
)
class ProcessingGatewayCloudConfig:
    environment: str
    project_id: str
    region: str
    runtime_version: str
    workload_service_account: str
    container_image: str
    lake_bucket: str

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[
            str,
            str,
        ] | None = None,
    ) -> ProcessingGatewayCloudConfig:
        source = _environment_source(
            environment
        )

        return cls(
            environment=_required_value(
                source,
                "RIVERWATCH_ENVIRONMENT",
            ),
            project_id=_required_value(
                source,
                "RIVERWATCH_DATAPROC_PROJECT_ID",
            ),
            region=_required_value(
                source,
                "RIVERWATCH_DATAPROC_REGION",
            ),
            runtime_version=_required_value(
                source,
                "RIVERWATCH_DATAPROC_RUNTIME_VERSION",
            ),
            workload_service_account=(
                _required_value(
                    source,
                    (
                        "RIVERWATCH_DATAPROC_"
                        "WORKLOAD_SERVICE_ACCOUNT"
                    ),
                )
            ),
            container_image=_required_value(
                source,
                (
                    "RIVERWATCH_DATAPROC_"
                    "CONTAINER_IMAGE"
                ),
            ),
            lake_bucket=_required_value(
                source,
                "RIVERWATCH_LAKE_BUCKET",
            ),

        )

@dataclass(
    frozen=True,
    slots=True,
)
class ProcessingCompletionRelayCloudConfig:
    lake_bucket: str
    processing_completed_topic: str

    @classmethod
    def from_environment(
        cls,
        source: Mapping[str, str] = os.environ,
    ) -> ProcessingCompletionRelayCloudConfig:
        return cls(
            lake_bucket=_required_value(
                source,
                "RIVERWATCH_LAKE_BUCKET",
            ),
            processing_completed_topic=(
                _required_value(
                    source,
                    "RIVERWATCH_PROCESSING_COMPLETED_TOPIC",
                )
            ),
        )


@dataclass(
    frozen=True,
    slots=True,
)
class ApiCloudConfig:
    environment: str
    analytics_backend: str
    project_id: str
    dataset_id: str
    location: str
    cors_origins: tuple[str, ...]

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> ApiCloudConfig:
        source = _environment_source(
            environment
        )

        analytics_backend = _required_value(
            source,
            "RIVERWATCH_ANALYTICS_BACKEND",
        )

        if analytics_backend != "bigquery":
            raise CloudConfigurationError(
                "RIVERWATCH_ANALYTICS_BACKEND must be "
                "'bigquery' in cloud mode."
            )

        return cls(
            environment=_required_value(
                source,
                "RIVERWATCH_ENVIRONMENT",
            ),
            analytics_backend=analytics_backend,
            project_id=_required_value(
                source,
                "RIVERWATCH_BIGQUERY_PROJECT_ID",
            ),
            dataset_id=_required_value(
                source,
                "RIVERWATCH_BIGQUERY_DATASET_ID",
            ),
            location=_required_value(
                source,
                "RIVERWATCH_BIGQUERY_LOCATION",
            ),
            cors_origins=_comma_separated_values(
                source,
                "RIVERWATCH_CORS_ORIGINS",
            ),
        )