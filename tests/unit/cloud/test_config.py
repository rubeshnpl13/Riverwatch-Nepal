import pytest

from riverwatch.cloud.config import (
    ApiCloudConfig,
    CloudConfigurationError,
    IngestionCloudConfig,
    ProcessingGatewayCloudConfig,
)


def test_ingestion_cloud_config_loads_environment() -> None:
    config = IngestionCloudConfig.from_environment(
        {
            "RIVERWATCH_ENVIRONMENT": " dev ",
            "RIVERWATCH_LAKE_BUCKET": " riverwatch-lake ",
            "RIVERWATCH_INGESTION_COMPLETED_TOPIC": (
                "projects/test/topics/ingestion-completed"
            ),
        }
    )

    assert config.environment == "dev"
    assert config.lake_bucket == "riverwatch-lake"

    assert config.ingestion_completed_topic == (
        "projects/test/topics/ingestion-completed"
    )


def test_ingestion_cloud_config_rejects_missing_value() -> None:
    with pytest.raises(
        CloudConfigurationError,
        match="RIVERWATCH_LAKE_BUCKET must be set",
    ):
        IngestionCloudConfig.from_environment(
            {
                "RIVERWATCH_ENVIRONMENT": "dev",
                "RIVERWATCH_INGESTION_COMPLETED_TOPIC": (
                    "projects/test/topics/ingestion-completed"
                ),
            }
        )


def test_processing_gateway_cloud_config_loads_environment() -> None:
    config = (
        ProcessingGatewayCloudConfig.from_environment(
            {
                "RIVERWATCH_ENVIRONMENT": "dev",
                "RIVERWATCH_DATAPROC_PROJECT_ID": (
                    "riverwatch-test"
                ),
                "RIVERWATCH_DATAPROC_REGION": (
                    "asia-south1"
                ),
                "RIVERWATCH_DATAPROC_RUNTIME_VERSION": (
                    "3.0"
                ),
                "RIVERWATCH_DATAPROC_WORKLOAD_SERVICE_ACCOUNT": (
                    "processing@example.iam.gserviceaccount.com"
                ),
                "RIVERWATCH_DATAPROC_CONTAINER_IMAGE": (
                    "asia-south1-docker.pkg.dev/"
                    "riverwatch-test/repository/"
                    "processing-spark:test"
                ),
            }
        )
    )

    assert config.environment == "dev"
    assert config.project_id == "riverwatch-test"
    assert config.region == "asia-south1"
    assert config.runtime_version == "3.0"

    assert config.workload_service_account == (
        "processing@example.iam.gserviceaccount.com"
    )


def test_api_cloud_config_loads_environment() -> None:
    config = ApiCloudConfig.from_environment(
        {
            "RIVERWATCH_ENVIRONMENT": "dev",
            "RIVERWATCH_ANALYTICS_BACKEND": "bigquery",
            "RIVERWATCH_BIGQUERY_PROJECT_ID": (
                "riverwatch-test"
            ),
            "RIVERWATCH_BIGQUERY_DATASET_ID": (
                "riverwatch_dev_analytics"
            ),
            "RIVERWATCH_BIGQUERY_LOCATION": (
                "asia-south1"
            ),
            "RIVERWATCH_CORS_ORIGINS": (
                "http://localhost:5173, "
                "https://riverwatch.example"
            ),
        }
    )

    assert config.analytics_backend == "bigquery"
    assert config.project_id == "riverwatch-test"

    assert config.cors_origins == (
        "http://localhost:5173",
        "https://riverwatch.example",
    )


def test_api_cloud_config_rejects_non_bigquery_backend() -> None:
    with pytest.raises(
        CloudConfigurationError,
        match="must be 'bigquery'",
    ):
        ApiCloudConfig.from_environment(
            {
                "RIVERWATCH_ENVIRONMENT": "dev",
                "RIVERWATCH_ANALYTICS_BACKEND": "duckdb",
                "RIVERWATCH_BIGQUERY_PROJECT_ID": (
                    "riverwatch-test"
                ),
                "RIVERWATCH_BIGQUERY_DATASET_ID": (
                    "riverwatch_dev_analytics"
                ),
                "RIVERWATCH_BIGQUERY_LOCATION": (
                    "asia-south1"
                ),
                "RIVERWATCH_CORS_ORIGINS": (
                    "http://localhost:5173"
                ),
            }
        )