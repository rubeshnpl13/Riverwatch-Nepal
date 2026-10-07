from __future__ import annotations

from fastapi import FastAPI

from riverwatch.analytics.bigquery import (
    BigQueryAnalyticsService,
)
from riverwatch.api.app import (
    AnalyticsServiceFactory,
    create_app,
)
from riverwatch.cloud.config import (
    ApiCloudConfig,
)


def create_cloud_api_app(
    *,
    config: ApiCloudConfig | None = None,
    analytics_service_factory: (
        AnalyticsServiceFactory
        | None
    ) = None,
) -> FastAPI:
    resolved_config = (
        ApiCloudConfig.from_environment()
        if config is None
        else config
    )

    if (
        resolved_config.analytics_backend
        != "bigquery"
    ):
        raise ValueError(
            "Cloud API analytics backend "
            "must be 'bigquery'"
        )

    if analytics_service_factory is None:

        def build_bigquery_service() -> BigQueryAnalyticsService:
            return BigQueryAnalyticsService(
                project_id=(
                    resolved_config
                    .project_id
                ),
                dataset_id=(
                    resolved_config
                    .dataset_id
                ),
                location=(
                    resolved_config
                    .location
                ),
            )

        resolved_factory: AnalyticsServiceFactory = (
            build_bigquery_service
        )

    else:
        resolved_factory = (
            analytics_service_factory
        )

    return create_app(
        cors_origins=(
            resolved_config.cors_origins
        ),
        analytics_service_factory=(
            resolved_factory
        ),
    )