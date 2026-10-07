from __future__ import annotations

from datetime import datetime

import pytest
from fastapi.testclient import (
    TestClient,
)

from riverwatch.analytics.model import (
    BasinCurrentSummary,
    CurrentNetworkSummary,
    CurrentRiverStation,
    StationObservation,
)
from riverwatch.cloud import api_app
from riverwatch.cloud.api_app import (
    create_cloud_api_app,
)
from riverwatch.cloud.config import (
    ApiCloudConfig,
)


def cloud_config(
    *,
    analytics_backend: str = "bigquery",
) -> ApiCloudConfig:
    return ApiCloudConfig(
        environment="dev",
        analytics_backend=(
            analytics_backend
        ),
        project_id="riverwatch-test",
        dataset_id=(
            "riverwatch_dev_analytics"
        ),
        location="asia-south1",
        cors_origins=(
            "https://example.test",
        ),
    )


class FakeAnalyticsService:
    def __init__(
        self,
    ) -> None:
        self.closed = False

    def get_current_snapshot(
        self,
    ) -> tuple[
        CurrentRiverStation,
        ...,
    ]:
        return ()

    def get_station(
        self,
        station_id: str,
    ) -> CurrentRiverStation | None:
        return None

    def get_station_history(
        self,
        station_id: str,
        *,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> tuple[
        StationObservation,
        ...,
    ]:
        return ()

    def get_basin_summaries(
        self,
    ) -> tuple[
        BasinCurrentSummary,
        ...,
    ]:
        return ()

    def get_network_summary(
        self,
    ) -> CurrentNetworkSummary:
        return CurrentNetworkSummary(
            basins_represented=0,
            total_stations=0,
            stations_with_observation=0,
            stations_without_observation=0,
            fresh_observations=0,
            stale_observations=0,
            future_observations=0,
            unassessable_observations=0,
            observations_with_water_level=0,
            observations_without_water_level=0,
            oldest_observed_at=None,
            latest_observed_at=None,
            coverage_ratio=0.0,
            freshness_ratio=0.0,
        )

    def close(
        self,
    ) -> None:
        self.closed = True


def test_cloud_app_uses_injected_service(
) -> None:
    service = FakeAnalyticsService()

    app = create_cloud_api_app(
        config=cloud_config(),
        analytics_service_factory=(
            lambda: service
        ),
    )

    with TestClient(
        app
    ) as client:
        response = client.get(
            "/api/v1/health/live"
        )

        assert (
            response.status_code
            == 200
        )

        assert (
            app.state.analytics_service
            is service
        )

    assert service.closed is True

    assert (
        app.state.analytics_service
        is None
    )


def test_cloud_app_uses_configured_cors(
) -> None:
    app = create_cloud_api_app(
        config=cloud_config(),
        analytics_service_factory=(
            FakeAnalyticsService
        ),
    )

    with TestClient(
        app
    ) as client:
        response = client.options(
            "/api/v1/health/live",
            headers={
                "Origin": (
                    "https://example.test"
                ),
                (
                    "Access-Control-"
                    "Request-Method"
                ): "GET",
            },
        )

    assert (
        response.status_code
        == 200
    )

    assert (
        response.headers[
            "access-control-allow-origin"
        ]
        == "https://example.test"
    )


def test_cloud_app_builds_bigquery_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[
        str,
        str,
    ] = {}

    service = FakeAnalyticsService()

    class FakeBigQueryAnalyticsService:
        def __init__(
            self,
            *,
            project_id: str,
            dataset_id: str,
            location: str,
        ) -> None:
            captured.update(
                {
                    "project_id":
                        project_id,
                    "dataset_id":
                        dataset_id,
                    "location":
                        location,
                }
            )

        def get_current_snapshot(
            self,
        ) -> tuple[
            CurrentRiverStation,
            ...,
        ]:
            return (
                service
                .get_current_snapshot()
            )

        def get_station(
            self,
            station_id: str,
        ) -> CurrentRiverStation | None:
            return service.get_station(
                station_id
            )

        def get_station_history(
            self,
            station_id: str,
            *,
            start_at: (
                datetime | None
            ) = None,
            end_at: (
                datetime | None
            ) = None,
        ) -> tuple[
            StationObservation,
            ...,
        ]:
            return (
                service
                .get_station_history(
                    station_id,
                    start_at=start_at,
                    end_at=end_at,
                )
            )

        def get_basin_summaries(
            self,
        ) -> tuple[
            BasinCurrentSummary,
            ...,
        ]:
            return (
                service
                .get_basin_summaries()
            )

        def get_network_summary(
            self,
        ) -> CurrentNetworkSummary:
            return (
                service
                .get_network_summary()
            )

        def close(
            self,
        ) -> None:
            service.close()

    monkeypatch.setattr(
        api_app,
        "BigQueryAnalyticsService",
        FakeBigQueryAnalyticsService,
    )

    app = create_cloud_api_app(
        config=cloud_config()
    )

    with TestClient(
        app
    ) as client:
        response = client.get(
            "/api/v1/health/live"
        )

    assert (
        response.status_code
        == 200
    )

    assert captured == {
        "project_id":
            "riverwatch-test",
        "dataset_id":
            "riverwatch_dev_analytics",
        "location":
            "asia-south1",
    }

    assert service.closed is True


def test_cloud_app_rejects_non_bigquery_backend(
) -> None:
    with pytest.raises(
        ValueError,
        match=(
            "must be 'bigquery'"
        ),
    ):
        create_cloud_api_app(
            config=cloud_config(
                analytics_backend="duckdb"
            ),
            analytics_service_factory=(
                FakeAnalyticsService
            ),
        )