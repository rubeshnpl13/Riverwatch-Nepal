from datetime import (
    UTC,
    datetime,
)
from unittest.mock import (
    MagicMock,
)

from fastapi.testclient import (
    TestClient,
)

from riverwatch.analytics.model import (
    BasinCurrentSummary,
    CurrentNetworkSummary,
)
from riverwatch.analytics.service import (
    AnalyticsService,
)
from riverwatch.api.app import (
    API_PREFIX,
    create_app,
)
from riverwatch.api.dependencies import (
    get_analytics_service,
)


def _create_test_app(
    *,
    monkeypatch,
    analytics: AnalyticsService,
):
    connection = MagicMock()

    def fake_create_connection(
        *,
        lake_root,
    ):
        del lake_root

        return connection

    monkeypatch.setattr(
        "riverwatch.api.app."
        "create_analytics_connection",
        fake_create_connection,
    )

    app = create_app()

    app.dependency_overrides[
        get_analytics_service
    ] = lambda: analytics

    return app

#Test basin summaries

def test_list_basin_summaries(
    monkeypatch,
) -> None:
    analytics = MagicMock(
        spec=AnalyticsService
    )

    analytics.get_basin_summaries.return_value = (
        BasinCurrentSummary(
            basin_name="Koshi",
            total_stations=50,
            stations_with_observation=49,
            stations_without_observation=1,
            fresh_observations=30,
            stale_observations=19,
            future_observations=0,
            unassessable_observations=0,
            observations_with_water_level=49,
            observations_without_water_level=0,
            coverage_ratio=0.98,
            freshness_ratio=30 / 49,
            oldest_observed_at=datetime(
                2020,
                8,
                30,
                9,
                0,
                tzinfo=UTC,
            ),
            latest_observed_at=datetime(
                2026,
                9,
                28,
                20,
                20,
                tzinfo=UTC,
            ),
        ),
        BasinCurrentSummary(
            basin_name="Unknown",
            total_stations=7,
            stations_with_observation=7,
            stations_without_observation=0,
            fresh_observations=4,
            stale_observations=3,
            future_observations=0,
            unassessable_observations=0,
            observations_with_water_level=7,
            observations_without_water_level=0,
            coverage_ratio=1.0,
            freshness_ratio=4 / 7,
            oldest_observed_at=datetime(
                2024,
                1,
                1,
                tzinfo=UTC,
            ),
            latest_observed_at=datetime(
                2026,
                9,
                28,
                20,
                20,
                tzinfo=UTC,
            ),
        ),
    )

    app = _create_test_app(
        monkeypatch=monkeypatch,
        analytics=analytics,
    )

    with TestClient(
        app
    ) as client:
        response = client.get(
            f"{API_PREFIX}/basins"
        )

    assert response.status_code == 200

    body = response.json()

    assert len(body) == 2

    assert body[0][
        "basin_name"
    ] == "Koshi"

    assert body[0][
        "total_stations"
    ] == 50

    assert body[0][
        "fresh_observations"
    ] == 30

    assert body[0][
        "latest_observed_at"
    ] == "2026-09-28T20:20:00Z"

    assert body[1][
        "basin_name"
    ] == "Unknown"

    (
        analytics
        .get_basin_summaries
        .assert_called_once_with()
    )

#Test empty basin collection
def test_list_basin_summaries_can_be_empty(
    monkeypatch,
) -> None:
    analytics = MagicMock(
        spec=AnalyticsService
    )

    analytics.get_basin_summaries.return_value = ()

    app = _create_test_app(
        monkeypatch=monkeypatch,
        analytics=analytics,
    )

    with TestClient(
        app
    ) as client:
        response = client.get(
            f"{API_PREFIX}/basins"
        )

    assert response.status_code == 200

    assert response.json() == []

#Test network summary
def test_get_network_summary(
    monkeypatch,
) -> None:
    analytics = MagicMock(
        spec=AnalyticsService
    )

    analytics.get_network_summary.return_value = (
        CurrentNetworkSummary(
            basins_represented=25,
            total_stations=284,
            stations_with_observation=279,
            stations_without_observation=5,
            fresh_observations=188,
            stale_observations=91,
            future_observations=0,
            unassessable_observations=0,
            observations_with_water_level=275,
            observations_without_water_level=4,
            oldest_observed_at=datetime(
                2020,
                7,
                21,
                1,
                55,
                tzinfo=UTC,
            ),
            latest_observed_at=datetime(
                2026,
                9,
                28,
                21,
                45,
                tzinfo=UTC,
            ),
            coverage_ratio=279 / 284,
            freshness_ratio=188 / 279,
        )
    )

    app = _create_test_app(
        monkeypatch=monkeypatch,
        analytics=analytics,
    )

    with TestClient(
        app
    ) as client:
        response = client.get(
            
                f"{API_PREFIX}"
                "/network/summary"
            
        )

    assert response.status_code == 200

    body = response.json()

    assert body[
        "basins_represented"
    ] == 25

    assert body[
        "total_stations"
    ] == 284

    assert body[
        "stations_with_observation"
    ] == 279

    assert body[
        "stations_without_observation"
    ] == 5

    assert body[
        "fresh_observations"
    ] == 188

    assert body[
        "stale_observations"
    ] == 91

    assert body[
        "coverage_ratio"
    ] == 279 / 284

    assert body[
        "latest_observed_at"
    ] == "2026-09-28T21:45:00Z"

    (
        analytics
        .get_network_summary
        .assert_called_once_with()
    )

#OpenAPI test
def test_summary_routes_are_in_openapi() -> None:
    app = create_app()

    paths = app.openapi()[
        "paths"
    ]

    assert (
        f"{API_PREFIX}/basins"
        in paths
    )

    assert (
        f"{API_PREFIX}"
        "/network/summary"
        in paths
    )