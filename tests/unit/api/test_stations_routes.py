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
    CurrentRiverStation,
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


def _station(
    *,
    station_id: str,
    station_name: str,
    has_observation: bool,
) -> CurrentRiverStation:
    if has_observation:
        return CurrentRiverStation(
            station_id=station_id,
            station_name=station_name,
            basin_name="Koshi",
            latitude=27.7,
            longitude=85.3,
            elevation_m=500.0,
            source_record_id=(
                f"current-{station_id}"
            ),
            observed_at=datetime(
                2026,
                9,
                30,
                9,
                30,
                tzinfo=UTC,
            ),
            water_level_m=1.5,
            observation_ingested_at=(
                datetime(
                    2026,
                    9,
                    30,
                    10,
                    0,
                    tzinfo=UTC,
                )
            ),
            has_observation=True,
            observation_age_hours=0.5,
        )

    return CurrentRiverStation(
        station_id=station_id,
        station_name=station_name,
        basin_name="Karnali",
        latitude=28.0,
        longitude=84.0,
        elevation_m=600.0,
        source_record_id=None,
        observed_at=None,
        water_level_m=None,
        observation_ingested_at=None,
        has_observation=False,
        observation_age_hours=None,
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


def test_list_current_stations(
    monkeypatch,
) -> None:
    analytics = MagicMock(
        spec=AnalyticsService
    )

    analytics.get_current_snapshot.return_value = (
        _station(
            station_id="44",
            station_name="Station A",
            has_observation=True,
        ),
        _station(
            station_id="46",
            station_name="Station C",
            has_observation=False,
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
            f"{API_PREFIX}/stations"
        )

    assert response.status_code == 200

    body = response.json()

    assert len(body) == 2

    assert body[0][
        "station_id"
    ] == "44"

    assert body[0][
        "station_name"
    ] == "Station A"

    assert body[0][
        "has_observation"
    ] is True

    assert body[0][
        "observed_at"
    ] == "2026-09-30T09:30:00Z"

    assert body[1][
        "station_id"
    ] == "46"

    assert body[1][
        "has_observation"
    ] is False

    assert body[1][
        "observed_at"
    ] is None

    assert body[1][
        "water_level_m"
    ] is None

    (
        analytics
        .get_current_snapshot
        .assert_called_once_with()
    )


def test_get_current_station(
    monkeypatch,
) -> None:
    analytics = MagicMock(
        spec=AnalyticsService
    )

    analytics.get_station.return_value = (
        _station(
            station_id="44",
            station_name="Station A",
            has_observation=True,
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
            f"{API_PREFIX}/stations/44"
        )

    assert response.status_code == 200

    assert response.json()[
        "station_id"
    ] == "44"

    assert response.json()[
        "water_level_m"
    ] == 1.5

    analytics.get_station.assert_called_once_with(
        "44"
    )


def test_get_current_station_returns_404(
    monkeypatch,
) -> None:
    analytics = MagicMock(
        spec=AnalyticsService
    )

    analytics.get_station.return_value = (
        None
    )

    app = _create_test_app(
        monkeypatch=monkeypatch,
        analytics=analytics,
    )

    with TestClient(
        app
    ) as client:
        response = client.get(
            f"{API_PREFIX}/stations/999"
        )

    assert response.status_code == 404

    assert response.json() == {
        "detail": "Station not found",
    }

    analytics.get_station.assert_called_once_with(
        "999"
    )


def test_station_routes_are_in_openapi() -> None:
    app = create_app()

    paths = app.openapi()[
        "paths"
    ]

    assert (
        f"{API_PREFIX}/stations"
        in paths
    )

    assert (
        f"{API_PREFIX}/stations/"
        "{station_id}"
        in paths
    )