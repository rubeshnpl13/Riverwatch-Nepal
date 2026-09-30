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

from riverwatch.analytics.errors import (
    InvalidAnalyticsQueryError,
)
from riverwatch.analytics.model import (
    CurrentRiverStation,
    StationObservation,
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


def _current_station() -> CurrentRiverStation:
    return CurrentRiverStation(
        station_id="44",
        station_name="Station A",
        basin_name="Koshi",
        latitude=27.7,
        longitude=85.3,
        elevation_m=500.0,
        source_record_id="current-44",
        observed_at=datetime(
            2026,
            9,
            30,
            9,
            30,
            tzinfo=UTC,
        ),
        water_level_m=1.5,
        observation_ingested_at=datetime(
            2026,
            9,
            30,
            10,
            0,
            tzinfo=UTC,
        ),
        has_observation=True,
        observation_age_hours=0.5,
    )


def _history() -> tuple[
    StationObservation,
    ...,
]:
    return (
        StationObservation(
            source_record_id="history-1",
            station_id="44",
            observed_at=datetime(
                2026,
                9,
                29,
                8,
                0,
                tzinfo=UTC,
            ),
            water_level_m=1.2,
            endpoint="river",
            run_id="history",
            ingested_at=datetime(
                2026,
                9,
                30,
                8,
                0,
                tzinfo=UTC,
            ),
        ),
        StationObservation(
            source_record_id="current-44",
            station_id="44",
            observed_at=datetime(
                2026,
                9,
                30,
                9,
                30,
                tzinfo=UTC,
            ),
            water_level_m=1.5,
            endpoint="river-stations",
            run_id="current",
            ingested_at=datetime(
                2026,
                9,
                30,
                10,
                0,
                tzinfo=UTC,
            ),
        ),
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

#test history without filters
def test_get_station_history(
    monkeypatch,
) -> None:
    analytics = MagicMock(
        spec=AnalyticsService
    )

    analytics.get_station.return_value = (
        _current_station()
    )

    analytics.get_station_history.return_value = (
        _history()
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
                "/stations/44/history"
            
        )

    assert response.status_code == 200

    body = response.json()

    assert len(body) == 2

    assert body[0][
        "source_record_id"
    ] == "history-1"

    assert body[0][
        "observed_at"
    ] == "2026-09-29T08:00:00Z"

    assert body[1][
        "source_record_id"
    ] == "current-44"

    analytics.get_station.assert_called_once_with(
        "44"
    )

    (
        analytics
        .get_station_history
        .assert_called_once_with(
            "44",
            start_at=None,
            end_at=None,
        )
    )

# Test time filters

def test_get_station_history_with_time_window(
    monkeypatch,
) -> None:
    analytics = MagicMock(
        spec=AnalyticsService
    )

    analytics.get_station.return_value = (
        _current_station()
    )

    analytics.get_station_history.return_value = (
        _history()[:1]
    )

    app = _create_test_app(
        monkeypatch=monkeypatch,
        analytics=analytics,
    )

    with TestClient(
        app
    ) as client:
        response = client.get(
            (
                f"{API_PREFIX}"
                "/stations/44/history"
            ),
            params={
                "start_at": (
                    "2026-09-29T00:00:00Z"
                ),
                "end_at": (
                    "2026-09-30T00:00:00Z"
                ),
            },
        )

    assert response.status_code == 200

    assert len(
        response.json()
    ) == 1

    (
        analytics
        .get_station_history
        .assert_called_once_with(
            "44",
            start_at=datetime(
                2026,
                9,
                29,
                0,
                0,
                tzinfo=UTC,
            ),
            end_at=datetime(
                2026,
                9,
                30,
                0,
                0,
                tzinfo=UTC,
            ),
        )
    )

#test Known station with no history

def test_known_station_without_history_returns_empty_list(
    monkeypatch,
) -> None:
    analytics = MagicMock(
        spec=AnalyticsService
    )

    analytics.get_station.return_value = (
        _current_station()
    )

    analytics.get_station_history.return_value = ()

    app = _create_test_app(
        monkeypatch=monkeypatch,
        analytics=analytics,
    )

    with TestClient(
        app
    ) as client:
        response = client.get(
            
                f"{API_PREFIX}"
                "/stations/44/history"
            
        )

    assert response.status_code == 200

    assert response.json() == []

#test Unknown station returns 404

def test_unknown_station_history_returns_404(
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
            
                f"{API_PREFIX}"
                "/stations/999/history"
            
        )

    assert response.status_code == 404

    assert response.json() == {
        "detail": "Station not found",
    }

    (
        analytics
        .get_station_history
        .assert_not_called()
    )

#test Invalid time window becomes 422
def test_invalid_history_time_window_returns_422(
    monkeypatch,
) -> None:
    analytics = MagicMock(
        spec=AnalyticsService
    )

    analytics.get_station.return_value = (
        _current_station()
    )

    analytics.get_station_history.side_effect = (
        InvalidAnalyticsQueryError(
            "start_at must be earlier "
            "than end_at"
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
            (
                f"{API_PREFIX}"
                "/stations/44/history"
            ),
            params={
                "start_at": (
                    "2026-09-30T00:00:00Z"
                ),
                "end_at": (
                    "2026-09-29T00:00:00Z"
                ),
            },
        )

    assert response.status_code == 422

    assert response.json() == {
        "detail": (
            "start_at must be earlier "
            "than end_at"
        )
    }

#test Reject timezone-naive query parameters

def test_history_rejects_naive_datetime(
    monkeypatch,
) -> None:
    analytics = MagicMock(
        spec=AnalyticsService
    )

    analytics.get_station.return_value = (
        _current_station()
    )

    app = _create_test_app(
        monkeypatch=monkeypatch,
        analytics=analytics,
    )

    with TestClient(
        app
    ) as client:
        response = client.get(
            (
                f"{API_PREFIX}"
                "/stations/44/history"
            ),
            params={
                "start_at": (
                    "2026-09-29T00:00:00"
                ),
            },
        )

    assert response.status_code == 422

    (
        analytics
        .get_station_history
        .assert_not_called()
    )

#OpenAPI test
def test_history_route_is_in_openapi() -> None:
    app = create_app()

    paths = app.openapi()[
        "paths"
    ]

    assert (
        f"{API_PREFIX}/stations/"
        "{station_id}/history"
        in paths
    )