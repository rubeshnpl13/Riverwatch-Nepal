from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from riverwatch.analytics.errors import (
    AnalyticsDatasetNotFoundError,
)
from riverwatch.api.app import (
    API_PREFIX,
    create_app,
)


def _write_parquet(
    *,
    path: Path,
    query: str,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    escaped_path = str(
        path
    ).replace(
        "'",
        "''",
    )

    connection = duckdb.connect(
        database=":memory:"
    )

    try:
        connection.execute(
            f"""
            COPY (
                {query}
            )
            TO '{escaped_path}'
            (
                FORMAT PARQUET
            )
            """
        )
    finally:
        connection.close()


def _station_path(
    *,
    lake_root: Path,
) -> Path:
    return (
        lake_root
        / "processed"
        / "dataset=stations"
        / "endpoint=river-stations"
        / "year=2026"
        / "month=09"
        / "day=30"
        / "hour=10"
        / "run-current"
        / "part.parquet"
    )


def _observation_path(
    *,
    lake_root: Path,
    endpoint: str,
    run_id: str,
) -> Path:
    return (
        lake_root
        / "processed"
        / "dataset=observations"
        / f"endpoint={endpoint}"
        / "year=2026"
        / "month=09"
        / "day=30"
        / "hour=10"
        / f"run-{run_id}"
        / "part.parquet"
    )


def _write_test_lake(
    *,
    lake_root: Path,
) -> None:
    _write_parquet(
        path=_station_path(
            lake_root=lake_root
        ),
        query="""
            SELECT *
            FROM (
                VALUES
                    (
                        '44',
                        'Station A',
                        'Koshi',
                        27.7::DOUBLE,
                        85.3::DOUBLE,
                        500.0::DOUBLE,
                        'current',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        '45',
                        'Station B',
                        'Koshi',
                        27.8::DOUBLE,
                        85.4::DOUBLE,
                        600.0::DOUBLE,
                        'current',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        '46',
                        'Station C',
                        'Karnali',
                        28.0::DOUBLE,
                        84.0::DOUBLE,
                        700.0::DOUBLE,
                        'current',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    )
            )
            AS t(
                station_id,
                station_name,
                basin_name,
                latitude,
                longitude,
                elevation_m,
                run_id,
                ingested_at
            )
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river-stations",
            run_id="current",
        ),
        query="""
            SELECT *
            FROM (
                VALUES
                    (
                        'current-44',
                        '44',
                        TIMESTAMP
                            '2026-09-30 09:30:00',
                        1.50::DOUBLE,
                        'current',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        'current-45',
                        '45',
                        TIMESTAMP
                            '2026-09-28 09:30:00',
                        2.00::DOUBLE,
                        'current',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    )
            )
            AS t(
                source_record_id,
                station_id,
                observed_at,
                water_level_m,
                run_id,
                ingested_at
            )
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river",
            run_id="history",
        ),
        query="""
            SELECT *
            FROM (
                VALUES
                    (
                        'history-44-1',
                        '44',
                        TIMESTAMP
                            '2026-09-28 08:00:00',
                        1.00::DOUBLE,
                        'history',
                        TIMESTAMP
                            '2026-09-30 08:00:00'
                    ),
                    (
                        'history-44-2',
                        '44',
                        TIMESTAMP
                            '2026-09-29 08:00:00',
                        1.20::DOUBLE,
                        'history',
                        TIMESTAMP
                            '2026-09-30 08:00:00'
                    )
            )
            AS t(
                source_record_id,
                station_id,
                observed_at,
                water_level_m,
                run_id,
                ingested_at
            )
        """,
    )

#test complete api path
def test_api_serves_processed_lake(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_test_lake(
        lake_root=lake_root
    )

    app = create_app(
        lake_root=lake_root
    )

    with TestClient(
        app
    ) as client:
        #
        # Liveness
        #

        response = client.get(
            f"{API_PREFIX}/health/live"
        )

        assert response.status_code == 200

        #
        # Current stations
        #

        response = client.get(
            f"{API_PREFIX}/stations"
        )

        assert response.status_code == 200

        stations = response.json()

        assert len(stations) == 3

        assert {
            item["station_id"]
            for item in stations
        } == {
            "44",
            "45",
            "46",
        }

        #
        # Single station
        #

        response = client.get(
            f"{API_PREFIX}/stations/44"
        )

        assert response.status_code == 200

        station = response.json()

        assert station[
            "station_name"
        ] == "Station A"

        assert station[
            "water_level_m"
        ] == 1.5

        assert station[
            "observed_at"
        ] == "2026-09-30T09:30:00Z"

        #
        # Station with no observation
        #

        response = client.get(
            f"{API_PREFIX}/stations/46"
        )

        assert response.status_code == 200

        station = response.json()

        assert (
            station["has_observation"]
            is False
        )

        assert (
            station["observed_at"]
            is None
        )

        #
        # Historical time series
        #

        response = client.get(
            
                f"{API_PREFIX}"
                "/stations/44/history"
            
        )

        assert response.status_code == 200

        history = response.json()

        assert [
            item["source_record_id"]
            for item in history
        ] == [
            "history-44-1",
            "history-44-2",
            "current-44",
        ]

        #
        # Basin summaries
        #

        response = client.get(
            f"{API_PREFIX}/basins"
        )

        assert response.status_code == 200

        basins = response.json()

        assert {
            item["basin_name"]
            for item in basins
        } == {
            "Koshi",
            "Karnali",
        }

        #
        # Network summary
        #

        response = client.get(
            
                f"{API_PREFIX}"
                "/network/summary"
            
        )

        assert response.status_code == 200

        network = response.json()

        assert (
            network["total_stations"]
            == 3
        )

        assert (
            network[
                "stations_with_observation"
            ]
            == 2
        )

        assert (
            network[
                "stations_without_observation"
            ]
            == 1
        )

        assert (
            network[
                "fresh_observations"
            ]
            == 1
        )

        assert (
            network[
                "stale_observations"
            ]
            == 1
        )

    assert (
        app.state.analytics_service
        is None
    )

#test integration test history filtering

def test_api_history_window_and_error_contracts(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_test_lake(
        lake_root=lake_root
    )

    app = create_app(
        lake_root=lake_root
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

        history = response.json()

        assert len(history) == 1

        assert (
            history[0][
                "source_record_id"
            ]
            == "history-44-2"
        )

        #
        # Unknown station
        #

        response = client.get(
            
                f"{API_PREFIX}"
                "/stations/999"
            
        )

        assert response.status_code == 404

        assert response.json() == {
            "detail": (
                "Station not found"
            )
        }

        #
        # Invalid history interval
        #

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

        #
        # Naive datetime
        #

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

#test Verify fail-fast startup
def test_api_startup_fails_without_processed_lake(
    tmp_path: Path,
) -> None:
    app = create_app(
        lake_root=(
            tmp_path
            / "missing-lake"
        )
    )

    with pytest.raises(
        AnalyticsDatasetNotFoundError
    ):
        with TestClient(
            app
        ):
            pass
