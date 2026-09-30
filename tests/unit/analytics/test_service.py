from datetime import (
    UTC,
    datetime,
)
from pathlib import Path

import duckdb
import pytest

from riverwatch.analytics.catalog import (
    create_analytics_connection,
)
from riverwatch.analytics.errors import (
    InvalidAnalyticsQueryError,
)
from riverwatch.analytics.model import (
    BasinCurrentSummary,
    CurrentNetworkSummary,
    CurrentRiverStation,
    StationObservation,
)
from riverwatch.analytics.service import (
    AnalyticsService,
)


#helpers
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


def _write_service_data(
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
                        1.5::DOUBLE,
                        'current',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        'current-45',
                        '45',
                        TIMESTAMP
                            '2026-09-28 09:30:00',
                        NULL::DOUBLE,
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
                        1.0::DOUBLE,
                        'history',
                        TIMESTAMP
                            '2026-09-30 08:00:00'
                    ),
                    (
                        'history-44-2',
                        '44',
                        TIMESTAMP
                            '2026-09-29 08:00:00',
                        1.2::DOUBLE,
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


def _create_service(
    *,
    lake_root: Path,
) -> tuple[
    AnalyticsService,
    duckdb.DuckDBPyConnection,
]:
    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    return (
        AnalyticsService(
            connection=connection
        ),
        connection,
    )

#Tested typed current snapshot

def test_get_current_snapshot_returns_models(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_service_data(
        lake_root=lake_root
    )

    (
        service,
        connection,
    ) = _create_service(
        lake_root=lake_root
    )

    try:
        snapshot = (
            service.get_current_snapshot()
        )

        assert len(snapshot) == 3

        assert all(
            isinstance(
                item,
                CurrentRiverStation,
            )
            for item in snapshot
        )

        station = next(
            item
            for item in snapshot
            if item.station_id == "44"
        )

        assert station.station_name == (
            "Station A"
        )

        assert station.basin_name == (
            "Koshi"
        )

        assert station.water_level_m == (
            1.5
        )

        assert station.has_observation

        assert (
            station.observed_at
            == datetime(
                2026,
                9,
                30,
                9,
                30,
                tzinfo=UTC,
            )
        )

        missing = next(
            item
            for item in snapshot
            if item.station_id == "46"
        )

        assert not (
            missing.has_observation
        )

        assert (
            missing.water_level_m
            is None
        )

    finally:
        connection.close()

#Tested single station lookup
def test_get_station(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_service_data(
        lake_root=lake_root
    )

    (
        service,
        connection,
    ) = _create_service(
        lake_root=lake_root
    )

    try:
        station = (
            service.get_station(
                "44"
            )
        )

        assert station is not None

        assert isinstance(
            station,
            CurrentRiverStation,
        )

        assert (
            station.station_name
            == "Station A"
        )

        assert (
            service.get_station(
                "999"
            )
            is None
        )

    finally:
        connection.close()

#Test station history + time window

def test_get_station_history_is_chronological_and_filterable(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_service_data(
        lake_root=lake_root
    )

    (
        service,
        connection,
    ) = _create_service(
        lake_root=lake_root
    )

    try:
        history = (
            service.get_station_history(
                "44"
            )
        )

        assert len(history) == 3

        assert all(
            isinstance(
                item,
                StationObservation,
            )
            for item in history
        )

        assert [
            item.source_record_id
            for item in history
        ] == [
            "history-44-1",
            "history-44-2",
            "current-44",
        ]

        filtered = (
            service.get_station_history(
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

        assert len(filtered) == 1

        assert (
            filtered[0]
            .source_record_id
            == "history-44-2"
        )

        assert (
            filtered[0]
            .observed_at
            .tzinfo
            is UTC
        )

    finally:
        connection.close()

#Test invalid queries

def test_invalid_station_history_query_fails(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_service_data(
        lake_root=lake_root
    )

    (
        service,
        connection,
    ) = _create_service(
        lake_root=lake_root
    )

    try:
        with pytest.raises(
            InvalidAnalyticsQueryError
        ):
            service.get_station_history(
                " ",
            )

        with pytest.raises(
            InvalidAnalyticsQueryError
        ):
            service.get_station_history(
                "44",
                start_at=datetime(
                    2026,
                    9,
                    30,
                    tzinfo=UTC,
                ),
                end_at=datetime(
                    2026,
                    9,
                    29,
                    tzinfo=UTC,
                ),
            )

    finally:
        connection.close()

#Test typed aggregate results

def test_get_basin_and_network_summaries(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_service_data(
        lake_root=lake_root
    )

    (
        service,
        connection,
    ) = _create_service(
        lake_root=lake_root
    )

    try:
        basins = (
            service.get_basin_summaries()
        )

        assert basins

        assert all(
            isinstance(
                item,
                BasinCurrentSummary,
            )
            for item in basins
        )

        koshi = next(
            item
            for item in basins
            if item.basin_name == "Koshi"
        )

        assert koshi.total_stations == 2

        assert (
            koshi
            .stations_with_observation
            == 2
        )

        assert (
            koshi.fresh_observations
            == 1
        )

        assert (
            koshi.stale_observations
            == 1
        )

        network = (
            service.get_network_summary()
        )

        assert isinstance(
            network,
            CurrentNetworkSummary,
        )

        assert (
            network.total_stations
            == 3
        )

        assert (
            network
            .stations_with_observation
            == 2
        )

        assert (
            network
            .stations_without_observation
            == 1
        )

    finally:
        connection.close()











