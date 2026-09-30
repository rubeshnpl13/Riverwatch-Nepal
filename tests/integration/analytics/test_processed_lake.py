from datetime import UTC
from pathlib import Path

import duckdb

from riverwatch.analytics.catalog import (
    create_analytics_connection,
)
from riverwatch.analytics.service import (
    AnalyticsService,
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


def test_processed_lake_supports_typed_analytics(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    station_path = (
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

    current_path = (
        lake_root
        / "processed"
        / "dataset=observations"
        / "endpoint=river-stations"
        / "year=2026"
        / "month=09"
        / "day=30"
        / "hour=10"
        / "run-current"
        / "part.parquet"
    )

    historical_path = (
        lake_root
        / "processed"
        / "dataset=observations"
        / "endpoint=river"
        / "year=2026"
        / "month=09"
        / "day=30"
        / "hour=10"
        / "run-history"
        / "part.parquet"
    )

    _write_parquet(
        path=station_path,
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
        path=current_path,
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
        path=historical_path,
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

    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    try:
        analytics = AnalyticsService(
            connection=connection
        )

        #
        # Current snapshot
        #

        snapshot = (
            analytics
            .get_current_snapshot()
        )

        assert len(snapshot) == 3

        assert {
            item.station_id
            for item in snapshot
        } == {
            "44",
            "45",
            "46",
        }

        station_46 = (
            analytics.get_station(
                "46"
            )
        )

        assert station_46 is not None
        assert not station_46.has_observation

        #
        # History
        #

        history = (
            analytics
            .get_station_history(
                "44"
            )
        )

        assert [
            item.source_record_id
            for item in history
        ] == [
            "history-44-1",
            "history-44-2",
            "current-44",
        ]

        assert all(
            item.observed_at.tzinfo
            is UTC
            for item in history
        )

        #
        # Network analytics
        #

        network = (
            analytics
            .get_network_summary()
        )

        assert network.total_stations == 3

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

        assert (
            network.fresh_observations
            == 1
        )

        assert (
            network.stale_observations
            == 1
        )

        #
        # Basin analytics
        #

        basins = (
            analytics
            .get_basin_summaries()
        )

        assert {
            basin.basin_name
            for basin in basins
        } == {
            "Koshi",
            "Karnali",
        }

        koshi = next(
            basin
            for basin in basins
            if basin.basin_name
            == "Koshi"
        )

        assert koshi.total_stations == 2

        assert (
            koshi
            .stations_with_observation
            == 2
        )

    finally:
        connection.close()