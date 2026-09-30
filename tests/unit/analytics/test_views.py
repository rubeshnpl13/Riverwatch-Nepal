from pathlib import Path

import duckdb

from riverwatch.analytics.catalog import (
    create_analytics_connection,
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
    run_id: str,
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
        / f"run-{run_id}"
        / "part-000.parquet"
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
        / "part-000.parquet"
    )


def test_current_views_use_latest_run(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_parquet(
        path=_station_path(
            lake_root=lake_root,
            run_id="old-run",
        ),
        query="""
            SELECT
                '44' AS station_id,
                'Old Station Name'
                    AS station_name,
                'Koshi'
                    AS basin_name,
                'old-run' AS run_id,
                TIMESTAMP
                    '2026-09-30 08:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_station_path(
            lake_root=lake_root,
            run_id="new-run",
        ),
        query="""
            SELECT
                '44' AS station_id,
                'New Station Name'
                    AS station_name,
                'Koshi'
                    AS basin_name,
                'new-run' AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river-stations",
            run_id="old-run",
        ),
        query="""
            SELECT
                '100' AS source_record_id,
                '44' AS station_id,
                TIMESTAMP
                    '2026-09-30 07:45:00'
                    AS observed_at,
                1.10::DOUBLE
                    AS water_level_m,
                'old-run' AS run_id,
                TIMESTAMP
                    '2026-09-30 08:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river-stations",
            run_id="new-run",
        ),
        query="""
            SELECT
                '101' AS source_record_id,
                '44' AS station_id,
                TIMESTAMP
                    '2026-09-30 09:45:00'
                    AS observed_at,
                1.25::DOUBLE
                    AS water_level_m,
                'new-run' AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    try:
        latest_run = (
            connection.execute(
                """
                SELECT run_id
                FROM latest_current_run
                """
            ).fetchone()
        )

        assert latest_run == (
            "new-run",
        )

        stations = (
            connection.execute(
                """
                SELECT
                    station_id,
                    station_name,
                    run_id
                FROM current_stations
                """
            ).fetchall()
        )

        assert stations == [
            (
                "44",
                "New Station Name",
                "new-run",
            )
        ]

        observations = (
            connection.execute(
                """
                SELECT
                    source_record_id,
                    station_id,
                    run_id
                FROM current_observations
                """
            ).fetchall()
        )

        assert observations == [
            (
                "101",
                "44",
                "new-run",
            )
        ]

    finally:
        connection.close()

def test_historical_view_deduplicates_across_runs(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_parquet(
        path=_station_path(
            lake_root=lake_root,
            run_id="current-run",
        ),
        query="""
            SELECT
                '44' AS station_id,
                'Station A'
                    AS station_name,
                'Koshi'
                    AS basin_name,
                'current-run' AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river-stations",
            run_id="current-run",
        ),
        query="""
            SELECT
                'current-100'
                    AS source_record_id,
                '44' AS station_id,
                TIMESTAMP
                    '2026-09-30 09:45:00'
                    AS observed_at,
                1.50::DOUBLE
                    AS water_level_m,
                'current-run' AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river",
            run_id="history-old",
        ),
        query="""
            SELECT
                'historical-1'
                    AS source_record_id,
                '44' AS station_id,
                TIMESTAMP
                    '2026-09-28 09:00:00'
                    AS observed_at,
                1.00::DOUBLE
                    AS water_level_m,
                'history-old' AS run_id,
                TIMESTAMP
                    '2026-09-29 08:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river",
            run_id="history-new",
        ),
        query="""
            SELECT
                'historical-1'
                    AS source_record_id,
                '44' AS station_id,
                TIMESTAMP
                    '2026-09-28 09:00:00'
                    AS observed_at,
                1.05::DOUBLE
                    AS water_level_m,
                'history-new' AS run_id,
                TIMESTAMP
                    '2026-09-30 08:00:00'
                    AS ingested_at
        """,
    )

    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    try:
        rows = (
            connection.execute(
                """
                SELECT
                    source_record_id,
                    water_level_m,
                    run_id
                FROM historical_observations
                ORDER BY source_record_id
                """
            ).fetchall()
        )

        assert rows == [
            (
                "historical-1",
                1.05,
                "history-new",
            )
        ]

    finally:
        connection.close()

#test latest observation per station

def test_latest_observation_per_station(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_parquet(
        path=_station_path(
            lake_root=lake_root,
            run_id="current-run",
        ),
        query="""
            SELECT
                '44' AS station_id,
                'Station A'
                    AS station_name,
                'Koshi'
                    AS basin_name,
                'current-run' AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river-stations",
            run_id="current-run",
        ),
        query="""
            SELECT
                'current-1'
                    AS source_record_id,
                '44' AS station_id,
                TIMESTAMP
                    '2026-09-30 09:45:00'
                    AS observed_at,
                2.00::DOUBLE
                    AS water_level_m,
                'current-run' AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river",
            run_id="history-run",
        ),
        query="""
            SELECT
                'history-1'
                    AS source_record_id,
                '44' AS station_id,
                TIMESTAMP
                    '2026-09-29 09:00:00'
                    AS observed_at,
                1.50::DOUBLE
                    AS water_level_m,
                'history-run' AS run_id,
                TIMESTAMP
                    '2026-09-30 08:00:00'
                    AS ingested_at
        """,
    )

    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    try:
        rows = (
            connection.execute(
                """
                SELECT
                    station_id,
                    source_record_id,
                    water_level_m
                FROM
                    latest_observation_per_station
                """
            ).fetchall()
        )

        assert rows == [
            (
                "44",
                "current-1",
                2.0,
            )
        ]

    finally:
        connection.close()

#test for the key invariant
def test_latest_observation_view_has_unique_station_ids(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_parquet(
        path=_station_path(
            lake_root=lake_root,
            run_id="current-run",
        ),
        query="""
            SELECT *
            FROM (
                VALUES
                    (
                        '44',
                        'Station A',
                        'Koshi',
                        'current-run',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        '45',
                        'Station B',
                        'Gandaki',
                        'current-run',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    )
            )
            AS t(
                station_id,
                station_name,
                basin_name,
                run_id,
                ingested_at
            )
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river-stations",
            run_id="current-run",
        ),
        query="""
            SELECT *
            FROM (
                VALUES
                    (
                        '100',
                        '44',
                        TIMESTAMP
                            '2026-09-30 09:30:00',
                        1.0::DOUBLE,
                        'current-run',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        '101',
                        '45',
                        TIMESTAMP
                            '2026-09-30 09:35:00',
                        2.0::DOUBLE,
                        'current-run',
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

    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    try:
        result = (
            connection.execute(
                """
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(
                        DISTINCT station_id
                    ) AS unique_stations
                FROM
                    latest_observation_per_station
                """
            ).fetchone()
        )

        assert result == (
            2,
            2,
        )

    finally:
        connection.close()

#test current and historical data combine

def test_station_history_combines_current_and_historical(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_parquet(
        path=_station_path(
            lake_root=lake_root,
            run_id="current-run",
        ),
        query="""
            SELECT
                '44' AS station_id,
                'Station A'
                    AS station_name,
                'Koshi'
                    AS basin_name,
                'current-run'
                    AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river",
            run_id="history-run",
        ),
        query="""
            SELECT *
            FROM (
                VALUES
                    (
                        'history-1',
                        '44',
                        TIMESTAMP
                            '2026-09-28 08:00:00',
                        1.00::DOUBLE,
                        'history-run',
                        TIMESTAMP
                            '2026-09-30 08:00:00'
                    ),
                    (
                        'history-2',
                        '44',
                        TIMESTAMP
                            '2026-09-29 08:00:00',
                        1.25::DOUBLE,
                        'history-run',
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

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river-stations",
            run_id="current-run",
        ),
        query="""
            SELECT
                'current-1'
                    AS source_record_id,
                '44'
                    AS station_id,
                TIMESTAMP
                    '2026-09-30 09:30:00'
                    AS observed_at,
                1.50::DOUBLE
                    AS water_level_m,
                'current-run'
                    AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    try:
        rows = (
            connection.execute(
                """
                SELECT
                    source_record_id,
                    observed_at,
                    water_level_m
                FROM station_observation_history
                WHERE station_id = '44'
                ORDER BY observed_at
                """
            ).fetchall()
        )

        assert len(rows) == 3

        assert [
            row[0]
            for row in rows
        ] == [
            "history-1",
            "history-2",
            "current-1",
        ]

    finally:
        connection.close()

#Test cross-endpoint duplicate timestamps

def test_station_history_deduplicates_same_station_timestamp(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_parquet(
        path=_station_path(
            lake_root=lake_root,
            run_id="current-run",
        ),
        query="""
            SELECT
                '44' AS station_id,
                'Station A'
                    AS station_name,
                'Koshi'
                    AS basin_name,
                'current-run'
                    AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river",
            run_id="history-run",
        ),
        query="""
            SELECT
                'history-copy'
                    AS source_record_id,
                '44'
                    AS station_id,
                TIMESTAMP
                    '2026-09-30 09:00:00'
                    AS observed_at,
                1.00::DOUBLE
                    AS water_level_m,
                'history-run'
                    AS run_id,
                TIMESTAMP
                    '2026-09-30 08:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river-stations",
            run_id="current-run",
        ),
        query="""
            SELECT
                'current-copy'
                    AS source_record_id,
                '44'
                    AS station_id,
                TIMESTAMP
                    '2026-09-30 09:00:00'
                    AS observed_at,
                1.10::DOUBLE
                    AS water_level_m,
                'current-run'
                    AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    try:
        rows = (
            connection.execute(
                """
                SELECT
                    source_record_id,
                    water_level_m
                FROM station_observation_history
                WHERE station_id = '44'
                """
            ).fetchall()
        )

        assert rows == [
            (
                "current-copy",
                1.1,
            )
        ]

    finally:
        connection.close()

#Test chronological uniqueness

def test_station_history_has_unique_timestamps_per_station(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_parquet(
        path=_station_path(
            lake_root=lake_root,
            run_id="current-run",
        ),
        query="""
            SELECT
                '44' AS station_id,
                'Station A'
                    AS station_name,
                'Koshi'
                    AS basin_name,
                'current-run'
                    AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river",
            run_id="history-run",
        ),
        query="""
            SELECT *
            FROM (
                VALUES
                    (
                        '1',
                        '44',
                        TIMESTAMP
                            '2026-09-28 08:00:00',
                        1.00::DOUBLE,
                        'history-run',
                        TIMESTAMP
                            '2026-09-30 08:00:00'
                    ),
                    (
                        '2',
                        '44',
                        TIMESTAMP
                            '2026-09-29 08:00:00',
                        1.10::DOUBLE,
                        'history-run',
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

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river-stations",
            run_id="current-run",
        ),
        query="""
            SELECT
                '3'
                    AS source_record_id,
                '44'
                    AS station_id,
                TIMESTAMP
                    '2026-09-30 09:00:00'
                    AS observed_at,
                1.20::DOUBLE
                    AS water_level_m,
                'current-run'
                    AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    try:
        result = (
            connection.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(
                        DISTINCT observed_at
                    )
                FROM station_observation_history
                WHERE station_id = '44'
                """
            ).fetchone()
        )

        assert result == (
            3,
            3,
        )

    finally:
        connection.close()

#Test parameterized time-window queries

def test_station_history_supports_time_window_query(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_parquet(
        path=_station_path(
            lake_root=lake_root,
            run_id="current-run",
        ),
        query="""
            SELECT
                '44' AS station_id,
                'Station A'
                    AS station_name,
                'Koshi'
                    AS basin_name,
                'current-run'
                    AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river",
            run_id="history-run",
        ),
        query="""
            SELECT *
            FROM (
                VALUES
                    (
                        '1',
                        '44',
                        TIMESTAMP
                            '2026-09-28 08:00:00',
                        1.00::DOUBLE,
                        'history-run',
                        TIMESTAMP
                            '2026-09-30 08:00:00'
                    ),
                    (
                        '2',
                        '44',
                        TIMESTAMP
                            '2026-09-29 08:00:00',
                        1.10::DOUBLE,
                        'history-run',
                        TIMESTAMP
                            '2026-09-30 08:00:00'
                    ),
                    (
                        '3',
                        '44',
                        TIMESTAMP
                            '2026-09-30 08:00:00',
                        1.20::DOUBLE,
                        'history-run',
                        TIMESTAMP
                            '2026-09-30 08:30:00'
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

    # A current observation dataset must
    # exist for analytics registration.
    _write_parquet(
        path=_observation_path(
            lake_root=lake_root,
            endpoint="river-stations",
            run_id="current-run",
        ),
        query="""
            SELECT
                'current'
                    AS source_record_id,
                '44'
                    AS station_id,
                TIMESTAMP
                    '2026-09-30 09:00:00'
                    AS observed_at,
                1.30::DOUBLE
                    AS water_level_m,
                'current-run'
                    AS run_id,
                TIMESTAMP
                    '2026-09-30 10:00:00'
                    AS ingested_at
        """,
    )

    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    try:
        rows = (
            connection.execute(
                """
                SELECT
                    source_record_id,
                    observed_at
                FROM station_observation_history
                WHERE
                    station_id = ?
                    AND observed_at >= ?
                    AND observed_at < ?
                ORDER BY observed_at
                """,
                [
                    "44",
                    "2026-09-29 00:00:00",
                    "2026-09-30 00:00:00",
                ],
            ).fetchall()
        )

        assert len(rows) == 1

        assert (
            rows[0][0]
            == "2"
        )

    finally:
        connection.close()
