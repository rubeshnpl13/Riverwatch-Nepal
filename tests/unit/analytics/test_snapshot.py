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


def test_snapshot_preserves_stations_without_observation(
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
            SELECT
                '100'
                    AS source_record_id,
                '44'
                    AS station_id,
                TIMESTAMP
                    '2026-09-30 09:30:00'
                    AS observed_at,
                1.25::DOUBLE
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
                    station_id,
                    station_name,
                    water_level_m,
                    has_observation
                FROM current_river_snapshot
                ORDER BY station_id
                """
            ).fetchall()
        )

        assert rows == [
            (
                "44",
                "Station A",
                1.25,
                True,
            ),
            (
                "45",
                "Station B",
                None,
                False,
            ),
        ]

    finally:
        connection.close()


def test_snapshot_uses_latest_current_observation(
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
                '44'
                    AS station_id,
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
                            '2026-09-30 08:00:00',
                        1.00::DOUBLE,
                        'current-run',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        '101',
                        '44',
                        TIMESTAMP
                            '2026-09-30 09:30:00',
                        1.50::DOUBLE,
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
        row = (
            connection.execute(
                """
                SELECT
                    station_id,
                    source_record_id,
                    water_level_m
                FROM current_river_snapshot
                """
            ).fetchone()
        )

        assert row == (
            "44",
            "101",
            1.5,
        )

    finally:
        connection.close()


def test_snapshot_has_one_row_per_station_and_age(
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
                            '2026-09-30 08:30:00',
                        1.00::DOUBLE,
                        'current-run',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        '101',
                        '45',
                        TIMESTAMP
                            '2026-09-30 09:30:00',
                        2.00::DOUBLE,
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
        counts = (
            connection.execute(
                """
                SELECT
                    COUNT(*) AS total_rows,

                    COUNT(
                        DISTINCT station_id
                    ) AS unique_stations,

                    COUNT(*) FILTER (
                        WHERE has_observation
                    ) AS with_observation,

                    COUNT(*) FILTER (
                        WHERE NOT has_observation
                    ) AS without_observation

                FROM current_river_snapshot
                """
            ).fetchone()
        )

        assert counts == (
            2,
            2,
            2,
            0,
        )

        ages = (
            connection.execute(
                """
                SELECT
                    station_id,
                    observation_age_hours
                FROM current_river_snapshot
                ORDER BY station_id
                """
            ).fetchall()
        )

        assert ages == [
            (
                "44",
                1.5,
            ),
            (
                "45",
                0.5,
            ),
        ]

    finally:
        connection.close()
