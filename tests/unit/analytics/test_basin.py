from pathlib import Path

import duckdb
import pytest

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
        / "run-current-run"
        / "part-000.parquet"
    )


def _observation_path(
    *,
    lake_root: Path,
) -> Path:
    return (
        lake_root
        / "processed"
        / "dataset=observations"
        / "endpoint=river-stations"
        / "year=2026"
        / "month=09"
        / "day=30"
        / "hour=10"
        / "run-current-run"
        / "part-000.parquet"
    )


def _write_test_data(
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
                        'current-run',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        '45',
                        'Station B',
                        'Koshi',
                        'current-run',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        '46',
                        'Station C',
                        'Karnali',
                        'current-run',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        '47',
                        'Station D',
                        '',
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
            lake_root=lake_root
        ),
        query="""
            SELECT *
            FROM (
                VALUES
                    (
                        '100',
                        '44',
                        TIMESTAMP
                            '2026-09-30 09:00:00',
                        1.00::DOUBLE,
                        'current-run',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        '101',
                        '45',
                        TIMESTAMP
                            '2026-09-28 10:00:00',
                        NULL::DOUBLE,
                        'current-run',
                        TIMESTAMP
                            '2026-09-30 10:00:00'
                    ),
                    (
                        '102',
                        '46',
                        TIMESTAMP
                            '2026-09-30 11:00:00',
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

#basin classification
def test_basin_summary_counts_health(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_test_data(
        lake_root=lake_root
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
                    basin_name,
                    total_stations,
                    stations_with_observation,
                    stations_without_observation,
                    fresh_observations,
                    stale_observations,
                    future_observations,
                    observations_with_water_level,
                    observations_without_water_level,
                    coverage_ratio,
                    freshness_ratio
                FROM basin_current_summary
                ORDER BY basin_name
                """
            ).fetchall()
        )

        assert rows[0][:9] == (
            "Karnali",
            1,
            1,
            0,
            0,
            0,
            1,
            1,
            0,
        )

        assert rows[1][:9] == (
            "Koshi",
            2,
            2,
            0,
            1,
            1,
            0,
            1,
            1,
        )

        assert rows[1][9] == pytest.approx(
            1.0
        )

        assert rows[1][10] == pytest.approx(
            0.5
        )

    finally:
        connection.close()

#test missing basin becomes unknown

def test_missing_basin_is_preserved_as_unknown(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_test_data(
        lake_root=lake_root
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
                    total_stations,
                    stations_with_observation,
                    stations_without_observation,
                    coverage_ratio
                FROM basin_current_summary
                WHERE basin_name = 'Unknown'
                """
            ).fetchone()
        )

        assert row is not None

        assert row[:3] == (
            1,
            0,
            1,
        )

        assert row[3] == pytest.approx(
            0.0
        )

    finally:
        connection.close()

#test network totals
def test_network_summary_matches_snapshot(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    _write_test_data(
        lake_root=lake_root
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
                    basins_represented,
                    total_stations,
                    stations_with_observation,
                    stations_without_observation,
                    fresh_observations,
                    stale_observations,
                    future_observations,
                    unassessable_observations,
                    observations_with_water_level,
                    observations_without_water_level,
                    coverage_ratio,
                    freshness_ratio
                FROM current_network_summary
                """
            ).fetchone()
        )

        assert row is not None

        assert row[:10] == (
            3,
            4,
            3,
            1,
            1,
            1,
            1,
            0,
            2,
            1,
        )

        assert row[10] == pytest.approx(
            0.75
        )

        assert row[11] == pytest.approx(
            1 / 3
        )

    finally:
        connection.close()