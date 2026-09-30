from pathlib import Path

import duckdb
import pytest

from riverwatch.analytics.catalog import (
    create_analytics_connection,
)
from riverwatch.analytics.errors import (
    AnalyticsDatasetNotFoundError,
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


def test_registers_processed_views(
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
        / "day=29"
        / "hour=21"
        / "run-test"
        / "part-000.parquet"
    )

    current_observation_path = (
        lake_root
        / "processed"
        / "dataset=observations"
        / "endpoint=river-stations"
        / "year=2026"
        / "month=09"
        / "day=29"
        / "hour=21"
        / "run-test"
        / "part-000.parquet"
    )

    historical_observation_path = (
        lake_root
        / "processed"
        / "dataset=observations"
        / "endpoint=river"
        / "year=2026"
        / "month=09"
        / "day=29"
        / "hour=21"
        / "run-test"
        / "part-000.parquet"
    )

    _write_parquet(
        path=station_path,
        query="""
            SELECT
                '44'
                    AS station_id,
                'Station A'
                    AS station_name,
                'Koshi'
                    AS basin_name,
                'run-test'
                    AS run_id,
                TIMESTAMP
                    '2026-09-29 21:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=current_observation_path,
        query="""
            SELECT
                '100'
                    AS source_record_id,
                '44'
                    AS station_id,
                TIMESTAMP
                    '2026-09-29 20:00:00'
                    AS observed_at,
                1.25::DOUBLE
                    AS water_level_m,
                'run-test'
                    AS run_id,
                TIMESTAMP
                    '2026-09-29 21:00:00'
                    AS ingested_at
        """,
    )

    _write_parquet(
        path=historical_observation_path,
        query="""
            SELECT
                '101'
                    AS source_record_id,
                '44'
                    AS station_id,
                TIMESTAMP
                    '2026-09-28 20:00:00'
                    AS observed_at,
                1.10::DOUBLE
                    AS water_level_m,
                'run-test'
                    AS run_id,
                TIMESTAMP
                    '2026-09-29 21:00:00'
                    AS ingested_at
        """,
    )

def test_missing_processed_dataset_fails(
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    with pytest.raises(
        AnalyticsDatasetNotFoundError
    ):
        create_analytics_connection(
            lake_root=lake_root
        )