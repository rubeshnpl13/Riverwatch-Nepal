from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection

from riverwatch.analytics.errors import (
    AnalyticsDatasetNotFoundError,
)
from riverwatch.analytics.views import (
    register_analytics_views,
)

STATIONS_VIEW = "stations"
OBSERVATIONS_VIEW = "observations"


def _dataset_root(
    *,
    lake_root: Path,
    dataset: str,
) -> Path:
    return (
        lake_root
        / "processed"
        / f"dataset={dataset}"
    )


def _require_parquet_dataset(
    *,
    lake_root: Path,
    dataset: str,
) -> Path:
    dataset_root = _dataset_root(
        lake_root=lake_root,
        dataset=dataset,
    )

    if not dataset_root.exists():
        raise AnalyticsDatasetNotFoundError(
            dataset=dataset,
            path=dataset_root,
        )

    if not any(
        dataset_root.rglob(
            "*.parquet"
        )
    ):
        raise AnalyticsDatasetNotFoundError(
            dataset=dataset,
            path=dataset_root,
        )

    return dataset_root


def _register_parquet_view(
    *,
    connection: DuckDBPyConnection,
    dataset_root: Path,
    view_name: str,
) -> None:
    parquet_glob = str(
        dataset_root
        / "**"
        / "*.parquet"
    )

    relation = connection.read_parquet(
        parquet_glob,
        hive_partitioning=True,
        union_by_name=True,
    )

    relation.create_view(
        view_name,
        replace=True,
    )


def register_processed_views(
    *,
    connection: DuckDBPyConnection,
    lake_root: Path,
) -> None:
    stations_root = (
        _require_parquet_dataset(
            lake_root=lake_root,
            dataset="stations",
        )
    )

    observations_root = (
        _require_parquet_dataset(
            lake_root=lake_root,
            dataset="observations",
        )
    )

    _register_parquet_view(
        connection=connection,
        dataset_root=stations_root,
        view_name=STATIONS_VIEW,
    )

    _register_parquet_view(
        connection=connection,
        dataset_root=observations_root,
        view_name=OBSERVATIONS_VIEW,
    )

    register_analytics_views(
        connection=connection
    )


def create_analytics_connection(
    *,
    lake_root: Path,
) -> DuckDBPyConnection:
    connection = duckdb.connect(
        database=":memory:"
    )

    connection.execute(
        "SET TimeZone = 'UTC'"
    )

    try:
        register_processed_views(
            connection=connection,
            lake_root=lake_root,
        )
    except Exception:
        connection.close()
        raise

    return connection