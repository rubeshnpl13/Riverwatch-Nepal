from pathlib import Path

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.discovery import (
    find_latest_manifest,
)
from riverwatch.processing.manifest_loader import (
    load_processing_input,
)
from riverwatch.processing.spark.reader import (
    read_raw_pages,
)
from riverwatch.processing.spark.session import (
    create_local_spark_session,
)
from riverwatch.processing.spark.transforms import (
    transform_historical_observations,
    transform_station_observations,
    transform_stations,
)


def load_run(
    *,
    endpoint: BipadEndpoint,
):
    lake_root = Path(
        "data/lake"
    )

    manifest_path = (
        find_latest_manifest(
            lake_root=lake_root,
            endpoint=endpoint,
        )
    )

    return load_processing_input(
        lake_root=lake_root,
        manifest_path=manifest_path,
    )


def main() -> None:
    spark = create_local_spark_session(
        app_name=(
            "riverwatch-transform-inspection"
        )
    )

    try:
        station_input = load_run(
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            )
        )

        station_raw = read_raw_pages(
            spark=spark,
            processing_input=station_input,
        )

        stations = transform_stations(
            raw_pages=station_raw,
            processing_input=station_input,
        )

        current_observations = (
            transform_station_observations(
                raw_pages=station_raw,
                processing_input=station_input,
            )
        )

        print()
        print(
            "=" * 70
        )
        print(
            "CURRENT STATION TRANSFORMATION"
        )

        print(
            "Stations:",
            stations.count(),
        )

        print(
            "Observations:",
            current_observations.count(),
        )

        stations.printSchema()

        stations.select(
            "station_id",
            "station_name",
            "latitude",
            "longitude",
        ).show(
            10,
            truncate=False,
        )

        current_observations.select(
            "station_id",
            "station_name",
            "observed_at",
            "water_level_m",
            "trend",
        ).show(
            10,
            truncate=False,
        )

        historical_input = load_run(
            endpoint=BipadEndpoint.RIVER
        )

        historical_raw = read_raw_pages(
            spark=spark,
            processing_input=(
                historical_input
            ),
        )

        historical_observations = (
            transform_historical_observations(
                raw_pages=historical_raw,
                processing_input=(
                    historical_input
                ),
            )
        )

        print()
        print(
            "=" * 70
        )
        print(
            "HISTORICAL TRANSFORMATION"
        )

        print(
            "Observations:",
            historical_observations.count(),
        )

        historical_observations.printSchema()

        historical_observations.select(
            "source_record_id",
            "station_id",
            "station_name",
            "observed_at",
            "water_level_m",
            "trend",
        ).show(
            10,
            truncate=False,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()