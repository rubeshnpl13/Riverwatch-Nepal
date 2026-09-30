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
    transform_station_observations,
    transform_stations,
)
from riverwatch.quality.spark.freshness import (
    summarize_current_data_health,
)


def main() -> None:
    lake_root = Path(
        "data/lake"
    )

    spark = create_local_spark_session(
        app_name=(
            "riverwatch-current-health"
        )
    )

    try:
        manifest_path = (
            find_latest_manifest(
                lake_root=lake_root,
                endpoint=(
                    BipadEndpoint
                    .RIVER_STATIONS
                ),
            )
        )

        processing_input = (
            load_processing_input(
                lake_root=lake_root,
                manifest_path=manifest_path,
            )
        )

        raw_pages = read_raw_pages(
            spark=spark,
            processing_input=(
                processing_input
            ),
        )

        stations = transform_stations(
            raw_pages=raw_pages,
            processing_input=(
                processing_input
            ),
        )

        observations = (
            transform_station_observations(
                raw_pages=raw_pages,
                processing_input=(
                    processing_input
                ),
            )
        )

        summary = (
            summarize_current_data_health(
                stations=stations,
                observations=observations,
            )
        )

        print()
        print("=" * 70)
        print("CURRENT DATA HEALTH")
        print()

        print(
            "Status:",
            summary.status.value,
        )

        print(
            "Total stations:",
            summary.total_stations,
        )

        print(
            "Stations with observation:",
            (
                summary
                .stations_with_observation
            ),
        )

        print(
            "Stations without observation:",
            (
                summary
                .stations_without_observation
            ),
        )

        print(
            "Total observations:",
            summary.total_observations,
        )

        print(
            "Fresh observations:",
            summary.fresh_observations,
        )

        print(
            "Stale observations:",
            summary.stale_observations,
        )

        print(
            "Future observations:",
            summary.future_observations,
        )

        print(
            "Unassessable observations:",
            (
                summary
                .unassessable_observations
            ),
        )

        print(
            "Station coverage:",
            f"{summary.coverage_ratio:.2%}",
        )

        print(
            "Observation freshness:",
            f"{summary.freshness_ratio:.2%}",
        )

        print(
            "Oldest observation:",
            summary.oldest_observed_at,
        )

        print(
            "Latest observation:",
            summary.latest_observed_at,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()