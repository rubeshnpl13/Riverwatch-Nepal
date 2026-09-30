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
)
from riverwatch.quality.spark.observations import (
    evaluate_observation_quality,
)


def load_raw(
    *,
    spark,
    lake_root: Path,
    endpoint: BipadEndpoint,
):
    manifest_path = find_latest_manifest(
        lake_root=lake_root,
        endpoint=endpoint,
    )

    processing_input = load_processing_input(
        lake_root=lake_root,
        manifest_path=manifest_path,
    )

    raw_pages = read_raw_pages(
        spark=spark,
        processing_input=processing_input,
    )

    return (
        processing_input,
        raw_pages,
    )


def main() -> None:
    lake_root = Path(
        "data/lake"
    )

    spark = create_local_spark_session(
        app_name=(
            "riverwatch-quality-inspection"
        )
    )

    try:
        (
            station_input,
            station_raw,
        ) = load_raw(
            spark=spark,
            lake_root=lake_root,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
        )

        current = (
            transform_station_observations(
                raw_pages=station_raw,
                processing_input=station_input,
            )
        )

        current_checked = (
            evaluate_observation_quality(
                observations=current,
                endpoint=(
                    BipadEndpoint.RIVER_STATIONS
                ),
            )
        )

        print()
        print("=" * 70)
        print("CURRENT OBSERVATION QUALITY")

        current_checked.groupBy(
            "quality_status"
        ).count().orderBy(
            "quality_status"
        ).show()

        current_checked.where(
            "quality_status != 'pass'"
        ).select(
            "station_id",
            "station_name",
            "observed_at",
            "water_level_m",
            "quality_errors",
            "quality_warnings",
        ).show(
            50,
            truncate=False,
        )

        (
            historical_input,
            historical_raw,
        ) = load_raw(
            spark=spark,
            lake_root=lake_root,
            endpoint=BipadEndpoint.RIVER,
        )

        historical = (
            transform_historical_observations(
                raw_pages=historical_raw,
                processing_input=historical_input,
            )
        )

        historical_checked = (
            evaluate_observation_quality(
                observations=historical,
                endpoint=BipadEndpoint.RIVER,
            )
        )

        print()
        print("=" * 70)
        print("HISTORICAL OBSERVATION QUALITY")

        historical_checked.groupBy(
            "quality_status"
        ).count().orderBy(
            "quality_status"
        ).show()

        historical_checked.where(
            "quality_status != 'pass'"
        ).select(
            "source_record_id",
            "station_id",
            "station_name",
            "observed_at",
            "water_level_m",
            "quality_errors",
            "quality_warnings",
        ).show(
            50,
            truncate=False,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()