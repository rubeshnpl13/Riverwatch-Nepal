from pathlib import Path

from pyspark.sql import (
    functions as F,
)

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
    transform_stations,
)
from riverwatch.quality.spark.stations import (
    evaluate_station_quality,
)


def main() -> None:
    lake_root = Path(
        "data/lake"
    )

    spark = create_local_spark_session(
        app_name=(
            "riverwatch-station-quality"
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
                manifest_path=(
                    manifest_path
                ),
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

        checked = (
            evaluate_station_quality(
                stations=stations
            )
        )

        print()
        print("=" * 70)
        print("STATION QUALITY")

        checked.groupBy(
            "quality_status"
        ).count().orderBy(
            "quality_status"
        ).show()

        print()
        print(
            "QUALITY ERROR COUNTS"
        )

        (
            checked
            .select(
                F.explode_outer(
                    "quality_errors"
                ).alias(
                    "rule"
                )
            )
            .where(
                F.col(
                    "rule"
                ).isNotNull()
            )
            .groupBy(
                "rule"
            )
            .count()
            .orderBy(
                F.desc(
                    "count"
                )
            )
            .show(
                truncate=False
            )
        )

        print()
        print(
            "QUALITY WARNING COUNTS"
        )

        (
            checked
            .select(
                F.explode_outer(
                    "quality_warnings"
                ).alias(
                    "rule"
                )
            )
            .where(
                F.col(
                    "rule"
                ).isNotNull()
            )
            .groupBy(
                "rule"
            )
            .count()
            .orderBy(
                F.desc(
                    "count"
                )
            )
            .show(
                truncate=False
            )
        )

        print()
        print(
            "FLAGGED STATIONS"
        )

        (
            checked
            .where(
                "quality_status != 'pass'"
            )
            .select(
                "station_id",
                "station_series_id",
                "station_name",
                "basin_name",
                "latitude",
                "longitude",
                "quality_errors",
                "quality_warnings",
            )
            .show(
                100,
                truncate=False,
            )
        )

        print(
            "Total stations:",
            checked.count(),
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()