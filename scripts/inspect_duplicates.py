from pathlib import Path

from pyspark.sql import (
    DataFrame,
)
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
    transform_historical_observations,
    transform_station_observations,
    transform_stations,
)
from riverwatch.quality.model import (
    ObservationQualityRule,
    StationQualityRule,
)
from riverwatch.quality.spark.observations import (
    evaluate_observation_quality,
)
from riverwatch.quality.spark.stations import (
    evaluate_station_quality,
)


def show_rule(
    *,
    dataframe: DataFrame,
    array_column: str,
    rule: str,
    columns: list[str],
) -> None:
    matching = dataframe.where(
        F.array_contains(
            F.col(
                array_column
            ),
            rule,
        )
    )

    print(
        f"{rule}:",
        matching.count(),
    )

    matching.select(
        *columns
    ).show(
        50,
        truncate=False,
    )


def main() -> None:
    lake_root = Path(
        "data/lake"
    )

    spark = create_local_spark_session(
        app_name=(
            "riverwatch-duplicate-inspection"
        )
    )

    try:
        #
        # Stations + current observations
        #

        station_manifest = (
            find_latest_manifest(
                lake_root=lake_root,
                endpoint=(
                    BipadEndpoint.RIVER_STATIONS
                ),
            )
        )

        station_input = (
            load_processing_input(
                lake_root=lake_root,
                manifest_path=station_manifest,
            )
        )

        station_raw = read_raw_pages(
            spark=spark,
            processing_input=station_input,
        )

        stations = evaluate_station_quality(
            stations=transform_stations(
                raw_pages=station_raw,
                processing_input=station_input,
            )
        )

        current = (
            evaluate_observation_quality(
                observations=(
                    transform_station_observations(
                        raw_pages=station_raw,
                        processing_input=station_input,
                    )
                ),
                endpoint=(
                    BipadEndpoint.RIVER_STATIONS
                ),
            )
        )

        print()
        print("=" * 70)
        print("STATION DUPLICATES")

        show_rule(
            dataframe=stations,
            array_column="quality_errors",
            rule=(
                StationQualityRule
                .DUPLICATE_STATION_ID
                .value
            ),
            columns=[
                "station_id",
                "station_series_id",
                "station_name",
            ],
        )

        show_rule(
            dataframe=stations,
            array_column="quality_warnings",
            rule=(
                StationQualityRule
                .DUPLICATE_STATION_SERIES_ID
                .value
            ),
            columns=[
                "station_id",
                "station_series_id",
                "station_name",
            ],
        )

        print()
        print("=" * 70)
        print(
            "CURRENT OBSERVATION DUPLICATES"
        )

        show_rule(
            dataframe=current,
            array_column="quality_errors",
            rule=(
                ObservationQualityRule
                .DUPLICATE_SOURCE_RECORD_ID
                .value
            ),
            columns=[
                "source_record_id",
                "station_id",
                "observed_at",
                "water_level_m",
            ],
        )

        show_rule(
            dataframe=current,
            array_column="quality_warnings",
            rule=(
                ObservationQualityRule
                .DUPLICATE_STATION_OBSERVED_AT
                .value
            ),
            columns=[
                "source_record_id",
                "station_id",
                "observed_at",
                "water_level_m",
            ],
        )

        #
        # Historical observations
        #

        historical_manifest = (
            find_latest_manifest(
                lake_root=lake_root,
                endpoint=BipadEndpoint.RIVER,
            )
        )

        historical_input = (
            load_processing_input(
                lake_root=lake_root,
                manifest_path=(
                    historical_manifest
                ),
            )
        )

        historical_raw = read_raw_pages(
            spark=spark,
            processing_input=historical_input,
        )

        historical = (
            evaluate_observation_quality(
                observations=(
                    transform_historical_observations(
                        raw_pages=historical_raw,
                        processing_input=(
                            historical_input
                        ),
                    )
                ),
                endpoint=BipadEndpoint.RIVER,
            )
        )

        print()
        print("=" * 70)
        print(
            "HISTORICAL OBSERVATION DUPLICATES"
        )

        show_rule(
            dataframe=historical,
            array_column="quality_errors",
            rule=(
                ObservationQualityRule
                .DUPLICATE_SOURCE_RECORD_ID
                .value
            ),
            columns=[
                "source_record_id",
                "station_id",
                "observed_at",
                "water_level_m",
            ],
        )

        show_rule(
            dataframe=historical,
            array_column="quality_warnings",
            rule=(
                ObservationQualityRule
                .DUPLICATE_STATION_OBSERVED_AT
                .value
            ),
            columns=[
                "source_record_id",
                "station_id",
                "observed_at",
                "water_level_m",
            ],
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()