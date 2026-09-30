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
from riverwatch.quality.model import (
    DatasetQualitySummary,
    QualityDataset,
)
from riverwatch.quality.spark.observations import (
    evaluate_observation_quality,
)
from riverwatch.quality.spark.stations import (
    evaluate_station_quality,
)
from riverwatch.quality.spark.summary import (
    summarize_dataset_quality,
)


def print_summary(
    *,
    title: str,
    summary: DatasetQualitySummary,
) -> None:
    print()
    print("=" * 70)
    print(title)
    print()

    print(
        "Status:",
        summary.status.value,
    )

    print(
        "Total rows:",
        summary.total_rows,
    )

    print(
        "Pass:",
        summary.pass_rows,
    )

    print(
        "Warn:",
        summary.warn_rows,
    )

    print(
        "Fail:",
        summary.fail_rows,
    )

    print(
        "Affected rows:",
        summary.affected_rows,
    )

    print(
        "Pass ratio:",
        f"{summary.pass_ratio:.2%}",
    )

    print()
    print("Error rules:")

    if summary.error_rule_counts:
        for item in (
            summary.error_rule_counts
        ):
            print(
                f"  {item.rule}: "
                f"{item.count}"
            )
    else:
        print("  none")

    print()
    print("Warning rules:")

    if summary.warning_rule_counts:
        for item in (
            summary.warning_rule_counts
        ):
            print(
                f"  {item.rule}: "
                f"{item.count}"
            )
    else:
        print("  none")


def main() -> None:
    lake_root = Path(
        "data/lake"
    )

    spark = create_local_spark_session(
        app_name=(
            "riverwatch-quality-summary"
        )
    )

    try:
        #
        # Current station run
        #

        station_manifest = (
            find_latest_manifest(
                lake_root=lake_root,
                endpoint=(
                    BipadEndpoint
                    .RIVER_STATIONS
                ),
            )
        )

        station_input = (
            load_processing_input(
                lake_root=lake_root,
                manifest_path=(
                    station_manifest
                ),
            )
        )

        station_raw = read_raw_pages(
            spark=spark,
            processing_input=(
                station_input
            ),
        )

        stations = (
            evaluate_station_quality(
                stations=(
                    transform_stations(
                        raw_pages=station_raw,
                        processing_input=(
                            station_input
                        ),
                    )
                )
            )
        )

        current_observations = (
            evaluate_observation_quality(
                observations=(
                    transform_station_observations(
                        raw_pages=station_raw,
                        processing_input=(
                            station_input
                        ),
                    )
                ),
                endpoint=(
                    BipadEndpoint
                    .RIVER_STATIONS
                ),
            )
        )

        station_summary = (
            summarize_dataset_quality(
                dataframe=stations,
                dataset=(
                    QualityDataset.STATIONS
                ),
            )
        )

        current_summary = (
            summarize_dataset_quality(
                dataframe=(
                    current_observations
                ),
                dataset=(
                    QualityDataset.OBSERVATIONS
                ),
            )
        )

        print_summary(
            title="STATION QUALITY SUMMARY",
            summary=station_summary,
        )

        print_summary(
            title=(
                "CURRENT OBSERVATION "
                "QUALITY SUMMARY"
            ),
            summary=current_summary,
        )

        #
        # Historical observations
        #

        historical_manifest = (
            find_latest_manifest(
                lake_root=lake_root,
                endpoint=(
                    BipadEndpoint.RIVER
                ),
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

        historical_raw = (
            read_raw_pages(
                spark=spark,
                processing_input=(
                    historical_input
                ),
            )
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
                endpoint=(
                    BipadEndpoint.RIVER
                ),
            )
        )

        historical_summary = (
            summarize_dataset_quality(
                dataframe=historical,
                dataset=(
                    QualityDataset.OBSERVATIONS
                ),
            )
        )

        print_summary(
            title=(
                "HISTORICAL OBSERVATION "
                "QUALITY SUMMARY"
            ),
            summary=historical_summary,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()