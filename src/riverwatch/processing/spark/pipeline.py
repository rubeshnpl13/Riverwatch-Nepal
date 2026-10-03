from pathlib import Path

from pyspark.sql import SparkSession

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.manifest_loader import (
    load_processing_input,
)
from riverwatch.processing.models import (
    ProcessedDataset,
    ProcessedOutput,
    ProcessingRunResult,
)
from riverwatch.processing.spark.reader import (
    read_raw_pages,
)
from riverwatch.processing.spark.transforms import (
    transform_historical_observations,
    transform_station_observations,
    transform_stations,
)
from riverwatch.processing.spark.writer import (
    ensure_processed_outputs_absent,
    write_processed_parquet,
)
from riverwatch.quality.model import (
    QualityDataset,
    QualityRunReport,
)
from riverwatch.quality.spark.freshness import (
    summarize_current_data_health,
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
from riverwatch.quality.writer import (
    QualityReportWriter,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)


def process_manifest(
    *,
    spark: SparkSession,
    lake_root: Path,
    manifest_path: Path,
    output_root: Path | None = None,
) -> ProcessingRunResult:
    processing_input = load_processing_input(
        lake_root=lake_root,
        manifest_path=manifest_path,
    )
    target_root = (
        output_root
        if output_root is not None
        else lake_root
    )

    quality_writer = QualityReportWriter(
        store=LocalObjectStore(
            root=target_root
        )
    )

    raw_pages = read_raw_pages(
        spark=spark,
        processing_input=processing_input,
    )

    outputs: list[
        ProcessedOutput
    ] = []

    if (
        processing_input.endpoint
        is BipadEndpoint.RIVER
    ):
        ensure_processed_outputs_absent(
            lake_root=target_root,
            datasets=[
                ProcessedDataset.OBSERVATIONS,
            ],
            processing_input=processing_input,
        )

        observations = (
            transform_historical_observations(
                raw_pages=raw_pages,
                processing_input=(
                    processing_input
                ),
            )
        )

        checked_observations = (
            evaluate_observation_quality(
                observations=observations,
                endpoint=BipadEndpoint.RIVER,
            )
        )

        observation_summary = (
            summarize_dataset_quality(
                dataframe=checked_observations,
                dataset=(
                    QualityDataset.OBSERVATIONS
                ),
            )
        )

        quality_report = QualityRunReport(
            report_version=1,
            run_id=processing_input.run_id,
            endpoint=processing_input.endpoint,
            captured_at=(
                processing_input.captured_at
            ),
            station_summary=None,
            observation_summary=(
                observation_summary
            ),
            current_data_health=None,
        )

        observation_count = (
            observation_summary.total_rows
        )

        observation_path = (
            write_processed_parquet(
                dataframe=observations,
                lake_root=target_root,
                dataset=(
                    ProcessedDataset.OBSERVATIONS
                ),
                processing_input=(
                    processing_input
                ),
            )
        )

        outputs.append(
            ProcessedOutput(
                dataset=(
                    ProcessedDataset.OBSERVATIONS
                ),
                path=observation_path,
                row_count=observation_count,
            )
        )

        quality_object = (
            quality_writer.write_report(
                report=quality_report
            )
        )

        quality_report_path = (
            target_root
            / quality_object.key
        )

    elif (
        processing_input.endpoint
        is BipadEndpoint.RIVER_STATIONS
    ):
        ensure_processed_outputs_absent(
            lake_root=target_root,
            datasets=[
                ProcessedDataset.STATIONS,
                ProcessedDataset.OBSERVATIONS,
            ],
            processing_input=processing_input,
        )

        stations = transform_stations(
            raw_pages=raw_pages,
            processing_input=processing_input,
        )

        observations = (
            transform_station_observations(
                raw_pages=raw_pages,
                processing_input=(
                    processing_input
                ),
            )
        )

        checked_stations = (
            evaluate_station_quality(
                stations=stations
            )
        )

        checked_observations = (
            evaluate_observation_quality(
                observations=observations,
                endpoint=(
                    BipadEndpoint.RIVER_STATIONS
                ),
            )
        )

        station_summary = (
            summarize_dataset_quality(
                dataframe=checked_stations,
                dataset=(
                    QualityDataset.STATIONS
                ),
            )
        )

        observation_summary = (
            summarize_dataset_quality(
                dataframe=checked_observations,
                dataset=(
                    QualityDataset.OBSERVATIONS
                ),
            )
        )

        current_data_health = (
            summarize_current_data_health(
                stations=stations,
                observations=observations,
            )
        )

        quality_report = QualityRunReport(
            report_version=1,
            run_id=processing_input.run_id,
            endpoint=processing_input.endpoint,
            captured_at=(
                processing_input.captured_at
            ),
            station_summary=(
                station_summary
            ),
            observation_summary=(
                observation_summary
            ),
            current_data_health=(
                current_data_health
            ),
        )

        station_count = (
            station_summary.total_rows
        )

        observation_count = (
            observation_summary.total_rows
        )

        station_path = (
            write_processed_parquet(
                dataframe=stations,
                lake_root=target_root,
                dataset=(
                    ProcessedDataset.STATIONS
                ),
                processing_input=(
                    processing_input
                ),
            )
        )

        observation_path = (
            write_processed_parquet(
                dataframe=observations,
                lake_root=target_root,
                dataset=(
                    ProcessedDataset.OBSERVATIONS
                ),
                processing_input=(
                    processing_input
                ),
            )
        )

        outputs.extend(
            [
                ProcessedOutput(
                    dataset=(
                        ProcessedDataset.STATIONS
                    ),
                    path=station_path,
                    row_count=station_count,
                ),
                ProcessedOutput(
                    dataset=(
                        ProcessedDataset.OBSERVATIONS
                    ),
                    path=observation_path,
                    row_count=(
                        observation_count
                    ),
                ),
            ]
        )

        quality_object = (
            quality_writer.write_report(
                report=quality_report
            )
        )

        quality_report_path = (
            target_root
            / quality_object.key
        )

    else:
        raise ValueError(
            "Unsupported processing endpoint: "
            f"{processing_input.endpoint.value}"
        )

    return ProcessingRunResult(
        run_id=processing_input.run_id,
        endpoint=processing_input.endpoint,
        outputs=tuple(
            outputs
        ),
        quality_report_path=(
            quality_report_path
        ),
    )