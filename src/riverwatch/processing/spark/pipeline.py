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


def process_manifest(
    *,
    spark: SparkSession,
    lake_root: Path,
    manifest_path: Path,
) -> ProcessingRunResult:
    processing_input = load_processing_input(
        lake_root=lake_root,
        manifest_path=manifest_path,
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
            lake_root=lake_root,
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

        observation_count = (
            observations.count()
        )

        observation_path = (
            write_processed_parquet(
                dataframe=observations,
                lake_root=lake_root,
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

    elif (
        processing_input.endpoint
        is BipadEndpoint.RIVER_STATIONS
    ):
        ensure_processed_outputs_absent(
            lake_root=lake_root,
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

        station_count = stations.count()

        observation_count = (
            observations.count()
        )

        station_path = (
            write_processed_parquet(
                dataframe=stations,
                lake_root=lake_root,
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
                lake_root=lake_root,
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
    )