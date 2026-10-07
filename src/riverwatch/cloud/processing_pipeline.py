from __future__ import annotations

from dataclasses import (
    asdict,
    dataclass,
)

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from riverwatch.cloud.processing_input import (
    load_gcs_processing_input,
)
from riverwatch.cloud.processing_output import (
    CloudProcessedOutput,
    GcsProcessedOutputState,
    ProcessingCloudStore,
    build_gcs_processed_output_target,
    create_or_verify_gcs_object,
    inspect_gcs_processed_output,
    write_or_recover_gcs_processed_output,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.models import (
    ProcessedDataset,
    ProcessingInputRun,
)
from riverwatch.processing.spark.reader import (
    read_raw_pages,
)
from riverwatch.processing.spark.transforms import (
    transform_historical_observations,
    transform_station_observations,
    transform_stations,
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
from riverwatch.storage.paths import (
    build_quality_report_key,
)
from riverwatch.storage.serialization import (
    serialize_json,
)


class CloudProcessingPipelineError(
    RuntimeError,
):
    """Cloud Spark processing cannot complete safely."""


@dataclass(
    frozen=True,
    slots=True,
)
class CloudProcessingDatasetResult:
    dataset: ProcessedDataset
    key: str
    uri: str
    row_count: int


@dataclass(
    frozen=True,
    slots=True,
)
class CloudProcessingRunResult:
    run_id: str
    endpoint: BipadEndpoint
    outputs: tuple[
        CloudProcessingDatasetResult,
        ...,
    ]
    quality_report_key: str
    quality_report_uri: str


def process_gcs_manifest(
    *,
    spark: SparkSession,
    store: ProcessingCloudStore,
    manifest_key: str,
) -> CloudProcessingRunResult:
    processing_input = (
        load_gcs_processing_input(
            store=store,
            manifest_key=manifest_key,
        )
    )

    return process_gcs_processing_input(
        spark=spark,
        store=store,
        processing_input=processing_input,
    )


def process_gcs_processing_input(
    *,
    spark: SparkSession,
    store: ProcessingCloudStore,
    processing_input: ProcessingInputRun,
) -> CloudProcessingRunResult:
    raw_pages = read_raw_pages(
        spark=spark,
        processing_input=processing_input,
    )

    if (
        processing_input.endpoint
        is BipadEndpoint.RIVER
    ):
        return _process_historical_river(
            spark=spark,
            store=store,
            processing_input=processing_input,
            raw_pages=raw_pages,
        )

    if (
        processing_input.endpoint
        is BipadEndpoint.RIVER_STATIONS
    ):
        return _process_current_stations(
            spark=spark,
            store=store,
            processing_input=processing_input,
            raw_pages=raw_pages,
        )

    raise CloudProcessingPipelineError(
        "Unsupported processing endpoint: "
        f"{processing_input.endpoint.value}"
    )


def _process_historical_river(
    *,
    spark: SparkSession,
    store: ProcessingCloudStore,
    processing_input: ProcessingInputRun,
    raw_pages: DataFrame,
) -> CloudProcessingRunResult:
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
            dataframe=(
                checked_observations
            ),
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

    output_frames = (
        (
            ProcessedDataset.OBSERVATIONS,
            observations,
        ),
    )

    return _commit_cloud_run(
        spark=spark,
        store=store,
        processing_input=processing_input,
        output_frames=output_frames,
        quality_report=quality_report,
    )


def _process_current_stations(
    *,
    spark: SparkSession,
    store: ProcessingCloudStore,
    processing_input: ProcessingInputRun,
    raw_pages: DataFrame,
) -> CloudProcessingRunResult:
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
                BipadEndpoint
                .RIVER_STATIONS
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
            dataframe=(
                checked_observations
            ),
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
        station_summary=station_summary,
        observation_summary=(
            observation_summary
        ),
        current_data_health=(
            current_data_health
        ),
    )

    output_frames = (
        (
            ProcessedDataset.STATIONS,
            stations,
        ),
        (
            ProcessedDataset.OBSERVATIONS,
            observations,
        ),
    )

    return _commit_cloud_run(
        spark=spark,
        store=store,
        processing_input=processing_input,
        output_frames=output_frames,
        quality_report=quality_report,
    )


def _commit_cloud_run(
    *,
    spark: SparkSession,
    store: ProcessingCloudStore,
    processing_input: ProcessingInputRun,
    output_frames: tuple[
        tuple[
            ProcessedDataset,
            DataFrame,
        ],
        ...,
    ],
    quality_report: QualityRunReport,
) -> CloudProcessingRunResult:
    targets = tuple(
        (
            dataset,
            dataframe,
            build_gcs_processed_output_target(
                bucket_name=(
                    store.bucket_name
                ),
                dataset=dataset,
                processing_input=(
                    processing_input
                ),
            ),
        )
        for dataset, dataframe
        in output_frames
    )

    # Preflight every final dataset before
    # writing any of them. This prevents a
    # known incomplete second dataset from
    # causing creation of a new first dataset.
    for _, _, target in targets:
        state = (
            inspect_gcs_processed_output(
                store=store,
                target=target,
            )
        )

        if (
            state
            is GcsProcessedOutputState.INCOMPLETE
        ):
            raise CloudProcessingPipelineError(
                "Processed output exists "
                "without a Spark success "
                "marker: "
                f"{target.uri}"
            )

    outputs: list[
        CloudProcessingDatasetResult
    ] = []

    for (
        _dataset,
        dataframe,
        target,
    ) in targets:
        written = (
            write_or_recover_gcs_processed_output(
                spark=spark,
                dataframe=dataframe,
                store=store,
                target=target,
            )
        )

        outputs.append(
            _dataset_result(
                written=written,
                key=target.key,
            )
        )

    quality_report_key = (
        build_quality_report_key(
            endpoint=(
                processing_input.endpoint
            ),
            captured_at=(
                processing_input.captured_at
            ),
            run_id=(
                processing_input.run_id
            ),
        )
    )

    quality_report_data = serialize_json(
        asdict(
            quality_report
        )
    )

    quality_report_uri = (
        create_or_verify_gcs_object(
            store=store,
            key=quality_report_key,
            data=quality_report_data,
            content_type=(
                "application/json"
            ),
        )
    )

    return CloudProcessingRunResult(
        run_id=processing_input.run_id,
        endpoint=processing_input.endpoint,
        outputs=tuple(
            outputs
        ),
        quality_report_key=(
            quality_report_key
        ),
        quality_report_uri=(
            quality_report_uri
        ),
    )


def _dataset_result(
    *,
    written: CloudProcessedOutput,
    key: str,
) -> CloudProcessingDatasetResult:
    return CloudProcessingDatasetResult(
        dataset=written.dataset,
        key=key,
        uri=written.uri,
        row_count=written.row_count,
    )