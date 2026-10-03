import shutil
from datetime import (
    UTC,
    datetime,
    timedelta,
)
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.manifest_writer import (
    BipadRunManifestWriter,
)
from riverwatch.ingestion.bipad.pagination import (
    BipadRawPage,
)
from riverwatch.ingestion.bipad.raw_writer import (
    BipadRawPageWriter,
)
from riverwatch.ingestion.metrics import (
    IngestionMetrics,
    IngestionRunResult,
)
from riverwatch.processing.manifest_loader import (
    load_processing_input,
)
from riverwatch.processing.models import (
    ProcessedDataset,
)
from riverwatch.processing.spark.pipeline import (
    process_manifest,
)
from riverwatch.processing.spark.recovery import (
    ProcessingRecoveryError,
    process_manifest_retry_safe,
)
from riverwatch.processing.spark.writer import (
    build_processed_output_path,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)

CAPTURED_AT = datetime(
    2026,
    9,
    29,
    7,
    0,
    tzinfo=UTC,
)


def build_manifest(
    *,
    tmp_path: Path,
    endpoint: BipadEndpoint,
    results: list[dict[str, object]],
    run_id: str,
) -> Path:
    store = LocalObjectStore(
        root=tmp_path
    )

    raw_writer = BipadRawPageWriter(
        store=store
    )

    manifest_writer = (
        BipadRunManifestWriter(
            store=store
        )
    )

    raw_result = raw_writer.write_page(
        page=BipadRawPage(
            count=len(results),
            next=None,
            previous=None,
            results=results,
        ),
        endpoint=endpoint,
        run_id=run_id,
        captured_at=CAPTURED_AT,
        page_index=0,
    )

    run_result = IngestionRunResult(
        run_id=run_id,
        endpoint=endpoint,
        started_at=CAPTURED_AT,
        finished_at=(
            CAPTURED_AT
            + timedelta(
                seconds=1,
            )
        ),
        duration_ms=1000,
        metrics=IngestionMetrics(
            records_received=len(
                results
            ),
            records_valid=len(
                results
            ),
            records_invalid=0,
            stations_emitted=(
                len(results)
                if endpoint
                is BipadEndpoint.RIVER_STATIONS
                else 0
            ),
            observations_emitted=(
                len(results)
            ),
            records_without_observation=0,
        ),
        quarantined_records=(),
    )

    manifest_result = (
        manifest_writer.write_manifest(
            run_result=run_result,
            captured_at=CAPTURED_AT,
            raw_pages=[
                raw_result,
            ],
            quarantine_objects=[],
        )
    )

    return (
        tmp_path
        / manifest_result.object.key
    )


def test_processes_historical_manifest_to_parquet(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    manifest_path = build_manifest(
        tmp_path=tmp_path,
        endpoint=BipadEndpoint.RIVER,
        run_id="historical-pipeline",
        results=[
            {
                "id": 100,
                "station": 44,
                "stationSeriesId": 500,
                "title": "Test River",
                "basin": "Gandaki",
                "waterLevel": 1.25,
                "waterLevelOn": (
                    "2026-09-29T03:00:00Z"
                ),
            }
        ],
    )

    result = process_manifest(
        spark=spark,
        lake_root=tmp_path,
        manifest_path=manifest_path,
    )

    assert (
        result.endpoint
        is BipadEndpoint.RIVER
    )

    assert len(
        result.outputs
    ) == 1

    output = result.outputs[0]

    assert (
        output.dataset
        is ProcessedDataset.OBSERVATIONS
    )

    assert output.row_count == 1
    assert output.path.exists()

    restored = spark.read.parquet(
        str(output.path)
    )

    assert restored.count() == 1

    assert (
        result.quality_report_path.exists()
    )

    report_text = (
        result.quality_report_path
        .read_text(
            encoding="utf-8"
        )
    )

    assert (
        '"endpoint":"river"'
        in report_text
    )

    assert (
        '"total_rows":1'
        in report_text
    )


def test_processes_station_manifest_to_two_outputs(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    manifest_path = build_manifest(
        tmp_path=tmp_path,
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
        run_id="station-pipeline",
        results=[
            {
                "id": 282,
                "stationSeriesId": 500,
                "title": "Banara River",
                "basin": "Mahakali",
                "waterLevel": 1.2,
                "waterLevelOn": (
                    "2026-09-29T03:00:00Z"
                ),
            },
            {
                "id": 162,
                "stationSeriesId": 501,
                "title": "No Observation",
                "waterLevelOn": None,
            },
        ],
    )

    result = process_manifest(
        spark=spark,
        lake_root=tmp_path,
        manifest_path=manifest_path,
    )

    assert len(
        result.outputs
    ) == 2

    outputs = {
        output.dataset: output
        for output in result.outputs
    }

    assert (
        outputs[
            ProcessedDataset.STATIONS
        ].row_count
        == 2
    )

    assert (
        outputs[
            ProcessedDataset.OBSERVATIONS
        ].row_count
        == 1
    )

    assert (
        result.quality_report_path.exists()
    )

    report_text = (
        result.quality_report_path
        .read_text(
            encoding="utf-8"
        )
    )

    assert (
        '"endpoint":"river-stations"'
        in report_text
    )

    assert (
        '"total_stations":2'
        in report_text
    )

def test_retry_safe_processing_recovers_completed_run(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    manifest_path = build_manifest(
        tmp_path=tmp_path,
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
        run_id="retry-safe-complete",
        results=[
            {
                "id": 282,
                "stationSeriesId": 500,
                "title": "Banara River",
                "basin": "Mahakali",
                "waterLevel": 1.2,
                "waterLevelOn": (
                    "2026-09-29T03:00:00Z"
                ),
            }
        ],
    )

    first = (
        process_manifest_retry_safe(
            spark=spark,
            lake_root=tmp_path,
            manifest_path=manifest_path,
        )
    )

    second = (
        process_manifest_retry_safe(
            spark=spark,
            lake_root=tmp_path,
            manifest_path=manifest_path,
        )
    )

    assert second.run_id == first.run_id

    assert (
        second.quality_report_path
        == first.quality_report_path
    )

    assert {
        output.dataset:
            output.row_count
        for output in second.outputs
    } == {
        ProcessedDataset.STATIONS: 1,
        ProcessedDataset.OBSERVATIONS: 1,
    }


def test_retry_safe_processing_recovers_after_partial_commit(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    manifest_path = build_manifest(
        tmp_path=tmp_path,
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
        run_id="partial-processing",
        results=[
            {
                "id": 282,
                "stationSeriesId": 500,
                "title": "Banara River",
                "basin": "Mahakali",
                "waterLevel": 1.2,
                "waterLevelOn": (
                    "2026-09-29T03:00:00Z"
                ),
            }
        ],
    )

    first = (
        process_manifest_retry_safe(
            spark=spark,
            lake_root=tmp_path,
            manifest_path=manifest_path,
        )
    )

    observations = next(
        output
        for output in first.outputs
        if (
            output.dataset
            is ProcessedDataset.OBSERVATIONS
        )
    )

    shutil.rmtree(
        observations.path
    )

    first.quality_report_path.unlink()

    recovered = (
        process_manifest_retry_safe(
            spark=spark,
            lake_root=tmp_path,
            manifest_path=manifest_path,
        )
    )

    outputs = {
        output.dataset:
            output
        for output in recovered.outputs
    }

    assert (
        outputs[
            ProcessedDataset.STATIONS
        ].path.exists()
    )

    assert (
        outputs[
            ProcessedDataset.OBSERVATIONS
        ].path.exists()
    )

    assert (
        outputs[
            ProcessedDataset.OBSERVATIONS
        ].path
        / "_SUCCESS"
    ).is_file()

    assert (
        recovered
        .quality_report_path
        .is_file()
    )


def test_retry_safe_processing_rejects_incomplete_final_output(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    manifest_path = build_manifest(
        tmp_path=tmp_path,
        endpoint=BipadEndpoint.RIVER,
        run_id="incomplete-final",
        results=[
            {
                "id": 100,
                "station": 44,
                "stationSeriesId": 500,
                "title": "Test River",
                "basin": "Gandaki",
                "waterLevel": 1.25,
                "waterLevelOn": (
                    "2026-09-29T03:00:00Z"
                ),
            }
        ],
    )

    processing_input = (
        load_processing_input(
            lake_root=tmp_path,
            manifest_path=manifest_path,
        )
    )

    output_path = (
        build_processed_output_path(
            lake_root=tmp_path,
            dataset=(
                ProcessedDataset.OBSERVATIONS
            ),
            processing_input=(
                processing_input
            ),
        )
    )

    output_path.mkdir(
        parents=True
    )

    (
        output_path
        / "part-00000.parquet"
    ).write_bytes(
        b"incomplete"
    )

    with pytest.raises(
        ProcessingRecoveryError,
        match=(
            "without a Spark success marker"
        ),
    ):
        process_manifest_retry_safe(
            spark=spark,
            lake_root=tmp_path,
            manifest_path=manifest_path,
        )

#test for separate output_root
def test_pipeline_can_write_to_separate_output_root(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    lake_root = (
        tmp_path
        / "lake"
    )

    output_root = (
        tmp_path
        / "staging"
    )

    manifest_path = build_manifest(
        tmp_path=lake_root,
        endpoint=BipadEndpoint.RIVER,
        run_id="separate-output-root",
        results=[
            {
                "id": 100,
                "station": 44,
                "stationSeriesId": 500,
                "title": "Test River",
                "basin": "Gandaki",
                "waterLevel": 1.25,
                "waterLevelOn": (
                    "2026-09-29T03:00:00Z"
                ),
            }
        ],
    )

    result = process_manifest(
        spark=spark,
        lake_root=lake_root,
        manifest_path=manifest_path,
        output_root=output_root,
    )

    assert all(
        output.path.is_relative_to(
            output_root
        )
        for output in result.outputs
    )

    assert (
        result
        .quality_report_path
        .is_relative_to(
            output_root
        )
    )

    assert not (
        lake_root
        / "processed"
    ).exists()

    assert not (
        lake_root
        / "quality"
    ).exists()