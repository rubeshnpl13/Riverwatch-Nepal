from datetime import (
    UTC,
    datetime,
    timedelta,
)
from pathlib import Path

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
from riverwatch.processing.models import (
    ProcessedDataset,
)
from riverwatch.processing.spark.pipeline import (
    process_manifest,
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