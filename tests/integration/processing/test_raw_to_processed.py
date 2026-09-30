import json
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


def test_raw_manifest_to_processed_parquet(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
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

    captured_at = datetime(
        2026,
        9,
        29,
        7,
        0,
        tzinfo=UTC,
    )

    page_zero = raw_writer.write_page(
        page=BipadRawPage(
            count=2,
            next="next-page",
            previous=None,
            results=[
                {
                    "id": 100,
                    "station": 44,
                    "stationSeriesId": 500,
                    "title": "River A",
                    "basin": "Gandaki",
                    "waterLevel": 1.25,
                    "waterLevelOn": (
                        "2026-09-29T03:00:00Z"
                    ),
                }
            ],
        ),
        endpoint=BipadEndpoint.RIVER,
        run_id="integration-run",
        captured_at=captured_at,
        page_index=0,
    )

    page_one = raw_writer.write_page(
        page=BipadRawPage(
            count=2,
            next=None,
            previous="previous-page",
            results=[
                {
                    "id": 101,
                    "station": 45,
                    "stationSeriesId": 501,
                    "title": "River B",
                    "basin": "Koshi",
                    "waterLevel": 2.50,
                    "waterLevelOn": (
                        "2026-09-29T03:15:00Z"
                    ),
                }
            ],
        ),
        endpoint=BipadEndpoint.RIVER,
        run_id="integration-run",
        captured_at=captured_at,
        page_index=1,
    )

    ingestion_result = IngestionRunResult(
        run_id="integration-run",
        endpoint=BipadEndpoint.RIVER,
        started_at=captured_at,
        finished_at=(
            captured_at
            + timedelta(
                seconds=1,
            )
        ),
        duration_ms=1000,
        metrics=IngestionMetrics(
            records_received=2,
            records_valid=2,
            records_invalid=0,
            stations_emitted=0,
            observations_emitted=2,
            records_without_observation=0,
        ),
        quarantined_records=(),
    )

    manifest_result = (
        manifest_writer.write_manifest(
            run_result=ingestion_result,
            captured_at=captured_at,
            raw_pages=[
                page_zero,
                page_one,
            ],
            quarantine_objects=[],
        )
    )

    manifest_path = (
        tmp_path
        / manifest_result.object.key
    )

    processing_result = (
        process_manifest(
            spark=spark,
            lake_root=tmp_path,
            manifest_path=manifest_path,
        )
    )

    assert (
        processing_result.run_id
        == "integration-run"
    )

    assert (
        processing_result.endpoint
        is BipadEndpoint.RIVER
    )

    assert (
        len(
            processing_result.outputs
        )
        == 1
    )

    output = (
        processing_result.outputs[0]
    )

    assert (
        output.dataset
        is ProcessedDataset.OBSERVATIONS
    )

    assert output.row_count == 2

    processed = spark.read.parquet(
        str(output.path)
    )

    assert processed.count() == 2

    assert {
        row["source_record_id"]
        for row in processed.collect()
    } == {
        "100",
        "101",
    }

    assert {
        row["run_id"]
        for row in processed.collect()
    } == {
        "integration-run",
    }

    assert (
        processing_result
        .quality_report_path
        .exists()
    )

    report = json.loads(
        processing_result
        .quality_report_path
        .read_text(
            encoding="utf-8"
        )
    )

    assert (
        report["report_version"]
        == 1
    )

    assert (
        report["run_id"]
        == processing_result.run_id
    )

    assert (
        report["endpoint"]
        == processing_result.endpoint.value
    )

    assert (
        report["observation_summary"]
        ["total_rows"]
        > 0
    )

    assert (
        report["observation_summary"]
        ["pass_rows"]
        + report["observation_summary"]
        ["warn_rows"]
        + report["observation_summary"]
        ["fail_rows"]
        == report["observation_summary"]
        ["total_rows"]
    )

    assert (
        report["station_summary"]
        is None
    )

    assert (
        report["current_data_health"]
        is None
    )

    for output in processing_result.outputs:
        assert (
            processing_result.run_id
            in str(
                output.path
            )
        )

    assert (
        processing_result.run_id
        in str(
            processing_result
            .quality_report_path
        )
    )