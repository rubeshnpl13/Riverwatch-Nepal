from __future__ import annotations

import json
from datetime import (
    UTC,
    datetime,
)
from pathlib import Path

import pytest
from pyspark.sql import (
    DataFrame,
    SparkSession,
)

import riverwatch.cloud.processing_pipeline as pipeline
from riverwatch.cloud.processing_output import (
    CloudProcessedOutput,
    GcsProcessedOutputTarget,
    build_gcs_processed_output_target,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.models import (
    ProcessedDataset,
    ProcessingInputPage,
    ProcessingInputRun,
)
from riverwatch.storage.errors import (
    ObjectAlreadyExistsError,
    StorageError,
)
from riverwatch.storage.models import (
    StoredObject,
)
from riverwatch.storage.serialization import (
    serialize_json,
    sha256_hex,
)

CAPTURED_AT = datetime(
    2026,
    10,
    5,
    12,
    0,
    tzinfo=UTC,
)


class FakeCloudStore:
    def __init__(
        self,
    ) -> None:
        self.bucket_name = (
            "riverwatch-dev-lake"
        )

        self.objects: dict[
            str,
            bytes,
        ] = {}

    def object_exists(
        self,
        *,
        key: str,
    ) -> bool:
        return key in self.objects

    def prefix_exists(
        self,
        *,
        prefix: str,
    ) -> bool:
        normalized = (
            f"{prefix.rstrip('/')}/"
        )

        return any(
            key.startswith(
                normalized
            )
            for key in self.objects
        )

    def read_bytes(
        self,
        *,
        key: str,
    ) -> bytes:
        try:
            return self.objects[
                key
            ]

        except KeyError as exc:
            raise StorageError(
                f"missing: {key}"
            ) from exc

    def create_bytes(
        self,
        *,
        key: str,
        data: bytes,
        content_type: str,
    ) -> StoredObject:
        if key in self.objects:
            raise ObjectAlreadyExistsError(
                f"Object already exists: {key}"
            )

        self.objects[
            key
        ] = data

        return StoredObject(
            key=key,
            size_bytes=len(data),
            content_type=content_type,
            sha256=sha256_hex(
                data
            ),
        )


def make_processing_input(
    *,
    tmp_path: Path,
    endpoint: BipadEndpoint,
    record: dict[str, object],
) -> ProcessingInputRun:
    payload = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            record,
        ],
    }

    payload_data = serialize_json(
        payload
    )

    payload_path = (
        tmp_path
        / "payload.json"
    )

    payload_path.write_bytes(
        payload_data
    )

    return ProcessingInputRun(
        run_id="cloud-run-123",
        endpoint=endpoint,
        captured_at=CAPTURED_AT,
        manifest_path=(
            tmp_path
            / "manifest.json"
        ),
        pages=(
            ProcessingInputPage(
                page_index=0,
                record_count=1,
                payload_key=(
                    "raw/payload.json"
                ),
                payload_path=(
                    payload_path
                ),
                payload_sha256=(
                    sha256_hex(
                        payload_data
                    )
                ),
            ),
        ),
    )


def install_fake_output_writer(
    *,
    monkeypatch: pytest.MonkeyPatch,
    store: FakeCloudStore,
) -> list[
    GcsProcessedOutputTarget
]:
    written_targets: list[
        GcsProcessedOutputTarget
    ] = []

    def fake_write(
        *,
        spark: object,
        dataframe: DataFrame,
        store: object,
        target: GcsProcessedOutputTarget,
    ) -> CloudProcessedOutput:
        _ = (
            spark,
            store,
        )

        written_targets.append(
            target
        )

        return CloudProcessedOutput(
            dataset=target.dataset,
            uri=target.uri,
            row_count=(
                dataframe.count()
            ),
        )

    monkeypatch.setattr(
        pipeline,
        "write_or_recover_gcs_processed_output",
        fake_write,
    )

    return written_targets


def test_processes_historical_river(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processing_input = (
        make_processing_input(
            tmp_path=tmp_path,
            endpoint=BipadEndpoint.RIVER,
            record={
                "id": 100,
                "station": 44,
                "stationSeriesId": 500,
                "title": "Test River",
                "basin": "Gandaki",
                "waterLevel": 1.25,
                "waterLevelOn": (
                    "2026-10-05T11:00:00Z"
                ),
            },
        )
    )

    store = FakeCloudStore()

    written_targets = (
        install_fake_output_writer(
            monkeypatch=monkeypatch,
            store=store,
        )
    )

    result = (
        pipeline
        .process_gcs_processing_input(
            spark=spark,
            store=store,
            processing_input=(
                processing_input
            ),
        )
    )

    assert result.run_id == (
        "cloud-run-123"
    )

    assert result.endpoint is (
        BipadEndpoint.RIVER
    )

    assert len(
        result.outputs
    ) == 1

    assert (
        result.outputs[0].dataset
        is ProcessedDataset.OBSERVATIONS
    )

    assert (
        result.outputs[0].row_count
        == 1
    )

    assert len(
        written_targets
    ) == 1

    assert result.quality_report_key in (
        store.objects
    )

    report = json.loads(
        store.objects[
            result.quality_report_key
        ]
    )

    assert report[
        "run_id"
    ] == "cloud-run-123"

    assert report[
        "station_summary"
    ] is None

    assert (
        report[
            "observation_summary"
        ]
        is not None
    )


def test_processes_current_station_run(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processing_input = (
        make_processing_input(
            tmp_path=tmp_path,
            endpoint=(
                BipadEndpoint
                .RIVER_STATIONS
            ),
            record={
                "id": 282,
                "seriesId": 36640,
                "title": (
                    "Banara River "
                    "at EW Highway"
                ),
                "basin": "Mahakali",
                "longitude": 80.3954179,
                "latitude": 28.8886706,
                "waterLevel": 1.2,
                "waterLevelOn": (
                    "2026-10-05T11:00:00Z"
                ),
            },
        )
    )

    store = FakeCloudStore()

    written_targets = (
        install_fake_output_writer(
            monkeypatch=monkeypatch,
            store=store,
        )
    )

    result = (
        pipeline
        .process_gcs_processing_input(
            spark=spark,
            store=store,
            processing_input=(
                processing_input
            ),
        )
    )

    assert result.endpoint is (
        BipadEndpoint
        .RIVER_STATIONS
    )

    assert {
        output.dataset
        for output in result.outputs
    } == {
        ProcessedDataset.STATIONS,
        ProcessedDataset.OBSERVATIONS,
    }

    assert len(
        written_targets
    ) == 2

    assert all(
        output.row_count == 1
        for output in result.outputs
    )

    report = json.loads(
        store.objects[
            result.quality_report_key
        ]
    )

    assert (
        report[
            "station_summary"
        ]
        is not None
    )

    assert (
        report[
            "observation_summary"
        ]
        is not None
    )

    assert (
        report[
            "current_data_health"
        ]
        is not None
    )


def test_preflight_rejects_incomplete_output_before_write(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processing_input = (
        make_processing_input(
            tmp_path=tmp_path,
            endpoint=(
                BipadEndpoint
                .RIVER_STATIONS
            ),
            record={
                "id": 282,
                "seriesId": 36640,
                "title": "Test River",
                "basin": "Mahakali",
                "longitude": 80.0,
                "latitude": 28.0,
                "waterLevel": 1.2,
                "waterLevelOn": (
                    "2026-10-05T11:00:00Z"
                ),
            },
        )
    )

    store = FakeCloudStore()

    observation_target = (
        build_gcs_processed_output_target(
            bucket_name=(
                store.bucket_name
            ),
            dataset=(
                ProcessedDataset
                .OBSERVATIONS
            ),
            processing_input=(
                processing_input
            ),
        )
    )

    store.objects[
        (
            f"{observation_target.key}/"
            "part-00000.parquet"
        )
    ] = b"partial"

    written_targets = (
        install_fake_output_writer(
            monkeypatch=monkeypatch,
            store=store,
        )
    )

    with pytest.raises(
        pipeline.CloudProcessingPipelineError,
        match=(
            "without a Spark "
            "success marker"
        ),
    ):
        pipeline.process_gcs_processing_input(
            spark=spark,
            store=store,
            processing_input=(
                processing_input
            ),
        )

    assert written_targets == []