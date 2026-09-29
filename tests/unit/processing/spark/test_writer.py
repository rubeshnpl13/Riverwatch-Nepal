from datetime import (
    UTC,
    datetime,
)
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.errors import (
    ProcessedOutputAlreadyExistsError,
)
from riverwatch.processing.models import (
    ProcessedDataset,
    ProcessingInputRun,
)
from riverwatch.processing.spark.writer import (
    ensure_processed_outputs_absent,
    write_processed_parquet,
)

CAPTURED_AT = datetime(
    2026,
    9,
    29,
    3,
    0,
    tzinfo=UTC,
)


def create_processing_input(
    tmp_path: Path,
) -> ProcessingInputRun:
    return ProcessingInputRun(
        run_id="processed-test",
        endpoint=BipadEndpoint.RIVER,
        captured_at=CAPTURED_AT,
        manifest_path=(
            tmp_path
            / "manifest.json"
        ),
        pages=(),
    )


def test_writes_processed_parquet(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "100",
                "44",
                1.25,
            ),
            (
                "101",
                "45",
                2.50,
            ),
        ],
        [
            "source_record_id",
            "station_id",
            "water_level_m",
        ],
    )

    processing_input = (
        create_processing_input(
            tmp_path
        )
    )

    output_path = (
        write_processed_parquet(
            dataframe=dataframe,
            lake_root=tmp_path,
            dataset=(
                ProcessedDataset.OBSERVATIONS
            ),
            processing_input=(
                processing_input
            ),
        )
    )

    assert output_path.exists()

    parquet_files = list(
        output_path.glob(
            "*.parquet"
        )
    )

    assert parquet_files

    restored = (
        spark.read.parquet(
            str(output_path)
        )
    )

    assert restored.count() == 2

    assert {
        row["source_record_id"]
        for row in restored.collect()
    } == {
        "100",
        "101",
    }


def test_processed_output_is_immutable(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "100",
                "44",
            ),
        ],
        [
            "source_record_id",
            "station_id",
        ],
    )

    processing_input = (
        create_processing_input(
            tmp_path
        )
    )

    write_processed_parquet(
        dataframe=dataframe,
        lake_root=tmp_path,
        dataset=(
            ProcessedDataset.OBSERVATIONS
        ),
        processing_input=processing_input,
    )

    with pytest.raises(
        ProcessedOutputAlreadyExistsError,
        match="already exists",
    ):
        write_processed_parquet(
            dataframe=dataframe,
            lake_root=tmp_path,
            dataset=(
                ProcessedDataset.OBSERVATIONS
            ),
            processing_input=(
                processing_input
            ),
        )

#test - station and observation path don't collide

def test_different_datasets_use_different_paths(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    dataframe = spark.createDataFrame(
        [
            ("44",),
        ],
        [
            "station_id",
        ],
    )

    processing_input = (
        ProcessingInputRun(
            run_id="station-run",
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
            captured_at=CAPTURED_AT,
            manifest_path=(
                tmp_path
                / "manifest.json"
            ),
            pages=(),
        )
    )

    station_path = (
        write_processed_parquet(
            dataframe=dataframe,
            lake_root=tmp_path,
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
            dataframe=dataframe,
            lake_root=tmp_path,
            dataset=(
                ProcessedDataset.OBSERVATIONS
            ),
            processing_input=(
                processing_input
            ),
        )
    )

    assert (
        station_path
        != observation_path
    )

    assert (
        "dataset=stations"
        in str(station_path)
    )

    assert (
        "dataset=observations"
        in str(observation_path)
    )

def test_preflight_detects_existing_output(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    dataframe = spark.createDataFrame(
        [
            ("44",),
        ],
        [
            "station_id",
        ],
    )

    processing_input = (
        ProcessingInputRun(
            run_id="preflight-run",
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
            captured_at=CAPTURED_AT,
            manifest_path=(
                tmp_path
                / "manifest.json"
            ),
            pages=(),
        )
    )

    write_processed_parquet(
        dataframe=dataframe,
        lake_root=tmp_path,
        dataset=(
            ProcessedDataset.STATIONS
        ),
        processing_input=(
            processing_input
        ),
    )

    with pytest.raises(
        ProcessedOutputAlreadyExistsError,
        match="already exists",
    ):
        ensure_processed_outputs_absent(
            lake_root=tmp_path,
            datasets=[
                ProcessedDataset.STATIONS,
                ProcessedDataset.OBSERVATIONS,
            ],
            processing_input=(
                processing_input
            ),
        )