from pathlib import Path
from uuid import UUID

import pytest
from pyspark.sql import SparkSession

from riverwatch.processing.spark.event_runner import (
    SparkProcessingEventRunner,
    UnsupportedProcessingRequestError,
)

IDEMPOTENCY_KEY = UUID(
    "11111111-1111-1111-1111-111111111111"
)


def test_processing_runner_rejects_provider(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    runner = SparkProcessingEventRunner(
        spark=spark,
        lake_root=tmp_path,
    )

    with pytest.raises(
        UnsupportedProcessingRequestError,
        match="Unsupported processing provider",
    ):
        runner.run(
            provider="other",
            endpoint="river-stations",
            run_id="run-123",
            manifest_path="manifest.json",
            idempotency_key=(
                IDEMPOTENCY_KEY
            ),
        )


def test_processing_runner_rejects_endpoint(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    runner = SparkProcessingEventRunner(
        spark=spark,
        lake_root=tmp_path,
    )

    with pytest.raises(
        UnsupportedProcessingRequestError,
        match="Unsupported processing endpoint",
    ):
        runner.run(
            provider="bipad",
            endpoint="unknown",
            run_id="run-123",
            manifest_path="manifest.json",
            idempotency_key=(
                IDEMPOTENCY_KEY
            ),
        )


def test_processing_runner_rejects_absolute_manifest_path(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    runner = SparkProcessingEventRunner(
        spark=spark,
        lake_root=tmp_path,
    )

    with pytest.raises(
        ValueError,
        match="manifest_path must be relative",
    ):
        runner.run(
            provider="bipad",
            endpoint="river-stations",
            run_id="run-123",
            manifest_path="/tmp/manifest.json",
            idempotency_key=(
                IDEMPOTENCY_KEY
            ),
        )


def test_processing_runner_rejects_path_escape(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    runner = SparkProcessingEventRunner(
        spark=spark,
        lake_root=tmp_path,
    )

    with pytest.raises(
        ValueError,
        match=(
            "manifest_path escapes "
            "the lake root"
        ),
    ):
        runner.run(
            provider="bipad",
            endpoint="river-stations",
            run_id="run-123",
            manifest_path=(
                "../manifest.json"
            ),
            idempotency_key=(
                IDEMPOTENCY_KEY
            ),
        )