from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.models import (
    ProcessingInputPage,
    ProcessingInputRun,
)
from riverwatch.processing.spark.reader import (
    read_raw_pages,
)
from riverwatch.storage.serialization import (
    serialize_json,
    sha256_hex,
)


def test_reads_raw_page_with_explicit_schema(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    payload = {
        "count": 9223372036854775807,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": 10,
                "station": 44,
                "title": "Test River",
                "waterLevel": 1.25,
                "waterLevelOn": (
                    "2026-09-29T03:00:00Z"
                ),
                "point": {
                    "type": "Point",
                    "coordinates": [
                        85.25,
                        27.70,
                    ],
                },
            }
        ],
    }

    data = serialize_json(
        payload
    )

    payload_path = (
        tmp_path
        / "payload.json"
    )

    payload_path.write_bytes(
        data
    )

    processing_input = ProcessingInputRun(
        run_id="spark-test",
        endpoint=BipadEndpoint.RIVER,
        captured_at=datetime(
            2026,
            9,
            29,
            7,
            0,
            tzinfo=UTC,
        ),
        manifest_path=(
            tmp_path
            / "manifest.json"
        ),
        pages=(
            ProcessingInputPage(
                page_index=0,
                record_count=1,
                payload_key="payload.json",
                payload_path=payload_path,
                payload_sha256=(
                    sha256_hex(data)
                ),
            ),
        ),
    )

    dataframe = read_raw_pages(
        spark=spark,
        processing_input=processing_input,
    )

    rows = dataframe.collect()

    assert len(rows) == 1

    assert (
        rows[0]["count"]
        == 9223372036854775807
    )

    assert (
        len(
            rows[0]["results"]
        )
        == 1
    )

    record = rows[0]["results"][0]

    assert record["id"] == 10
    assert record["station"] == 44

    assert (
        record["waterLevel"]
        == 1.25
    )

    assert (
        record["waterLevelOn"]
        == "2026-09-29T03:00:00Z"
    )

    assert (
        record["point"]["coordinates"]
        == [
            85.25,
            27.70,
        ]
    )


def test_reads_multiple_raw_pages(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    pages: list[
        ProcessingInputPage
    ] = []

    for page_index in range(
        2
    ):
        payload = {
            "count": 2,
            "next": None,
            "previous": None,
            "results": [
                {
                    "id": (
                        page_index + 1
                    ),
                    "station": 44,
                    "waterLevel": 1.0,
                }
            ],
        }

        data = serialize_json(
            payload
        )

        path = (
            tmp_path
            / f"page-{page_index}.json"
        )

        path.write_bytes(
            data
        )

        pages.append(
            ProcessingInputPage(
                page_index=page_index,
                record_count=1,
                payload_key=path.name,
                payload_path=path,
                payload_sha256=(
                    sha256_hex(data)
                ),
            )
        )

    processing_input = ProcessingInputRun(
        run_id="multiple-pages",
        endpoint=BipadEndpoint.RIVER,
        captured_at=datetime(
            2026,
            9,
            29,
            7,
            0,
            tzinfo=UTC,
        ),
        manifest_path=(
            tmp_path
            / "manifest.json"
        ),
        pages=tuple(
            pages
        ),
    )

    dataframe = read_raw_pages(
        spark=spark,
        processing_input=processing_input,
    )

    assert dataframe.count() == 2


def test_rejects_processing_run_without_pages(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    processing_input = ProcessingInputRun(
        run_id="empty-run",
        endpoint=BipadEndpoint.RIVER,
        captured_at=datetime(
            2026,
            9,
            29,
            7,
            0,
            tzinfo=UTC,
        ),
        manifest_path=(
            tmp_path
            / "manifest.json"
        ),
        pages=(),
    )

    with pytest.raises(
        ValueError,
        match="no raw pages",
    ):
        read_raw_pages(
            spark=spark,
            processing_input=processing_input,
        )