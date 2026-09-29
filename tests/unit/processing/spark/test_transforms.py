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
from riverwatch.processing.models import (
    ProcessingInputPage,
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
from riverwatch.storage.serialization import (
    serialize_json,
    sha256_hex,
)

CAPTURED_AT = datetime(
    2026,
    9,
    29,
    7,
    0,
    tzinfo=UTC,
)


def create_processing_input(
    *,
    tmp_path: Path,
    endpoint: BipadEndpoint,
    payload: dict[str, object],
    record_count: int,
) -> ProcessingInputRun:
    payload_path = (
        tmp_path
        / "raw"
        / "provider=bipad"
        / f"endpoint={endpoint.value}"
        / "page=000000"
        / "payload.json"
    )

    payload_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    data = serialize_json(
        payload
    )

    payload_path.write_bytes(
        data
    )

    return ProcessingInputRun(
        run_id="transform-test",
        endpoint=endpoint,
        captured_at=CAPTURED_AT,
        manifest_path=(
            tmp_path
            / "manifest.json"
        ),
        pages=(
            ProcessingInputPage(
                page_index=0,
                record_count=record_count,
                payload_key=str(
                    payload_path.relative_to(
                        tmp_path
                    )
                ),
                payload_path=payload_path,
                payload_sha256=(
                    sha256_hex(data)
                ),
            ),
        ),
    )


def test_transforms_historical_observation(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    payload: dict[str, object] = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": 100,
                "station": 44,
                "stationSeriesId": 19260,
                "title": "Test Station",
                "basin": "Gandaki",
                "point": {
                    "type": "Point",
                    "coordinates": [
                        85.25,
                        27.70,
                    ],
                },
                "waterLevel": 1.25,
                "warningLevel": 3.0,
                "dangerLevel": 4.0,
                "waterLevelOn": (
                    "2026-09-29T03:00:00Z"
                ),
                "steady": "RISING",
                "status": "NORMAL",
                "createdOn": (
                    "2026-09-29T02:00:00Z"
                ),
                "modifiedOn": (
                    "2026-09-29T03:00:00Z"
                ),
            }
        ],
    }

    processing_input = (
        create_processing_input(
            tmp_path=tmp_path,
            endpoint=BipadEndpoint.RIVER,
            payload=payload,
            record_count=1,
        )
    )

    raw_pages = read_raw_pages(
        spark=spark,
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

    rows = observations.collect()

    assert len(rows) == 1

    row = rows[0]

    assert (
        row["source_record_id"]
        == "100"
    )

    assert row["station_id"] == "44"

    assert (
        row["source_station_series_id"]
        == "19260"
    )

    assert row["latitude"] == 27.70
    assert row["longitude"] == 85.25

    assert (
        row["water_level_m"]
        == 1.25
    )

    assert row["trend"] == "RISING"
    assert row["provider"] == "bipad"
    assert row["original_source"] == "dhm"

    assert (
        row["observed_at"]
        is not None
    )

    assert (
        row["source_page_index"]
        == 0
    )

    assert (
        row["run_id"]
        == "transform-test"
    )


def test_transforms_station_snapshot(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    payload: dict[str, object] = {
        "count": 2,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": 282,
                "stationSeriesId": 500,
                "title": "Banara River",
                "basin": "Mahakali",
                "point": {
                    "type": "Point",
                    "coordinates": [
                        80.1,
                        28.5,
                    ],
                },
                "waterLevel": 1.2,
                "warningLevel": 2.0,
                "dangerLevel": 3.0,
                "waterLevelOn": (
                    "2026-09-29T03:00:00Z"
                ),
                "steady": "STEADY",
                "status": "NORMAL",
                "elevation": 150.0,
            },
            {
                "id": 162,
                "stationSeriesId": 501,
                "title": "No Data Station",
                "basin": "Bagmati",
                "waterLevel": None,
                "waterLevelOn": None,
            },
        ],
    }

    processing_input = (
        create_processing_input(
            tmp_path=tmp_path,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
            payload=payload,
            record_count=2,
        )
    )

    raw_pages = read_raw_pages(
        spark=spark,
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

    assert stations.count() == 2
    assert observations.count() == 1

    station = (
        stations
        .where(
            "station_id = '282'"
        )
        .collect()[0]
    )

    assert (
        station["station_name"]
        == "Banara River"
    )

    assert (
        station["longitude"]
        == 80.1
    )

    assert (
        station["latitude"]
        == 28.5
    )

    observation = (
        observations.collect()[0]
    )

    assert (
        observation["station_id"]
        == "282"
    )

    assert (
        observation["trend"]
        == "STABLE"
    )

    assert (
        observation["observed_at"]
        is not None
    )

#test endpoint protection
def test_rejects_wrong_endpoint_for_historical_transform(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    payload: dict[str, object] = {
        "count": 0,
        "next": None,
        "previous": None,
        "results": [],
    }

    processing_input = (
        create_processing_input(
            tmp_path=tmp_path,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
            payload=payload,
            record_count=0,
        )
    )

    raw_pages = read_raw_pages(
        spark=spark,
        processing_input=processing_input,
    )

    with pytest.raises(
        ValueError,
        match="Unexpected endpoint",
    ):
        transform_historical_observations(
            raw_pages=raw_pages,
            processing_input=(
                processing_input
            ),
        )