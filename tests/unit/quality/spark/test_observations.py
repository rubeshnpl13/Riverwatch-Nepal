from datetime import (
    UTC,
    datetime,
)

from pyspark.sql import SparkSession

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.quality.model import (
    ObservationQualityRule,
)
from riverwatch.quality.spark.observations import (
    evaluate_observation_quality,
)


def test_valid_observation_passes(
    spark: SparkSession,
) -> None:
    observations = spark.createDataFrame(
        [
            {
                "source_record_id": "100",
                "station_id": "44",
                "latitude": 27.7,
                "longitude": 85.3,
                "observed_at": datetime(
                    2026,
                    9,
                    29,
                    6,
                    30,
                    tzinfo=UTC,
                ),
                "water_level_m": 1.25,
                "warning_level_m": 3.0,
                "danger_level_m": 4.0,
                "ingested_at": datetime(
                    2026,
                    9,
                    29,
                    7,
                    0,
                    tzinfo=UTC,
                ),
                "run_id": "run-123",
            }
        ]
    )

    checked = (
        evaluate_observation_quality(
            observations=observations,
            endpoint=BipadEndpoint.RIVER,
        )
    )

    row = checked.collect()[0]

    assert row["quality_status"] == "pass"
    assert row["quality_errors"] == []
    assert row["quality_warnings"] == []
    assert row["quality_error_count"] == 0
    assert row["quality_warning_count"] == 0


def test_invalid_observation_is_flagged(
    spark: SparkSession,
) -> None:
    observations = spark.createDataFrame(
        [
            {
                "source_record_id": "bad",
                "station_id": None,
                "latitude": 95.0,
                "longitude": 200.0,
                "observed_at": None,
                "water_level_m": -99991.53125,
                "warning_level_m": 5.0,
                "danger_level_m": 4.0,
                "ingested_at": datetime(
                    2026,
                    9,
                    29,
                    7,
                    0,
                    tzinfo=UTC,
                ),
                "run_id": "run-123",
            },
            {
                "source_record_id": "type-helper",
                "station_id": "44",
                "latitude": 27.7,
                "longitude": 85.3,
                "observed_at": datetime(
                    2026,
                    9,
                    29,
                    6,
                    0,
                    tzinfo=UTC,
                ),
                "water_level_m": 1.0,
                "warning_level_m": 3.0,
                "danger_level_m": 4.0,
                "ingested_at": datetime(
                    2026,
                    9,
                    29,
                    7,
                    0,
                    tzinfo=UTC,
                ),
                "run_id": "run-123",
            },
        ]
    )

    checked = evaluate_observation_quality(
        observations=observations,
        endpoint=BipadEndpoint.RIVER,
    )

    row = (
        checked
        .where(
            "source_record_id = 'bad'"
        )
        .collect()[0]
    )

    assert (
        ObservationQualityRule.MISSING_STATION_ID.value
        in row["quality_errors"]
    )

    assert (
        ObservationQualityRule.MISSING_OBSERVED_AT.value
        in row["quality_errors"]
    )

    assert (
        ObservationQualityRule.INVALID_LATITUDE.value
        in row["quality_errors"]
    )

    assert (
        ObservationQualityRule.INVALID_LONGITUDE.value
        in row["quality_errors"]
    )

    assert (
        ObservationQualityRule.SUSPICIOUS_WATER_LEVEL.value
        in row["quality_warnings"]
    )

    assert (
        ObservationQualityRule.WARNING_ABOVE_DANGER.value
        in row["quality_warnings"]
    )

    assert row["quality_status"] == "fail"


def test_stale_current_observation_warns(
    spark: SparkSession,
) -> None:
    observations = spark.createDataFrame(
        [
            {
                "source_record_id": "100",
                "station_id": "44",
                "latitude": 27.7,
                "longitude": 85.3,
                "observed_at": datetime(
                    2026,
                    9,
                    27,
                    6,
                    0,
                    tzinfo=UTC,
                ),
                "water_level_m": 1.25,
                "warning_level_m": 3.0,
                "danger_level_m": 4.0,
                "ingested_at": datetime(
                    2026,
                    9,
                    29,
                    7,
                    0,
                    tzinfo=UTC,
                ),
                "run_id": "run-123",
            }
        ]
    )

    checked = (
        evaluate_observation_quality(
            observations=observations,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
        )
    )

    row = checked.collect()[0]

    assert row["quality_status"] == "warn"

    assert (
        ObservationQualityRule
        .STALE_CURRENT_OBSERVATION
        .value
        in row["quality_warnings"]
    )


def test_historical_observation_is_not_checked_for_freshness(
    spark: SparkSession,
) -> None:
    observations = spark.createDataFrame(
        [
            {
                "source_record_id": "100",
                "station_id": "44",
                "latitude": 27.7,
                "longitude": 85.3,
                "observed_at": datetime(
                    2020,
                    8,
                    19,
                    19,
                    0,
                    tzinfo=UTC,
                ),
                "water_level_m": 1.25,
                "warning_level_m": 3.0,
                "danger_level_m": 4.0,
                "ingested_at": datetime(
                    2026,
                    9,
                    29,
                    7,
                    0,
                    tzinfo=UTC,
                ),
                "run_id": "run-123",
            }
        ]
    )

    checked = (
        evaluate_observation_quality(
            observations=observations,
            endpoint=BipadEndpoint.RIVER,
        )
    )

    row = checked.collect()[0]

    assert (
        ObservationQualityRule
        .STALE_CURRENT_OBSERVATION
        .value
        not in row["quality_warnings"]
    )

    assert row["quality_status"] == "pass"

def test_duplicate_source_record_id_fails(
    spark: SparkSession,
) -> None:
    observations = spark.createDataFrame(
        [
            {
                "source_record_id": "100",
                "station_id": "44",
                "latitude": 27.7,
                "longitude": 85.3,
                "observed_at": datetime(
                    2026,
                    9,
                    29,
                    6,
                    0,
                    tzinfo=UTC,
                ),
                "water_level_m": 1.0,
                "warning_level_m": 3.0,
                "danger_level_m": 4.0,
                "ingested_at": datetime(
                    2026,
                    9,
                    29,
                    7,
                    0,
                    tzinfo=UTC,
                ),
                "run_id": "run-123",
            },
            {
                "source_record_id": "100",
                "station_id": "45",
                "latitude": 27.8,
                "longitude": 85.4,
                "observed_at": datetime(
                    2026,
                    9,
                    29,
                    6,
                    15,
                    tzinfo=UTC,
                ),
                "water_level_m": 2.0,
                "warning_level_m": 3.0,
                "danger_level_m": 4.0,
                "ingested_at": datetime(
                    2026,
                    9,
                    29,
                    7,
                    0,
                    tzinfo=UTC,
                ),
                "run_id": "run-123",
            },
        ]
    )

    checked = (
        evaluate_observation_quality(
            observations=observations,
            endpoint=BipadEndpoint.RIVER,
        )
    )

    rows = checked.collect()

    for row in rows:
        assert (
            row["quality_status"]
            == "fail"
        )

        assert (
            ObservationQualityRule
            .DUPLICATE_SOURCE_RECORD_ID
            .value
            in row["quality_errors"]
        )

#observation duplicate test

def test_duplicate_station_timestamp_warns(
    spark: SparkSession,
) -> None:
    observed_at = datetime(
        2026,
        9,
        29,
        6,
        0,
        tzinfo=UTC,
    )

    observations = spark.createDataFrame(
        [
            {
                "source_record_id": "100",
                "station_id": "44",
                "latitude": 27.7,
                "longitude": 85.3,
                "observed_at": observed_at,
                "water_level_m": 1.0,
                "warning_level_m": 3.0,
                "danger_level_m": 4.0,
                "ingested_at": datetime(
                    2026,
                    9,
                    29,
                    7,
                    0,
                    tzinfo=UTC,
                ),
                "run_id": "run-123",
            },
            {
                "source_record_id": "101",
                "station_id": "44",
                "latitude": 27.7,
                "longitude": 85.3,
                "observed_at": observed_at,
                "water_level_m": 1.1,
                "warning_level_m": 3.0,
                "danger_level_m": 4.0,
                "ingested_at": datetime(
                    2026,
                    9,
                    29,
                    7,
                    0,
                    tzinfo=UTC,
                ),
                "run_id": "run-123",
            },
        ]
    )

    checked = (
        evaluate_observation_quality(
            observations=observations,
            endpoint=BipadEndpoint.RIVER,
        )
    )

    rows = checked.collect()

    for row in rows:
        assert (
            row["quality_status"]
            == "warn"
        )

        assert (
            ObservationQualityRule
            .DUPLICATE_STATION_OBSERVED_AT
            .value
            in row["quality_warnings"]
        )

def test_same_station_different_timestamps_is_not_duplicate(
    spark: SparkSession,
) -> None:
    observations = spark.createDataFrame(
        [
            {
                "source_record_id": "100",
                "station_id": "44",
                "latitude": 27.7,
                "longitude": 85.3,
                "observed_at": datetime(
                    2026,
                    9,
                    29,
                    6,
                    0,
                    tzinfo=UTC,
                ),
                "water_level_m": 1.0,
                "warning_level_m": 3.0,
                "danger_level_m": 4.0,
                "ingested_at": datetime(
                    2026,
                    9,
                    29,
                    7,
                    0,
                    tzinfo=UTC,
                ),
                "run_id": "run-123",
            },
            {
                "source_record_id": "101",
                "station_id": "44",
                "latitude": 27.7,
                "longitude": 85.3,
                "observed_at": datetime(
                    2026,
                    9,
                    29,
                    6,
                    15,
                    tzinfo=UTC,
                ),
                "water_level_m": 1.1,
                "warning_level_m": 3.0,
                "danger_level_m": 4.0,
                "ingested_at": datetime(
                    2026,
                    9,
                    29,
                    7,
                    0,
                    tzinfo=UTC,
                ),
                "run_id": "run-123",
            },
        ]
    )

    checked = (
        evaluate_observation_quality(
            observations=observations,
            endpoint=BipadEndpoint.RIVER,
        )
    )

    rows = checked.collect()

    for row in rows:
        assert (
            ObservationQualityRule
            .DUPLICATE_SOURCE_RECORD_ID
            .value
            not in row["quality_errors"]
        )

        assert (
            ObservationQualityRule
            .DUPLICATE_STATION_OBSERVED_AT
            .value
            not in row["quality_warnings"]
        )