from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from riverwatch.quality.model import (
    StationQualityRule,
)
from riverwatch.quality.spark.stations import (
    evaluate_station_quality,
)

STATION_TEST_SCHEMA = StructType(
    [
        StructField(
            "station_id",
            StringType(),
            True,
        ),
        StructField(
            "station_series_id",
            StringType(),
            True,
        ),
        StructField(
            "station_name",
            StringType(),
            True,
        ),
        StructField(
            "basin_name",
            StringType(),
            True,
        ),
        StructField(
            "latitude",
            DoubleType(),
            True,
        ),
        StructField(
            "longitude",
            DoubleType(),
            True,
        ),
        StructField(
            "run_id",
            StringType(),
            True,
        ),
    ]
)


def test_valid_station_passes(
    spark: SparkSession,
) -> None:
    stations = spark.createDataFrame(
        [
            (
                "282",
                "500",
                "Banara River",
                "Mahakali",
                28.88,
                80.39,
                "run-123",
            ),
        ],
        schema=STATION_TEST_SCHEMA,
    )

    checked = evaluate_station_quality(
        stations=stations
    )

    row = checked.collect()[0]

    assert row["quality_status"] == "pass"
    assert row["quality_errors"] == []
    assert row["quality_warnings"] == []

    assert (
        row["quality_error_count"]
        == 0
    )

    assert (
        row["quality_warning_count"]
        == 0
    )


def test_invalid_station_fails(
    spark: SparkSession,
) -> None:
    stations = spark.createDataFrame(
        [
            (
                None,
                "500",
                " ",
                "Mahakali",
                95.0,
                200.0,
                None,
            ),
        ],
        schema=STATION_TEST_SCHEMA,
    )

    checked = evaluate_station_quality(
        stations=stations
    )

    row = checked.collect()[0]

    assert row["quality_status"] == "fail"

    assert set(
        row["quality_errors"]
    ) == {
        (
            StationQualityRule
            .MISSING_STATION_ID
            .value
        ),
        (
            StationQualityRule
            .MISSING_STATION_NAME
            .value
        ),
        (
            StationQualityRule
            .MISSING_RUN_ID
            .value
        ),
        (
            StationQualityRule
            .INVALID_LATITUDE
            .value
        ),
        (
            StationQualityRule
            .INVALID_LONGITUDE
            .value
        ),
    }


def test_incomplete_station_warns(
    spark: SparkSession,
) -> None:
    stations = spark.createDataFrame(
        [
            (
                "162",
                None,
                "Marin River",
                "",
                None,
                None,
                "run-123",
            ),
        ],
        schema=STATION_TEST_SCHEMA,
    )

    checked = evaluate_station_quality(
        stations=stations
    )

    row = checked.collect()[0]

    assert row["quality_status"] == "warn"

    assert row["quality_errors"] == []

    assert set(
        row["quality_warnings"]
    ) == {
        (
            StationQualityRule
            .MISSING_STATION_SERIES_ID
            .value
        ),
        (
            StationQualityRule
            .MISSING_BASIN_NAME
            .value
        ),
        (
            StationQualityRule
            .MISSING_COORDINATES
            .value
        ),
    }

#station duplicate tests
def test_duplicate_station_id_fails(
    spark: SparkSession,
) -> None:
    stations = spark.createDataFrame(
        [
            (
                "44",
                "500",
                "Station A",
                "Koshi",
                27.7,
                85.3,
                "run-123",
            ),
            (
                "44",
                "501",
                "Station A Duplicate",
                "Koshi",
                27.8,
                85.4,
                "run-123",
            ),
        ],
        schema=STATION_TEST_SCHEMA,
    )

    checked = evaluate_station_quality(
        stations=stations
    )

    rows = checked.collect()

    assert len(rows) == 2

    for row in rows:
        assert (
            row["quality_status"]
            == "fail"
        )

        assert (
            StationQualityRule
            .DUPLICATE_STATION_ID
            .value
            in row["quality_errors"]
        )


def test_duplicate_station_series_id_warns(
    spark: SparkSession,
) -> None:
    stations = spark.createDataFrame(
        [
            (
                "44",
                "500",
                "Station A",
                "Koshi",
                27.7,
                85.3,
                "run-123",
            ),
            (
                "45",
                "500",
                "Station B",
                "Koshi",
                27.8,
                85.4,
                "run-123",
            ),
        ],
        schema=STATION_TEST_SCHEMA,
    )

    checked = evaluate_station_quality(
        stations=stations
    )

    rows = checked.collect()

    for row in rows:
        assert (
            row["quality_status"]
            == "warn"
        )

        assert (
            StationQualityRule
            .DUPLICATE_STATION_SERIES_ID
            .value
            in row["quality_warnings"]
        )