from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

from riverwatch.quality.model import (
    QualityDataset,
    QualityStatus,
)
from riverwatch.quality.spark.summary import (
    summarize_dataset_quality,
)

QUALITY_SCHEMA = StructType(
    [
        StructField(
            "quality_status",
            StringType(),
            False,
        ),
        StructField(
            "quality_errors",
            ArrayType(
                StringType(),
                containsNull=False,
            ),
            False,
        ),
        StructField(
            "quality_warnings",
            ArrayType(
                StringType(),
                containsNull=False,
            ),
            False,
        ),
    ]
)


def test_all_pass_dataset_summary(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "pass",
                [],
                [],
            ),
            (
                "pass",
                [],
                [],
            ),
        ],
        schema=QUALITY_SCHEMA,
    )

    summary = (
        summarize_dataset_quality(
            dataframe=dataframe,
            dataset=(
                QualityDataset.OBSERVATIONS
            ),
        )
    )

    assert summary.total_rows == 2
    assert summary.pass_rows == 2
    assert summary.warn_rows == 0
    assert summary.fail_rows == 0
    assert summary.affected_rows == 0
    assert summary.pass_ratio == 1.0

    assert (
        summary.status
        is QualityStatus.PASS
    )

    assert (
        summary.error_rule_counts
        == ()
    )

    assert (
        summary.warning_rule_counts
        == ()
    )


def test_mixed_dataset_uses_worst_status(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "pass",
                [],
                [],
            ),
            (
                "warn",
                [],
                [
                    "stale_current_observation",
                ],
            ),
            (
                "fail",
                [
                    "missing_station_id",
                ],
                [],
            ),
        ],
        schema=QUALITY_SCHEMA,
    )

    summary = (
        summarize_dataset_quality(
            dataframe=dataframe,
            dataset=(
                QualityDataset.OBSERVATIONS
            ),
        )
    )

    assert summary.total_rows == 3
    assert summary.pass_rows == 1
    assert summary.warn_rows == 1
    assert summary.fail_rows == 1

    assert (
        summary.affected_rows
        == 2
    )

    assert (
        summary.pass_ratio
        == 1 / 3
    )

    assert (
        summary.status
        is QualityStatus.FAIL
    )


def test_counts_rules_across_rows(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "warn",
                [],
                [
                    "stale_current_observation",
                    "missing_water_level",
                ],
            ),
            (
                "warn",
                [],
                [
                    "stale_current_observation",
                ],
            ),
            (
                "fail",
                [
                    "missing_station_id",
                ],
                [
                    "missing_water_level",
                ],
            ),
        ],
        schema=QUALITY_SCHEMA,
    )

    summary = (
        summarize_dataset_quality(
            dataframe=dataframe,
            dataset=(
                QualityDataset.OBSERVATIONS
            ),
        )
    )

    assert {
        item.rule: item.count
        for item in (
            summary.warning_rule_counts
        )
    } == {
        "missing_water_level": 2,
        "stale_current_observation": 2,
    }

    assert {
        item.rule: item.count
        for item in (
            summary.error_rule_counts
        )
    } == {
        "missing_station_id": 1,
    }


def test_warn_only_dataset_warns(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "pass",
                [],
                [],
            ),
            (
                "warn",
                [],
                [
                    "missing_basin_name",
                ],
            ),
        ],
        schema=QUALITY_SCHEMA,
    )

    summary = (
        summarize_dataset_quality(
            dataframe=dataframe,
            dataset=(
                QualityDataset.STATIONS
            ),
        )
    )

    assert (
        summary.status
        is QualityStatus.WARN
    )

    assert summary.pass_rows == 1
    assert summary.warn_rows == 1
    assert summary.fail_rows == 0


def test_empty_dataset_fails(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [],
        schema=QUALITY_SCHEMA,
    )

    summary = (
        summarize_dataset_quality(
            dataframe=dataframe,
            dataset=(
                QualityDataset.OBSERVATIONS
            ),
        )
    )

    assert summary.total_rows == 0

    assert (
        summary.pass_ratio
        == 0.0
    )

    assert (
        summary.status
        is QualityStatus.FAIL
    )