from datetime import (
    UTC,
    datetime,
)

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from riverwatch.quality.model import (
    QualityStatus,
)
from riverwatch.quality.spark.freshness import (
    summarize_current_data_health,
)

STATION_SCHEMA = StructType(
    [
        StructField(
            "station_id",
            StringType(),
            True,
        ),
    ]
)


OBSERVATION_SCHEMA = StructType(
    [
        StructField(
            "station_id",
            StringType(),
            True,
        ),
        StructField(
            "observed_at",
            TimestampType(),
            True,
        ),
        StructField(
            "ingested_at",
            TimestampType(),
            True,
        ),
    ]
)


INGESTED_AT = datetime(
    2026,
    9,
    29,
    7,
    0,
    tzinfo=UTC,
)


def test_healthy_current_dataset_passes(
    spark: SparkSession,
) -> None:
    stations = spark.createDataFrame(
        [
            ("44",),
            ("45",),
        ],
        schema=STATION_SCHEMA,
    )

    observations = (
        spark.createDataFrame(
            [
                (
                    "44",
                    datetime(
                        2026,
                        9,
                        29,
                        6,
                        30,
                        tzinfo=UTC,
                    ),
                    INGESTED_AT,
                ),
                (
                    "45",
                    datetime(
                        2026,
                        9,
                        29,
                        6,
                        45,
                        tzinfo=UTC,
                    ),
                    INGESTED_AT,
                ),
            ],
            schema=OBSERVATION_SCHEMA,
        )
    )

    summary = (
        summarize_current_data_health(
            stations=stations,
            observations=observations,
        )
    )

    assert summary.total_stations == 2

    assert (
        summary.stations_with_observation
        == 2
    )

    assert (
        summary.stations_without_observation
        == 0
    )

    assert summary.total_observations == 2
    assert summary.fresh_observations == 2
    assert summary.stale_observations == 0
    assert summary.future_observations == 0

    assert (
        summary.unassessable_observations
        == 0
    )

    assert summary.coverage_ratio == 1.0
    assert summary.freshness_ratio == 1.0

    assert (
        summary.status
        is QualityStatus.PASS
    )
    assert summary.oldest_observed_at == datetime(
        2026,
        9,
        29,
        6,
        30,
        tzinfo=UTC,
    )

    assert summary.latest_observed_at == datetime(
        2026,
        9,
        29,
        6,
        45,
        tzinfo=UTC,
    )


def test_incomplete_and_stale_dataset_warns(
    spark: SparkSession,
) -> None:
    stations = spark.createDataFrame(
        [
            ("44",),
            ("45",),
            ("46",),
        ],
        schema=STATION_SCHEMA,
    )

    observations = (
        spark.createDataFrame(
            [
                (
                    "44",
                    datetime(
                        2026,
                        9,
                        29,
                        6,
                        30,
                        tzinfo=UTC,
                    ),
                    INGESTED_AT,
                ),
                (
                    "45",
                    datetime(
                        2026,
                        9,
                        27,
                        6,
                        0,
                        tzinfo=UTC,
                    ),
                    INGESTED_AT,
                ),
            ],
            schema=OBSERVATION_SCHEMA,
        )
    )

    summary = (
        summarize_current_data_health(
            stations=stations,
            observations=observations,
        )
    )

    assert summary.total_stations == 3

    assert (
        summary.stations_with_observation
        == 2
    )

    assert (
        summary.stations_without_observation
        == 1
    )

    assert summary.total_observations == 2
    assert summary.fresh_observations == 1
    assert summary.stale_observations == 1

    assert (
        summary.coverage_ratio
        == 2 / 3
    )

    assert (
        summary.freshness_ratio
        == 0.5
    )

    assert (
        summary.status
        is QualityStatus.WARN
    )


def test_no_current_observations_fails(
    spark: SparkSession,
) -> None:
    stations = spark.createDataFrame(
        [
            ("44",),
            ("45",),
        ],
        schema=STATION_SCHEMA,
    )

    observations = (
        spark.createDataFrame(
            [],
            schema=OBSERVATION_SCHEMA,
        )
    )

    summary = (
        summarize_current_data_health(
            stations=stations,
            observations=observations,
        )
    )

    assert summary.total_stations == 2

    assert (
        summary.stations_with_observation
        == 0
    )

    assert (
        summary.stations_without_observation
        == 2
    )

    assert summary.total_observations == 0
    assert summary.fresh_observations == 0

    assert (
        summary.status
        is QualityStatus.FAIL
    )


def test_future_observation_warns(
    spark: SparkSession,
) -> None:
    stations = spark.createDataFrame(
        [
            ("44",),
            ("45",),
        ],
        schema=STATION_SCHEMA,
    )

    observations = (
        spark.createDataFrame(
            [
                (
                    "44",
                    datetime(
                        2026,
                        9,
                        29,
                        6,
                        30,
                        tzinfo=UTC,
                    ),
                    INGESTED_AT,
                ),
                (
                    "45",
                    datetime(
                        2026,
                        9,
                        30,
                        7,
                        0,
                        tzinfo=UTC,
                    ),
                    INGESTED_AT,
                ),
            ],
            schema=OBSERVATION_SCHEMA,
        )
    )

    summary = (
        summarize_current_data_health(
            stations=stations,
            observations=observations,
        )
    )

    assert summary.fresh_observations == 1
    assert summary.future_observations == 1

    assert (
        summary.status
        is QualityStatus.WARN
    )