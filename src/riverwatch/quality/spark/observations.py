from pyspark.sql import (
    Column,
    DataFrame,
)
from pyspark.sql import (
    functions as F,
)
from pyspark.sql.window import Window

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.quality.model import (
    DEFAULT_OBSERVATION_QUALITY_POLICY,
    ObservationQualityPolicy,
    ObservationQualityRule,
    QualityStatus,
)


def _is_blank(
    column_name: str,
) -> Column:
    column = F.col(
        column_name
    )

    return (
        column.isNull()
        | (
            F.trim(
                column.cast("string")
            )
            == ""
        )
    )


def _flags(
    *rules: tuple[
        Column,
        ObservationQualityRule,
    ],
) -> Column:
    candidates = [
        F.when(
            condition,
            F.lit(
                rule.value
            ),
        )
        for condition, rule in rules
    ]

    return F.filter(
        F.array(
            *candidates
        ),
        lambda item: (
            item.isNotNull()
        ),
    )


def evaluate_observation_quality(
    *,
    observations: DataFrame,
    endpoint: BipadEndpoint,
    policy: ObservationQualityPolicy = (
        DEFAULT_OBSERVATION_QUALITY_POLICY
    ),
) -> DataFrame:
    invalid_latitude = (
        F.col(
            "latitude"
        ).isNotNull()
        & (
            (
                F.col(
                    "latitude"
                )
                < -90.0
            )
            | (
                F.col(
                    "latitude"
                )
                > 90.0
            )
        )
    )

    invalid_longitude = (
        F.col(
            "longitude"
        ).isNotNull()
        & (
            (
                F.col(
                    "longitude"
                )
                < -180.0
            )
            | (
                F.col(
                    "longitude"
                )
                > 180.0
            )
        )
    )

    missing_coordinates = (
        F.col(
            "latitude"
        ).isNull()
        | F.col(
            "longitude"
        ).isNull()
    )

    suspicious_water_level = (
        F.col(
            "water_level_m"
        ).isNotNull()
        & (
            F.abs(
                F.col(
                    "water_level_m"
                )
            )
            >= (
                policy
                .suspicious_absolute_water_level_m
            )
        )
    )

    warning_above_danger = (
        F.col(
            "warning_level_m"
        ).isNotNull()
        & F.col(
            "danger_level_m"
        ).isNotNull()
        & (
            F.col(
                "warning_level_m"
            )
            > F.col(
                "danger_level_m"
            )
        )
    )

    if (
        endpoint
        is BipadEndpoint.RIVER_STATIONS
    ):
        maximum_age_seconds = int(
            policy.current_max_age.total_seconds()
        )

        observation_age_seconds = (
            F.unix_timestamp(
                F.col(
                    "ingested_at"
                )
            )
            - F.unix_timestamp(
                F.col(
                    "observed_at"
                )
            )
        )

        stale_current_observation = (
            F.col(
                "observed_at"
            ).isNotNull()
            & F.col(
                "ingested_at"
            ).isNotNull()
            & (
                observation_age_seconds
                > maximum_age_seconds
            )
        )

    else:
        stale_current_observation = (
            F.lit(False)
        )

    source_record_window = (
        Window.partitionBy(
            "run_id",
            "source_record_id",
        )
    )

    station_time_window = (
        Window.partitionBy(
            "run_id",
            "station_id",
            "observed_at",
        )
    )

    checked_observations = (
        observations
        .withColumn(
            "_duplicate_source_record_id",
            (
                ~_is_blank(
                    "run_id"
                )
                & ~_is_blank(
                    "source_record_id"
                )
                & (
                    F.count(
                        F.lit(1)
                    ).over(
                        source_record_window
                    )
                    > 1
                )
            ),
        )
        .withColumn(
            "_duplicate_station_observed_at",
            (
                ~_is_blank(
                    "run_id"
                )
                & ~_is_blank(
                    "station_id"
                )
                & F.col(
                    "observed_at"
                ).isNotNull()
                & (
                    F.count(
                        F.lit(1)
                    ).over(
                        station_time_window
                    )
                    > 1
                )
            ),
        )
    )

    quality_errors = _flags(
        (
            _is_blank(
                "source_record_id"
            ),
            (
                ObservationQualityRule
                .MISSING_SOURCE_RECORD_ID
            ),
        ),
        (
            _is_blank(
                "station_id"
            ),
            (
                ObservationQualityRule
                .MISSING_STATION_ID
            ),
        ),
        (
            F.col(
                "observed_at"
            ).isNull(),
            (
                ObservationQualityRule
                .MISSING_OBSERVED_AT
            ),
        ),
        (
            _is_blank(
                "run_id"
            ),
            (
                ObservationQualityRule
                .MISSING_RUN_ID
            ),
        ),
        (
            invalid_latitude,
            (
                ObservationQualityRule
                .INVALID_LATITUDE
            ),
        ),
        (
            invalid_longitude,
            (
                ObservationQualityRule
                .INVALID_LONGITUDE
            ),
        ),
        (
            F.col(
                "_duplicate_source_record_id"
            ),
            (
                ObservationQualityRule
                .DUPLICATE_SOURCE_RECORD_ID
            ),
        ),
    )

    quality_warnings = _flags(
        (
            F.col(
                "water_level_m"
            ).isNull(),
            (
                ObservationQualityRule
                .MISSING_WATER_LEVEL
            ),
        ),
        (
            missing_coordinates,
            (
                ObservationQualityRule
                .MISSING_COORDINATES
            ),
        ),
        (
            suspicious_water_level,
            (
                ObservationQualityRule
                .SUSPICIOUS_WATER_LEVEL
            ),
        ),
        (
            warning_above_danger,
            (
                ObservationQualityRule
                .WARNING_ABOVE_DANGER
            ),
        ),
        (
            stale_current_observation,
            (
                ObservationQualityRule
                .STALE_CURRENT_OBSERVATION
            ),
        ),
        (
            F.col(
                "_duplicate_station_observed_at"
            ),
            (
                ObservationQualityRule
                .DUPLICATE_STATION_OBSERVED_AT
            ),
        ),
    )

    result = (
        checked_observations
        .withColumn(
            "quality_errors",
            quality_errors,
        )
        .withColumn(
            "quality_warnings",
            quality_warnings,
        )
        .withColumn(
            "quality_error_count",
            F.size(
                F.col(
                    "quality_errors"
                )
            ),
        )
        .withColumn(
            "quality_warning_count",
            F.size(
                F.col(
                    "quality_warnings"
                )
            ),
        )
    )

    return (
        result
        .withColumn(
            "quality_status",
            F.when(
                F.col(
                    "quality_error_count"
                )
                > 0,
                F.lit(
                    QualityStatus.FAIL.value
                ),
            )
            .when(
                F.col(
                    "quality_warning_count"
                )
                > 0,
                F.lit(
                    QualityStatus.WARN.value
                ),
            )
            .otherwise(
                F.lit(
                    QualityStatus.PASS.value
                )
            ),
        )
        .drop(
            "_duplicate_source_record_id",
            "_duplicate_station_observed_at",
        )
    )