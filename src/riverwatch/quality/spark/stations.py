from pyspark.sql import (
    Column,
    DataFrame,
)
from pyspark.sql import (
    functions as F,
)
from pyspark.sql.window import Window

from riverwatch.quality.model import (
    QualityStatus,
    StationQualityRule,
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
        StationQualityRule,
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


def evaluate_station_quality(
    *,
    stations: DataFrame,
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

    station_id_window = (
        Window.partitionBy(
            "run_id",
            "station_id",
        )
    )

    station_series_window = (
        Window.partitionBy(
            "run_id",
            "station_series_id",
        )
    )

    checked_stations = (
        stations
        .withColumn(
            "_duplicate_station_id",
            (
                ~_is_blank(
                    "run_id"
                )
                & ~_is_blank(
                    "station_id"
                )
                & (
                    F.count(
                        F.lit(1)
                    ).over(
                        station_id_window
                    )
                    > 1
                )
            ),
        )
        .withColumn(
            "_duplicate_station_series_id",
            (
                ~_is_blank(
                    "run_id"
                )
                & ~_is_blank(
                    "station_series_id"
                )
                & (
                    F.count(
                        F.lit(1)
                    ).over(
                        station_series_window
                    )
                    > 1
                )
            ),
        )
    )

    quality_errors = _flags(
        (
            _is_blank(
                "station_id"
            ),
            (
                StationQualityRule
                .MISSING_STATION_ID
            ),
        ),
        (
            _is_blank(
                "station_name"
            ),
            (
                StationQualityRule
                .MISSING_STATION_NAME
            ),
        ),
        (
            _is_blank(
                "run_id"
            ),
            (
                StationQualityRule
                .MISSING_RUN_ID
            ),
        ),
        (
            invalid_latitude,
            (
                StationQualityRule
                .INVALID_LATITUDE
            ),
        ),
        (
            invalid_longitude,
            (
                StationQualityRule
                .INVALID_LONGITUDE
            ),
        ),
        (
            F.col(
                "_duplicate_station_id"
            ),
            (
                StationQualityRule
                .DUPLICATE_STATION_ID
            ),
        ),
    )

    quality_warnings = _flags(
        (
            _is_blank(
                "station_series_id"
            ),
            (
                StationQualityRule
                .MISSING_STATION_SERIES_ID
            ),
        ),
        (
            _is_blank(
                "basin_name"
            ),
            (
                StationQualityRule
                .MISSING_BASIN_NAME
            ),
        ),
        (
            missing_coordinates,
            (
                StationQualityRule
                .MISSING_COORDINATES
            ),
        ),
        (
            F.col(
                "_duplicate_station_series_id"
            ),
            (
                StationQualityRule
                .DUPLICATE_STATION_SERIES_ID
            ),
        ),
    )

    result = (
        checked_stations
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
            "_duplicate_station_id",
            "_duplicate_station_series_id",
        )
    )