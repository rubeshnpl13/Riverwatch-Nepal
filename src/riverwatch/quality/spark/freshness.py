from datetime import (
    UTC,
    datetime,
)

from pyspark.sql import (
    DataFrame,
)
from pyspark.sql import (
    functions as F,
)

from riverwatch.quality.model import (
    DEFAULT_OBSERVATION_QUALITY_POLICY,
    CurrentDataHealthSummary,
    ObservationQualityPolicy,
    QualityStatus,
)


def _utc_datetime_from_epoch(
    value: int | None,
) -> datetime | None:
    if value is None:
        return None

    return datetime.fromtimestamp(
        value,
        tz=UTC,
    )

def summarize_current_data_health(
    *,
    stations: DataFrame,
    observations: DataFrame,
    policy: ObservationQualityPolicy = (
        DEFAULT_OBSERVATION_QUALITY_POLICY
    ),
) -> CurrentDataHealthSummary:
    #
    # Station coverage
    #

    station_ids = (
        stations
        .select(
            "station_id"
        )
        .where(
            F.col(
                "station_id"
            ).isNotNull()
            & (
                F.trim(
                    F.col(
                        "station_id"
                    )
                )
                != ""
            )
        )
        .distinct()
    )

    observation_station_ids = (
        observations
        .select(
            "station_id"
        )
        .where(
            F.col(
                "station_id"
            ).isNotNull()
            & (
                F.trim(
                    F.col(
                        "station_id"
                    )
                )
                != ""
            )
        )
        .distinct()
        .withColumn(
            "_has_observation",
            F.lit(1),
        )
    )

    coverage = (
        station_ids
        .join(
            observation_station_ids,
            on="station_id",
            how="left",
        )
        .agg(
            F.count(
                F.lit(1)
            ).alias(
                "total_stations"
            ),
            F.coalesce(
                F.sum(
                    F.coalesce(
                        F.col(
                            "_has_observation"
                        ),
                        F.lit(0),
                    )
                ),
                F.lit(0),
            ).alias(
                "stations_with_observation"
            ),
        )
        .collect()[0]
    )

    total_stations = int(
        coverage[
            "total_stations"
        ]
    )

    stations_with_observation = int(
        coverage[
            "stations_with_observation"
        ]
    )

    stations_without_observation = (
        total_stations
        - stations_with_observation
    )

    #
    # Observation freshness
    #

    maximum_age_seconds = int(
        policy.current_max_age.total_seconds()
    )

    assessable = (
        F.col(
            "observed_at"
        ).isNotNull()
        & F.col(
            "ingested_at"
        ).isNotNull()
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

    fresh = (
        assessable
        & (
            observation_age_seconds
            >= 0
        )
        & (
            observation_age_seconds
            <= maximum_age_seconds
        )
    )

    stale = (
        assessable
        & (
            observation_age_seconds
            > maximum_age_seconds
        )
    )

    future = (
        assessable
        & (
            observation_age_seconds
            < 0
        )
    )

    freshness = (
        observations
        .agg(
            F.count(
                F.lit(1)
            ).alias(
                "total_observations"
            ),
            F.coalesce(
                F.sum(
                    F.when(
                        fresh,
                        F.lit(1),
                    ).otherwise(
                        F.lit(0)
                    )
                ),
                F.lit(0),
            ).alias(
                "fresh_observations"
            ),
            F.coalesce(
                F.sum(
                    F.when(
                        stale,
                        F.lit(1),
                    ).otherwise(
                        F.lit(0)
                    )
                ),
                F.lit(0),
            ).alias(
                "stale_observations"
            ),
            F.coalesce(
                F.sum(
                    F.when(
                        future,
                        F.lit(1),
                    ).otherwise(
                        F.lit(0)
                    )
                ),
                F.lit(0),
            ).alias(
                "future_observations"
            ),
            F.coalesce(
                F.sum(
                    F.when(
                        ~assessable,
                        F.lit(1),
                    ).otherwise(
                        F.lit(0)
                    )
                ),
                F.lit(0),
            ).alias(
                "unassessable_observations"
            ),
            F.min(
                F.col(
                    "observed_at"
                ).cast(
                    "long"
                )
            ).alias(
                "oldest_observed_at_epoch"
            ),

            F.max(
                F.col(
                    "observed_at"
                ).cast(
                    "long"
                )
            ).alias(
                "latest_observed_at_epoch"
            ),
        )
        .collect()[0]
    )

    total_observations = int(
        freshness[
            "total_observations"
        ]
    )

    fresh_observations = int(
        freshness[
            "fresh_observations"
        ]
    )

    stale_observations = int(
        freshness[
            "stale_observations"
        ]
    )

    future_observations = int(
        freshness[
            "future_observations"
        ]
    )

    unassessable_observations = int(
        freshness[
            "unassessable_observations"
        ]
    )

    if total_stations > 0:
        coverage_ratio = (
            stations_with_observation
            / total_stations
        )
    else:
        coverage_ratio = 0.0

    if total_observations > 0:
        freshness_ratio = (
            fresh_observations
            / total_observations
        )
    else:
        freshness_ratio = 0.0

    #
    # Dataset health
    #

    if (
        total_stations == 0
        or total_observations == 0
        or fresh_observations == 0
    ):
        status = QualityStatus.FAIL

    elif (
        stations_without_observation > 0
        or stale_observations > 0
        or future_observations > 0
        or unassessable_observations > 0
    ):
        status = QualityStatus.WARN

    else:
        status = QualityStatus.PASS

    return CurrentDataHealthSummary(
        total_stations=total_stations,
        stations_with_observation=(
            stations_with_observation
        ),
        stations_without_observation=(
            stations_without_observation
        ),
        total_observations=(
            total_observations
        ),
        fresh_observations=(
            fresh_observations
        ),
        stale_observations=(
            stale_observations
        ),
        future_observations=(
            future_observations
        ),
        unassessable_observations=(
            unassessable_observations
        ),
        coverage_ratio=(
            coverage_ratio
        ),
        freshness_ratio=(
            freshness_ratio
        ),
        oldest_observed_at=(
            _utc_datetime_from_epoch(
                freshness[
                    "oldest_observed_at_epoch"
                ]
            )
        ),
        latest_observed_at=(
            _utc_datetime_from_epoch(
                freshness[
                    "latest_observed_at_epoch"
                ]
            )
        ),
        status=status,
    )