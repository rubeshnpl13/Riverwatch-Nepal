from pyspark.sql import (
    Column,
    DataFrame,
)
from pyspark.sql import (
    functions as F,
)

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.models.source import (
    DataProvider,
    OriginalDataSource,
)
from riverwatch.processing.models import (
    ProcessingInputRun,
)


def _require_endpoint(
    *,
    processing_input: ProcessingInputRun,
    endpoint: BipadEndpoint,
) -> None:
    if processing_input.endpoint is not endpoint:
        raise ValueError(
            "Unexpected endpoint for transformation: "
            f"{processing_input.endpoint.value}"
        )


def _explode_records(
    *,
    raw_pages: DataFrame,
    processing_input: ProcessingInputRun,
) -> DataFrame:
    return (
        raw_pages
        .withColumn(
            "_source_file",
            F.input_file_name(),
        )
        .withColumn(
            "source_page_index",
            F.regexp_extract(
                F.col("_source_file"),
                r"page=(\d+)/payload\.json$",
                1,
            ).cast("int"),
        )
        .select(
            F.explode_outer(
                F.col("results")
            ).alias(
                "record"
            ),
            F.col(
                "source_page_index"
            ),
        )
        .where(
            F.col("record").isNotNull()
        )
        .withColumn(
            "run_id",
            F.lit(
                processing_input.run_id
            ),
        )
        .withColumn(
            "ingested_at",
            F.lit(
                processing_input.captured_at
            ).cast(
                "timestamp"
            ),
        )
    )


def _trend_column() -> Column:
    source_trend = F.upper(
        F.trim(
            F.col(
                "record.steady"
            )
        )
    )

    return (
        F.when(
            source_trend
            == "RISING",
            F.lit("RISING"),
        )
        .when(
            source_trend
            == "FALLING",
            F.lit("FALLING"),
        )
        .when(
            source_trend
            == "STEADY",
            F.lit("STABLE"),
        )
        .otherwise(
            F.lit("UNKNOWN")
        )
    )


def _longitude_column() -> Column:
    return F.expr(
        "try_element_at("
        "record.point.coordinates, 1"
        ")"
    )


def _latitude_column() -> Column:
    return F.expr(
        "try_element_at("
        "record.point.coordinates, 2"
        ")"
    )


def transform_historical_observations(
    *,
    raw_pages: DataFrame,
    processing_input: ProcessingInputRun,
) -> DataFrame:
    _require_endpoint(
        processing_input=processing_input,
        endpoint=BipadEndpoint.RIVER,
    )

    records = _explode_records(
        raw_pages=raw_pages,
        processing_input=processing_input,
    )

    return records.select(
        F.col(
            "record.id"
        ).cast(
            "string"
        ).alias(
            "source_record_id"
        ),
        F.col(
            "record.station"
        ).cast(
            "string"
        ).alias(
            "station_id"
        ),
        F.col(
            "record.stationSeriesId"
        ).cast(
            "string"
        ).alias(
            "source_station_series_id"
        ),
        F.col(
            "record.title"
        ).alias(
            "station_name"
        ),
        F.lit(
            None
        ).cast(
            "string"
        ).alias(
            "river_name"
        ),
        F.col(
            "record.basin"
        ).alias(
            "basin_name"
        ),
        _latitude_column().alias(
            "latitude"
        ),
        _longitude_column().alias(
            "longitude"
        ),
        F.try_to_timestamp(
            F.col(
                "record.waterLevelOn"
            )
        ).alias(
            "observed_at"
        ),
        F.col(
            "record.waterLevel"
        ).alias(
            "water_level_m"
        ),
        F.col(
            "record.warningLevel"
        ).alias(
            "warning_level_m"
        ),
        F.col(
            "record.dangerLevel"
        ).alias(
            "danger_level_m"
        ),
        _trend_column().alias(
            "trend"
        ),
        F.col(
            "record.status"
        ).alias(
            "official_status"
        ),
        F.lit(
            DataProvider.BIPAD.value
        ).alias(
            "provider"
        ),
        F.lit(
            OriginalDataSource.DHM.value
        ).alias(
            "original_source"
        ),
        F.try_to_timestamp(
            F.col(
                "record.createdOn"
            )
        ).alias(
            "source_created_at"
        ),
        F.try_to_timestamp(
            F.col(
                "record.modifiedOn"
            )
        ).alias(
            "source_modified_at"
        ),
        F.col(
            "ingested_at"
        ),
        F.col(
            "run_id"
        ),
        F.col(
            "source_page_index"
        ),
    )


def transform_stations(
    *,
    raw_pages: DataFrame,
    processing_input: ProcessingInputRun,
) -> DataFrame:
    _require_endpoint(
        processing_input=processing_input,
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
    )

    records = _explode_records(
        raw_pages=raw_pages,
        processing_input=processing_input,
    )

    return records.select(
        F.col(
            "record.id"
        ).cast(
            "string"
        ).alias(
            "station_id"
        ),
        F.col(
            "record.stationSeriesId"
        ).cast(
            "string"
        ).alias(
            "station_series_id"
        ),
        F.col(
            "record.title"
        ).alias(
            "station_name"
        ),
        F.col(
            "record.basin"
        ).alias(
            "basin_name"
        ),
        _latitude_column().alias(
            "latitude"
        ),
        _longitude_column().alias(
            "longitude"
        ),
        F.col(
            "record.elevation"
        ).alias(
            "elevation_m"
        ),
        F.col(
            "record.provinceId"
        ).alias(
            "province_id"
        ),
        F.col(
            "record.districtId"
        ).alias(
            "district_id"
        ),
        F.col(
            "record.municipalityId"
        ).alias(
            "municipality_id"
        ),
        F.col(
            "record.wardId"
        ).alias(
            "ward_id"
        ),
        F.col(
            "record.description"
        ).alias(
            "description"
        ),
        F.lit(
            DataProvider.BIPAD.value
        ).alias(
            "provider"
        ),
        F.lit(
            OriginalDataSource.DHM.value
        ).alias(
            "original_source"
        ),
        F.try_to_timestamp(
            F.col(
                "record.createdOn"
            )
        ).alias(
            "source_created_at"
        ),
        F.try_to_timestamp(
            F.col(
                "record.modifiedOn"
            )
        ).alias(
            "source_modified_at"
        ),
        F.col(
            "ingested_at"
        ),
        F.col(
            "run_id"
        ),
        F.col(
            "source_page_index"
        ),
    )


def transform_station_observations(
    *,
    raw_pages: DataFrame,
    processing_input: ProcessingInputRun,
) -> DataFrame:
    _require_endpoint(
        processing_input=processing_input,
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
    )

    records = _explode_records(
        raw_pages=raw_pages,
        processing_input=processing_input,
    )

    observations = records.select(
        F.concat(
            F.lit(
                "station-snapshot:"
            ),
            F.col(
                "record.id"
            ).cast(
                "string"
            ),
            F.lit(":"),
            F.col(
                "record.waterLevelOn"
            ),
        ).alias(
            "source_record_id"
        ),
        F.col(
            "record.id"
        ).cast(
            "string"
        ).alias(
            "station_id"
        ),
        F.col(
            "record.stationSeriesId"
        ).cast(
            "string"
        ).alias(
            "source_station_series_id"
        ),
        F.col(
            "record.title"
        ).alias(
            "station_name"
        ),
        F.lit(
            None
        ).cast(
            "string"
        ).alias(
            "river_name"
        ),
        F.col(
            "record.basin"
        ).alias(
            "basin_name"
        ),
        _latitude_column().alias(
            "latitude"
        ),
        _longitude_column().alias(
            "longitude"
        ),
        F.try_to_timestamp(
            F.col(
                "record.waterLevelOn"
            )
        ).alias(
            "observed_at"
        ),
        F.col(
            "record.waterLevel"
        ).alias(
            "water_level_m"
        ),
        F.col(
            "record.warningLevel"
        ).alias(
            "warning_level_m"
        ),
        F.col(
            "record.dangerLevel"
        ).alias(
            "danger_level_m"
        ),
        _trend_column().alias(
            "trend"
        ),
        F.col(
            "record.status"
        ).alias(
            "official_status"
        ),
        F.lit(
            DataProvider.BIPAD.value
        ).alias(
            "provider"
        ),
        F.lit(
            OriginalDataSource.DHM.value
        ).alias(
            "original_source"
        ),
        F.try_to_timestamp(
            F.col(
                "record.createdOn"
            )
        ).alias(
            "source_created_at"
        ),
        F.try_to_timestamp(
            F.col(
                "record.modifiedOn"
            )
        ).alias(
            "source_modified_at"
        ),
        F.col(
            "ingested_at"
        ),
        F.col(
            "run_id"
        ),
        F.col(
            "source_page_index"
        ),
    )

    # Same rule as Phase 2:
    # a station can exist without having a current
    # observation timestamp.
    return observations.where(
        F.col(
            "observed_at"
        ).isNotNull()
    )