import pytest
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructType,
)

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.spark.schemas import (
    BIPAD_RIVER_PAGE_SCHEMA,
    BIPAD_RIVER_RECORD_SCHEMA,
    BIPAD_RIVER_STATION_SCHEMA,
    BIPAD_RIVER_STATIONS_PAGE_SCHEMA,
    GEOJSON_POINT_SCHEMA,
    schema_for_endpoint,
)


def test_geojson_point_schema() -> None:
    assert isinstance(
        GEOJSON_POINT_SCHEMA[
            "type"
        ].dataType,
        StringType,
    )

    coordinates_type = (
        GEOJSON_POINT_SCHEMA[
            "coordinates"
        ].dataType
    )

    assert isinstance(
        coordinates_type,
        ArrayType,
    )

    assert isinstance(
        coordinates_type.elementType,
        DoubleType,
    )


def test_historical_record_contains_station_id() -> None:
    station_type = (
        BIPAD_RIVER_RECORD_SCHEMA[
            "station"
        ].dataType
    )

    assert isinstance(
        station_type,
        LongType,
    )


def test_station_snapshot_does_not_have_station_field() -> None:
    assert (
        "station"
        not in BIPAD_RIVER_STATION_SCHEMA.fieldNames()
    )


def test_source_timestamps_remain_strings() -> None:
    assert isinstance(
        BIPAD_RIVER_RECORD_SCHEMA[
            "waterLevelOn"
        ].dataType,
        StringType,
    )

    assert isinstance(
        BIPAD_RIVER_STATION_SCHEMA[
            "waterLevelOn"
        ].dataType,
        StringType,
    )


def test_water_levels_are_double() -> None:
    assert isinstance(
        BIPAD_RIVER_RECORD_SCHEMA[
            "waterLevel"
        ].dataType,
        DoubleType,
    )

    assert isinstance(
        BIPAD_RIVER_STATION_SCHEMA[
            "waterLevel"
        ].dataType,
        DoubleType,
    )


def test_page_count_uses_long_type() -> None:
    assert isinstance(
        BIPAD_RIVER_PAGE_SCHEMA[
            "count"
        ].dataType,
        LongType,
    )

    assert isinstance(
        BIPAD_RIVER_STATIONS_PAGE_SCHEMA[
            "count"
        ].dataType,
        LongType,
    )


def test_page_results_are_struct_array() -> None:
    results_type = (
        BIPAD_RIVER_PAGE_SCHEMA[
            "results"
        ].dataType
    )

    assert isinstance(
        results_type,
        ArrayType,
    )

    assert isinstance(
        results_type.elementType,
        StructType,
    )


def test_selects_schema_by_endpoint() -> None:
    assert (
        schema_for_endpoint(
            BipadEndpoint.RIVER
        )
        is BIPAD_RIVER_PAGE_SCHEMA
    )

    assert (
        schema_for_endpoint(
            BipadEndpoint.RIVER_STATIONS
        )
        is BIPAD_RIVER_STATIONS_PAGE_SCHEMA
    )


@pytest.mark.parametrize(
    "endpoint",
    [
        BipadEndpoint.FLOOD_STATION,
        BipadEndpoint.STREAMFLOW,
    ],
)
def test_rejects_unsupported_processing_endpoint(
    endpoint: BipadEndpoint,
) -> None:
    with pytest.raises(
        ValueError,
        match="No Spark processing schema",
    ):
        schema_for_endpoint(
            endpoint
        )