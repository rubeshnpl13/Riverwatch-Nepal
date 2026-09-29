from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)

GEOJSON_POINT_SCHEMA = StructType(
    [
        StructField(
            "type",
            StringType(),
            nullable=True,
        ),
        StructField(
            "coordinates",
            ArrayType(
                DoubleType(),
                containsNull=True,
            ),
            nullable=True,
        ),
    ]
)


BIPAD_RIVER_RECORD_SCHEMA = StructType(
    [
        StructField(
            "id",
            LongType(),
            nullable=True,
        ),
        StructField(
            "createdOn",
            StringType(),
            nullable=True,
        ),
        StructField(
            "modifiedOn",
            StringType(),
            nullable=True,
        ),
        StructField(
            "title",
            StringType(),
            nullable=True,
        ),
        StructField(
            "basin",
            StringType(),
            nullable=True,
        ),
        StructField(
            "point",
            GEOJSON_POINT_SCHEMA,
            nullable=True,
        ),
        StructField(
            "waterLevel",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "dangerLevel",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "warningLevel",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "waterLevelOn",
            StringType(),
            nullable=True,
        ),
        StructField(
            "status",
            StringType(),
            nullable=True,
        ),
        StructField(
            "elevation",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "steady",
            StringType(),
            nullable=True,
        ),
        StructField(
            "description",
            StringType(),
            nullable=True,
        ),
        StructField(
            "stationSeriesId",
            LongType(),
            nullable=True,
        ),
        StructField(
            "dataSource",
            StringType(),
            nullable=True,
        ),
        StructField(
            "dataSourceId",
            LongType(),
            nullable=True,
        ),
        StructField(
            "provinceId",
            LongType(),
            nullable=True,
        ),
        StructField(
            "districtId",
            LongType(),
            nullable=True,
        ),
        StructField(
            "municipalityId",
            LongType(),
            nullable=True,
        ),
        StructField(
            "wardId",
            LongType(),
            nullable=True,
        ),
        StructField(
            "station",
            LongType(),
            nullable=True,
        ),
    ]
)


BIPAD_RIVER_STATION_SCHEMA = StructType(
    [
        StructField(
            "id",
            LongType(),
            nullable=True,
        ),
        StructField(
            "createdOn",
            StringType(),
            nullable=True,
        ),
        StructField(
            "modifiedOn",
            StringType(),
            nullable=True,
        ),
        StructField(
            "title",
            StringType(),
            nullable=True,
        ),
        StructField(
            "basin",
            StringType(),
            nullable=True,
        ),
        StructField(
            "point",
            GEOJSON_POINT_SCHEMA,
            nullable=True,
        ),
        StructField(
            "waterLevel",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "dangerLevel",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "warningLevel",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "waterLevelOn",
            StringType(),
            nullable=True,
        ),
        StructField(
            "status",
            StringType(),
            nullable=True,
        ),
        StructField(
            "elevation",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "steady",
            StringType(),
            nullable=True,
        ),
        StructField(
            "description",
            StringType(),
            nullable=True,
        ),
        StructField(
            "stationSeriesId",
            LongType(),
            nullable=True,
        ),
        StructField(
            "dataSource",
            StringType(),
            nullable=True,
        ),
        StructField(
            "dataSourceId",
            LongType(),
            nullable=True,
        ),
        StructField(
            "provinceId",
            LongType(),
            nullable=True,
        ),
        StructField(
            "districtId",
            LongType(),
            nullable=True,
        ),
        StructField(
            "municipalityId",
            LongType(),
            nullable=True,
        ),
        StructField(
            "wardId",
            LongType(),
            nullable=True,
        ),
    ]
)


def build_bipad_page_schema(
    *,
    record_schema: StructType,
) -> StructType:
    return StructType(
        [
            # BIPAD currently returns an extremely
            # large count value, so this MUST be LongType.
            StructField(
                "count",
                LongType(),
                nullable=True,
            ),
            StructField(
                "next",
                StringType(),
                nullable=True,
            ),
            StructField(
                "previous",
                StringType(),
                nullable=True,
            ),
            StructField(
                "results",
                ArrayType(
                    record_schema,
                    containsNull=True,
                ),
                nullable=False,
            ),
        ]
    )


BIPAD_RIVER_PAGE_SCHEMA = (
    build_bipad_page_schema(
        record_schema=(
            BIPAD_RIVER_RECORD_SCHEMA
        )
    )
)


BIPAD_RIVER_STATIONS_PAGE_SCHEMA = (
    build_bipad_page_schema(
        record_schema=(
            BIPAD_RIVER_STATION_SCHEMA
        )
    )
)


def schema_for_endpoint(
    endpoint: BipadEndpoint,
) -> StructType:
    if endpoint is BipadEndpoint.RIVER:
        return BIPAD_RIVER_PAGE_SCHEMA

    if endpoint is BipadEndpoint.RIVER_STATIONS:
        return BIPAD_RIVER_STATIONS_PAGE_SCHEMA

    raise ValueError(
        "No Spark processing schema "
        f"defined for endpoint: "
        f"{endpoint.value}"
    )