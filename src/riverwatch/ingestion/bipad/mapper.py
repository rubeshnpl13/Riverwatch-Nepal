from datetime import UTC, datetime

from riverwatch.ingestion.bipad.schemas import (
    BipadRiverRecord,
    BipadRiverStation,
)
from riverwatch.models import (
    DataProvider,
    OriginalDataSource,
    RiverObservation,
    RiverStation,
    RiverTrend,
)


def map_river_trend(
    source_value: str | None,
) -> RiverTrend:
    if source_value is None:
        return RiverTrend.UNKNOWN

    normalized = source_value.strip().upper()

    mapping = {
        "RISING": RiverTrend.RISING,
        "FALLING": RiverTrend.FALLING,
        "STEADY": RiverTrend.STABLE,
    }

    return mapping.get(
        normalized,
        RiverTrend.UNKNOWN,
    )


def clean_optional_text(
    value: str | None,
) -> str | None:
    if value is None:
        return None

    cleaned = value.strip()

    return cleaned or None


def map_bipad_river_record(
    record: BipadRiverRecord,
    *,
    ingested_at: datetime | None = None,
) -> RiverObservation:
    ingestion_time = (
        ingested_at
        if ingested_at is not None
        else datetime.now(UTC)
    )

    return RiverObservation(
        station_id=str(record.station),
        source_record_id=str(record.id),
        source_station_series_id=(
            str(record.station_series_id)
            if record.station_series_id is not None
            else None
        ),
        station_name=clean_optional_text(
            record.title
        ),
        river_name=None,
        basin_name=clean_optional_text(
            record.basin
        ),
        latitude=record.point.latitude,
        longitude=record.point.longitude,
        observed_at=record.water_level_on,
        water_level_m=record.water_level,
        warning_level_m=record.warning_level,
        danger_level_m=record.danger_level,
        trend=map_river_trend(
            record.steady
        ),
        official_status=clean_optional_text(
            record.status
        ),
        provider=DataProvider.BIPAD,
        original_source=OriginalDataSource.DHM,
        source_created_at=record.created_on,
        source_modified_at=record.modified_on,
        ingested_at=ingestion_time,
    )


def map_bipad_station(
    source: BipadRiverStation,
) -> RiverStation:
    demography = source.affected_demography

    return RiverStation(
        station_id=str(source.id),
        station_series_id=(
            str(source.station_series_id)
            if source.station_series_id is not None
            else None
        ),
        station_name=source.title.strip(),
        basin_name=clean_optional_text(
            source.basin
        ),
        latitude=source.point.latitude,
        longitude=source.point.longitude,
        elevation_m=source.elevation,
        province_id=source.province,
        district_id=source.district,
        municipality_id=source.municipality,
        ward_id=source.ward,
        description=clean_optional_text(
            source.description
        ),
        affected_male_count=(
            demography.male_count
            if demography is not None
            else None
        ),
        affected_female_count=(
            demography.female_count
            if demography is not None
            else None
        ),
        affected_household_count=(
            demography.household_count
            if demography is not None
            else None
        ),
        provider=DataProvider.BIPAD,
        original_source=OriginalDataSource.DHM,
    )


def map_bipad_station_observation(
    source: BipadRiverStation,
    *,
    ingested_at: datetime | None = None,
) -> RiverObservation | None:
    if source.water_level_on is None:
        return None

    ingestion_time = (
        ingested_at
        if ingested_at is not None
        else datetime.now(UTC)
    )

    return RiverObservation(
        station_id=str(source.id),
        source_record_id=(
            f"station-snapshot:{source.id}:"
            f"{source.water_level_on.isoformat()}"
        ),
        source_station_series_id=(
            str(source.station_series_id)
            if source.station_series_id is not None
            else None
        ),
        station_name=clean_optional_text(
            source.title
        ),
        river_name=None,
        basin_name=clean_optional_text(
            source.basin
        ),
        latitude=source.point.latitude,
        longitude=source.point.longitude,
        observed_at=source.water_level_on,
        water_level_m=source.water_level,
        warning_level_m=source.warning_level,
        danger_level_m=source.danger_level,
        trend=map_river_trend(
            source.steady
        ),
        official_status=clean_optional_text(
            source.status
        ),
        provider=DataProvider.BIPAD,
        original_source=OriginalDataSource.DHM,
        source_created_at=source.created_on,
        source_modified_at=source.modified_on,
        ingested_at=ingestion_time,
    )