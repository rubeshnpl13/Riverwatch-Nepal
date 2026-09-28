from pydantic import BaseModel, ConfigDict, Field

from riverwatch.models.source import (
    DataProvider,
    OriginalDataSource,
)


class RiverStation(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    station_id: str = Field(
        min_length=1,
    )

    station_series_id: str | None = None

    station_name: str

    basin_name: str | None = None

    latitude: float = Field(
        ge=-90,
        le=90,
    )

    longitude: float = Field(
        ge=-180,
        le=180,
    )

    elevation_m: float | None = None

    province_id: int | None = None
    district_id: int | None = None
    municipality_id: int | None = None
    ward_id: int | None = None

    description: str | None = None

    affected_male_count: int | None = None
    affected_female_count: int | None = None
    affected_household_count: int | None = None

    provider: DataProvider

    original_source: OriginalDataSource