from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from riverwatch.models.source import DataProvider, OriginalDataSource


class RiverTrend(StrEnum):
    RISING = "rising"
    FALLING = "falling"
    STABLE = "stable"
    UNKNOWN = "unknown"


class RiverObservation(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    station_id: str = Field(
        min_length=1,
    )

    source_record_id: str

    source_station_series_id: str | None = None

    station_name: str | None = None

    river_name: str | None = None

    basin_name: str | None = None

    latitude: float | None = Field(
        default=None,
        ge=-90,
        le=90,
    )

    longitude: float | None = Field(
        default=None,
        ge=-180,
        le=180,
    )

    observed_at: datetime

    water_level_m: float | None = None
    warning_level_m: float | None = None
    danger_level_m: float | None = None

    trend: RiverTrend = RiverTrend.UNKNOWN

    official_status: str | None = None

    provider: DataProvider
    original_source: OriginalDataSource

    source_created_at: datetime | None = None
    source_modified_at: datetime | None = None

    ingested_at: datetime