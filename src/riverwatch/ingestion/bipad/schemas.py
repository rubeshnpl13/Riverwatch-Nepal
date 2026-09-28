from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class GeoJSONPoint(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
    )

    type: Literal["Point"]

    coordinates: tuple[
        float,
        float,
    ]

    @property
    def longitude(self) -> float:
        return self.coordinates[0]

    @property
    def latitude(self) -> float:
        return self.coordinates[1]


class BipadRiverRecord(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    id: int

    created_on: datetime = Field(
        alias="createdOn",
    )

    modified_on: datetime = Field(
        alias="modifiedOn",
    )

    title: str

    basin: str | None = None

    point: GeoJSONPoint

    water_level: float | None = Field(
        default=None,
        alias="waterLevel",
    )

    image: str | None = None

    danger_level: float | None = Field(
        default=None,
        alias="dangerLevel",
    )

    warning_level: float | None = Field(
        default=None,
        alias="warningLevel",
    )

    water_level_on: datetime = Field(
        alias="waterLevelOn",
    )

    status: str | None = None

    elevation: float | None = None

    steady: str | None = None

    description: str | None = None

    station_series_id: int | None = Field(
        default=None,
        alias="stationSeriesId",
    )

    data_source: str | None = Field(
        default=None,
        alias="dataSource",
    )

    data_source_id: int | str | None = Field(
        default=None,
        alias="dataSourceId",
    )

    ward: int | None = None

    municipality: int | None = None

    district: int | None = None

    province: int | None = None

    station: int


class BipadRiverPage(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )

    count: int

    next: str | None = None

    previous: str | None = None

    results: list[BipadRiverRecord]

class BipadAffectedDemography(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    male_count: int | None = Field(
        default=None,
        alias="maleCount",
    )

    female_count: int | None = Field(
        default=None,
        alias="femaleCount",
    )

    household_count: int | None = Field(
        default=None,
        alias="householdCount",
    )

class BipadRiverStation(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    id: int

    affected_demography: BipadAffectedDemography | None = Field(
        default=None,
        alias="affectedDemography",
    )

    created_on: datetime = Field(
        alias="createdOn",
    )

    modified_on: datetime = Field(
        alias="modifiedOn",
    )

    title: str

    basin: str | None = None

    point: GeoJSONPoint

    water_level: float | None = Field(
        default=None,
        alias="waterLevel",
    )

    image: str | None = None

    danger_level: float | None = Field(
        default=None,
        alias="dangerLevel",
    )

    warning_level: float | None = Field(
        default=None,
        alias="warningLevel",
    )

    water_level_on: datetime | None = Field(
        default=None,
        alias="waterLevelOn",
    )

    status: str | None = None

    elevation: float | None = None

    steady: str | None = None

    description: str | None = None

    station_series_id: int | None = Field(
        default=None,
        alias="stationSeriesId",
    )

    data_source: str | None = Field(
        default=None,
        alias="dataSource",
    )

    data_source_id: int | str | None = Field(
        default=None,
        alias="dataSourceId",
    )

    ward: int | None = None
    municipality: int | None = None
    district: int | None = None
    province: int | None = None

#page schema

class BipadRiverStationsPage(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )

    count: int
    next: str | None = None
    previous: str | None = None

    results: list[BipadRiverStation]