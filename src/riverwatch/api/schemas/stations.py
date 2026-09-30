from pydantic import AwareDatetime

from riverwatch.api.schemas.base import (
    APIResponseModel,
)


class CurrentRiverStationResponse(
    APIResponseModel
):
    station_id: str | None
    station_name: str | None
    basin_name: str | None

    latitude: float | None
    longitude: float | None
    elevation_m: float | None

    source_record_id: str | None

    observed_at: AwareDatetime | None

    water_level_m: float | None

    observation_ingested_at: (
        AwareDatetime | None
    )

    has_observation: bool

    observation_age_hours: (
        float | None
    )