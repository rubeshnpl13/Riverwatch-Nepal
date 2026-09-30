from pydantic import AwareDatetime

from riverwatch.api.schemas.base import (
    APIResponseModel,
)


class StationObservationResponse(
    APIResponseModel
):
    source_record_id: str | None

    station_id: str

    observed_at: AwareDatetime

    water_level_m: float | None

    endpoint: str
    run_id: str

    ingested_at: AwareDatetime | None