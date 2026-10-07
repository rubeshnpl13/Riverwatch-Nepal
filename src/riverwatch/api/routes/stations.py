from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
)

from riverwatch.analytics.protocol import (
    AnalyticsReader,
)
from riverwatch.api.dependencies import (
    get_analytics_service,
)
from riverwatch.api.schemas.stations import (
    CurrentRiverStationResponse,
)

router = APIRouter(
    prefix="/stations",
    tags=[
        "stations",
    ],
)


@router.get(
    "",
    response_model=list[
        CurrentRiverStationResponse
    ],
    summary="List current river stations",
)
def list_current_stations(
    analytics: Annotated[
        AnalyticsReader,
        Depends(
            get_analytics_service
        ),
    ],
) -> list[
    CurrentRiverStationResponse
]:
    stations = (
        analytics.get_current_snapshot()
    )

    return [
        CurrentRiverStationResponse
        .model_validate(
            station
        )
        for station in stations
    ]


@router.get(
    "/{station_id}",
    response_model=(
        CurrentRiverStationResponse
    ),
    summary="Get current river station",
)
def get_current_station(
    station_id: str,
    analytics: Annotated[
        AnalyticsReader,
        Depends(
            get_analytics_service
        ),
    ],
) -> CurrentRiverStationResponse:
    station = analytics.get_station(
        station_id
    )

    if station is None:
        raise HTTPException(
            status_code=(
                status.HTTP_404_NOT_FOUND
            ),
            detail="Station not found",
        )

    return (
        CurrentRiverStationResponse
        .model_validate(
            station
        )
    )