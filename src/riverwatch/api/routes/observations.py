from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    status,
)
from pydantic import AwareDatetime

from riverwatch.analytics.errors import (
    InvalidAnalyticsQueryError,
)
from riverwatch.analytics.protocol import (
    AnalyticsReader,
)
from riverwatch.api.dependencies import (
    get_analytics_service,
)
from riverwatch.api.schemas.observations import (
    StationObservationResponse,
)

router = APIRouter(
    prefix="/stations",
    tags=[
        "observations",
    ],
)


@router.get(
    "/{station_id}/history",
    response_model=list[
        StationObservationResponse
    ],
    summary="Get station observation history",
)
def get_station_history(
    station_id: str,
    analytics: Annotated[
        AnalyticsReader,
        Depends(
            get_analytics_service
        ),
    ],
    start_at: Annotated[
        AwareDatetime | None,
        Query(
            description=(
                "Inclusive observation "
                "start time."
            ),
        ),
    ] = None,
    end_at: Annotated[
        AwareDatetime | None,
        Query(
            description=(
                "Exclusive observation "
                "end time."
            ),
        ),
    ] = None,
) -> list[
    StationObservationResponse
]:
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

    try:
        observations = (
            analytics.get_station_history(
                station_id,
                start_at=start_at,
                end_at=end_at,
            )
        )

    except InvalidAnalyticsQueryError as exc:
        raise HTTPException(
            status_code=(
                status
                .HTTP_422_UNPROCESSABLE_CONTENT
            ),
            detail=str(
                exc
            ),
        ) from exc

    return [
        StationObservationResponse
        .model_validate(
            observation
        )
        for observation in observations
    ]