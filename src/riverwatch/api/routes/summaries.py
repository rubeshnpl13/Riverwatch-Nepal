from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
)

from riverwatch.analytics.protocol import (
    AnalyticsReader,
)
from riverwatch.api.dependencies import (
    get_analytics_service,
)
from riverwatch.api.schemas.summaries import (
    BasinCurrentSummaryResponse,
    CurrentNetworkSummaryResponse,
)

router = APIRouter(
    tags=[
        "summaries",
    ],
)


@router.get(
    "/basins",
    response_model=list[
        BasinCurrentSummaryResponse
    ],
    summary="List current basin summaries",
)
def list_basin_summaries(
    analytics: Annotated[
        AnalyticsReader,
        Depends(
            get_analytics_service
        ),
    ],
) -> list[
    BasinCurrentSummaryResponse
]:
    basins = (
        analytics.get_basin_summaries()
    )

    return [
        BasinCurrentSummaryResponse
        .model_validate(
            basin
        )
        for basin in basins
    ]


@router.get(
    "/network/summary",
    response_model=(
        CurrentNetworkSummaryResponse
    ),
    summary="Get current network summary",
)
def get_network_summary(
    analytics: Annotated[
        AnalyticsReader,
        Depends(
            get_analytics_service
        ),
    ],
) -> CurrentNetworkSummaryResponse:
    summary = (
        analytics.get_network_summary()
    )

    return (
        CurrentNetworkSummaryResponse
        .model_validate(
            summary
        )
    )