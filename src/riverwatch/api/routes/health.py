from fastapi import APIRouter

from riverwatch.api.schemas.health import (
    HealthResponse,
)

router = APIRouter(
    prefix="/health",
    tags=[
        "health",
    ],
)


@router.get(
    "/live",
    response_model=HealthResponse,
    summary="Check API liveness",
)
def get_liveness() -> HealthResponse:
    return HealthResponse(
        service="riverwatch",
        status="ok",
    )