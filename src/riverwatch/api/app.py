from collections.abc import (
    AsyncIterator,
)
from contextlib import (
    asynccontextmanager,
)
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import (
    CORSMiddleware,
)

from riverwatch.analytics.catalog import (
    create_analytics_connection,
)
from riverwatch.analytics.service import (
    AnalyticsService,
)
from riverwatch.api.routes.health import (
    router as health_router,
)
from riverwatch.api.routes.observations import (
    router as observations_router,
)
from riverwatch.api.routes.stations import (
    router as stations_router,
)
from riverwatch.api.routes.summaries import (
    router as summaries_router,
)

API_PREFIX = "/api/v1"

DEFAULT_LAKE_ROOT = Path(
    "data/lake"
)

DEFAULT_CORS_ORIGINS = (
    "http://127.0.0.1:5173",
    "http://localhost:5173",
)


@asynccontextmanager
async def lifespan(
    app: FastAPI,
) -> AsyncIterator[None]:
    lake_root: Path = (
        app.state.lake_root
    )

    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    try:
        app.state.analytics_service = (
            AnalyticsService(
                connection=connection
            )
        )

        yield

    finally:
        app.state.analytics_service = None

        connection.close()


def create_app(
    *,
    lake_root: Path | None = None,
    cors_origins: (
        tuple[str, ...] | None
    ) = None,
) -> FastAPI:
    app = FastAPI(
        title="RiverWatch API",
        description=(
            "Read-only API for RiverWatch river "
            "monitoring and analytics. "
            "RiverWatch freshness and quality "
            "metrics are not official flood "
            "warnings."
        ),
        version="0.1.0",
        lifespan=lifespan,
    )

    app.state.lake_root = (
        DEFAULT_LAKE_ROOT
        if lake_root is None
        else lake_root
    )

    origins = (
        DEFAULT_CORS_ORIGINS
        if cors_origins is None
        else cors_origins
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(
            origins
        ),
        allow_credentials=False,
        allow_methods=[
            "GET",
        ],
        allow_headers=[
            "Accept",
            "Content-Type",
        ],
    )

    app.include_router(
        health_router,
        prefix=API_PREFIX,
    )

    app.include_router(
        stations_router,
        prefix=API_PREFIX,
    )

    app.include_router(
        observations_router,
        prefix=API_PREFIX,
    )

    app.include_router(
        summaries_router,
        prefix=API_PREFIX,
    )

    return app