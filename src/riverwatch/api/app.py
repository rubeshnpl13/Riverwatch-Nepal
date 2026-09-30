from collections.abc import (
    AsyncIterator,
)
from contextlib import (
    asynccontextmanager,
)
from pathlib import Path

from fastapi import FastAPI

from riverwatch.analytics.catalog import (
    create_analytics_connection,
)
from riverwatch.analytics.service import (
    AnalyticsService,
)
from riverwatch.api.routes.health import (
    router as health_router,
)

API_PREFIX = "/api/v1"

DEFAULT_LAKE_ROOT = Path(
    "data/lake"
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
) -> FastAPI:
    app = FastAPI(
        title="RiverWatch API",
        description=(
            "API for RiverWatch river monitoring "
            "and analytics."
        ),
        version="0.1.0",
        lifespan=lifespan,
    )

    app.state.lake_root = (
        DEFAULT_LAKE_ROOT
        if lake_root is None
        else lake_root
    )

    app.include_router(
        health_router,
        prefix=API_PREFIX,
    )

    return app