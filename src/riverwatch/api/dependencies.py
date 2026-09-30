from fastapi import Request

from riverwatch.analytics.service import (
    AnalyticsService,
)


def get_analytics_service(
    request: Request,
) -> AnalyticsService:
    service = getattr(
        request.app.state,
        "analytics_service",
        None,
    )

    if not isinstance(
        service,
        AnalyticsService,
    ):
        raise RuntimeError(
            "AnalyticsService is not initialized"
        )

    return service