from fastapi import Request

from riverwatch.analytics.protocol import (
    AnalyticsReader,
)


def get_analytics_service(
    request: Request,
) -> AnalyticsReader:
    service = getattr(
        request.app.state,
        "analytics_service",
        None,
    )

    if not isinstance(
        service,
        AnalyticsReader,
    ):
        raise RuntimeError(
            "AnalyticsService is not initialized"
        )

    return service