from unittest.mock import (
    MagicMock,
)

import pytest
from fastapi import Request

from riverwatch.analytics.service import (
    AnalyticsService,
)
from riverwatch.api.app import (
    create_app,
)
from riverwatch.api.dependencies import (
    get_analytics_service,
)


def test_get_analytics_service() -> None:
    app = create_app()

    service = AnalyticsService(
        connection=MagicMock()
    )

    app.state.analytics_service = (
        service
    )

    request = Request(
        {
            "type": "http",
            "app": app,
        }
    )

    assert (
        get_analytics_service(
            request
        )
        is service
    )


def test_get_analytics_service_fails_when_not_initialized() -> None:
    app = create_app()

    request = Request(
        {
            "type": "http",
            "app": app,
        }
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "AnalyticsService "
            "is not initialized"
        ),
    ):
        get_analytics_service(
            request
        )