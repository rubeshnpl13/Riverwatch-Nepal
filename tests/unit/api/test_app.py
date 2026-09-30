from pathlib import Path
from unittest.mock import (
    MagicMock,
)

from fastapi.testclient import (
    TestClient,
)

from riverwatch.analytics.service import (
    AnalyticsService,
)
from riverwatch.api.app import (
    API_PREFIX,
    create_app,
)


def _mock_analytics_connection(
    monkeypatch,
) -> MagicMock:
    connection = MagicMock()

    monkeypatch.setattr(
        "riverwatch.api.app."
        "create_analytics_connection",
        lambda *,
        lake_root: connection,
    )

    return connection


def test_app_metadata() -> None:
    app = create_app()

    assert app.title == (
        "RiverWatch API"
    )

    assert app.version == "0.1.0"


def test_liveness_endpoint(
    monkeypatch,
) -> None:
    _mock_analytics_connection(
        monkeypatch
    )

    app = create_app()

    with TestClient(
        app
    ) as client:
        response = client.get(
            f"{API_PREFIX}/health/live"
        )

    assert response.status_code == 200

    assert response.json() == {
        "service": "riverwatch",
        "status": "ok",
    }


def test_unknown_api_route_returns_404(
    monkeypatch,
) -> None:
    _mock_analytics_connection(
        monkeypatch
    )

    app = create_app()

    with TestClient(
        app
    ) as client:
        response = client.get(
            f"{API_PREFIX}/does-not-exist"
        )

    assert response.status_code == 404


def test_lifespan_initializes_and_closes_analytics(
    monkeypatch,
    tmp_path: Path,
) -> None:
    connection = MagicMock()

    captured_lake_root: list[
        Path
    ] = []

    def fake_create_connection(
        *,
        lake_root: Path,
    ) -> MagicMock:
        captured_lake_root.append(
            lake_root
        )

        return connection

    monkeypatch.setattr(
        "riverwatch.api.app."
        "create_analytics_connection",
        fake_create_connection,
    )

    app = create_app(
        lake_root=tmp_path
    )

    assert getattr(
        app.state,
        "analytics_service",
        None,
    ) is None

    with TestClient(
        app
    ):
        service = getattr(
            app.state,
            "analytics_service",
            None,
        )

        assert isinstance(
            service,
            AnalyticsService,
        )

        assert captured_lake_root == [
            tmp_path
        ]

        connection.close.assert_not_called()

    connection.close.assert_called_once_with()

    assert (
        app.state.analytics_service
        is None
    )