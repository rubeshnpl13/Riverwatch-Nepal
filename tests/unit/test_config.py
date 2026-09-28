from riverwatch.config import Settings


def test_default_settings() -> None:
    settings = Settings()

    assert settings.app_env == "local"
    assert settings.http_timeout_seconds > 0
    assert settings.http_max_retries >= 0