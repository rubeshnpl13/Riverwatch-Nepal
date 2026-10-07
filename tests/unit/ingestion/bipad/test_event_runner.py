import json
import logging
from pathlib import Path
from uuid import UUID

import httpx
import pytest
import respx

from riverwatch.config import (
    Settings,
)
from riverwatch.ingestion.bipad.event_runner import (
    BipadEventIngestionRunner,
    UnsupportedIngestionRequestError,
)
from riverwatch.ingestion.bipad.run_repository import (
    LocalIngestionRunRepository,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)

BASE_URL = (
    "https://bipadportal.gov.np"
)

STATION_URL = (
    "https://bipadportal.gov.np/"
    "api/v1/river-stations/"
)

STATION_FIXTURE = Path(
    "tests/fixtures/bipad/"
    "river_station.json"
)

REQUEST_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)


def load_station_fixture() -> dict[
    str,
    object,
]:
    return json.loads(
        STATION_FIXTURE.read_text(
            encoding="utf-8"
        )
    )


def make_settings() -> Settings:
    return Settings(
        bipad_base_url=BASE_URL,
        bipad_api_version="v1",
        http_timeout_seconds=10,
        http_max_retries=0,
    )


def make_runner(
    lake_root: Path,
) -> BipadEventIngestionRunner:
    return BipadEventIngestionRunner(
        lake_root=lake_root,
        settings=make_settings(),
        logger=logging.getLogger(
            "riverwatch.test.event_runner"
        ),
    )


@respx.mock
def test_runner_writes_completed_lake_run(
    tmp_path: Path,
) -> None:
    route = respx.get(
        STATION_URL
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 1,
                "next": None,
                "previous": None,
                "results": [
                    load_station_fixture()
                ],
            },
        )
    )

    runner = make_runner(
        tmp_path
    )

    result = runner.run(
        provider="bipad",
        endpoint="river-stations",
        idempotency_key=REQUEST_ID,
    )

    assert (
        result.run_id
        == (
            "event-"
            f"{REQUEST_ID}"
            "-attempt-0001"
        )
    )

    assert (
        tmp_path
        / result.manifest_path
    ).is_file()

    raw_payloads = list(
        (
            tmp_path
            / "raw"
        ).rglob(
            "payload.json"
        )
    )

    assert len(
        raw_payloads
    ) == 1

    assert len(
        route.calls
    ) == 1


@respx.mock
def test_runner_recovers_completed_run_without_refetch(
    tmp_path: Path,
) -> None:
    route = respx.get(
        STATION_URL
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 1,
                "next": None,
                "previous": None,
                "results": [
                    load_station_fixture()
                ],
            },
        )
    )

    runner = make_runner(
        tmp_path
    )

    first = runner.run(
        provider="bipad",
        endpoint="river-stations",
        idempotency_key=REQUEST_ID,
    )

    second = runner.run(
        provider="bipad",
        endpoint="river-stations",
        idempotency_key=REQUEST_ID,
    )

    assert second == first

    assert len(
        route.calls
    ) == 1


@respx.mock
def test_runner_uses_next_attempt_after_partial_run(
    tmp_path: Path,
) -> None:
    attempt_one = (
        "event-"
        f"{REQUEST_ID}"
        "-attempt-0001"
    )

    partial_run = (
        tmp_path
        / "raw"
        / "provider=bipad"
        / "endpoint=river-stations"
        / "year=2026"
        / "month=10"
        / "day=03"
        / "hour=00"
        / f"run_id={attempt_one}"
    )

    partial_run.mkdir(
        parents=True,
        exist_ok=True,
    )

    respx.get(
        STATION_URL
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 1,
                "next": None,
                "previous": None,
                "results": [
                    load_station_fixture()
                ],
            },
        )
    )

    runner = make_runner(
        tmp_path
    )

    result = runner.run(
        provider="bipad",
        endpoint="river-stations",
        idempotency_key=REQUEST_ID,
    )

    assert (
        result.run_id
        == (
            "event-"
            f"{REQUEST_ID}"
            "-attempt-0002"
        )
    )


def test_runner_rejects_unsupported_provider(
    tmp_path: Path,
) -> None:
    runner = make_runner(
        tmp_path
    )

    with pytest.raises(
        UnsupportedIngestionRequestError,
        match=(
            "Unsupported ingestion "
            "provider"
        ),
    ):
        runner.run(
            provider="other",
            endpoint="river-stations",
            idempotency_key=REQUEST_ID,
        )


def test_runner_rejects_unsupported_endpoint(
    tmp_path: Path,
) -> None:
    runner = make_runner(
        tmp_path
    )

    with pytest.raises(
        UnsupportedIngestionRequestError,
        match=(
            "Unsupported BIPAD "
            "ingestion endpoint"
        ),
    ):
        runner.run(
            provider="bipad",
            endpoint="river",
            idempotency_key=REQUEST_ID,
        )

def test_runner_rejects_store_without_repository(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    with pytest.raises(
        ValueError,
        match="must be provided together",
    ):
        BipadEventIngestionRunner(
            lake_root=tmp_path,
            settings=make_settings(),
            store=store,
        )


def test_runner_rejects_repository_without_store(
    tmp_path: Path,
) -> None:
    repository = (
        LocalIngestionRunRepository(
            lake_root=tmp_path
        )
    )

    with pytest.raises(
        ValueError,
        match="must be provided together",
    ):
        BipadEventIngestionRunner(
            lake_root=tmp_path,
            settings=make_settings(),
            run_repository=repository,
        )

def test_runner_requires_lake_root_for_local_backend() -> None:
    with pytest.raises(
        ValueError,
        match="lake_root must be provided",
    ):
        BipadEventIngestionRunner(
            settings=make_settings(),
        )


def test_runner_allows_injected_backends_without_lake_root(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path,
    )

    repository = (
        LocalIngestionRunRepository(
            lake_root=tmp_path,
        )
    )

    runner = BipadEventIngestionRunner(
        settings=make_settings(),
        store=store,
        run_repository=repository,
    )

    assert isinstance(
        runner,
        BipadEventIngestionRunner,
    )