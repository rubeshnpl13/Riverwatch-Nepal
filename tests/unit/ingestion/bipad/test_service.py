import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import httpx
import respx

from riverwatch.ingestion.bipad.client import (
    BipadClient,
)
from riverwatch.ingestion.bipad.raw_writer import (
    BipadRawPageWriter,
)
from riverwatch.ingestion.bipad.service import (
    RiverStationIngestionService,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)

BASE_URL = "https://bipadportal.gov.np"

FIXTURE_PATH = Path(
    "tests/fixtures/bipad/river_station.json"
)


def load_station_fixture() -> dict[str, object]:
    return json.loads(
        FIXTURE_PATH.read_text(
            encoding="utf-8",
        )
    )


def create_client() -> BipadClient:
    return BipadClient(
        base_url=BASE_URL,
        api_version="v1",
        timeout_seconds=10,
        max_retries=0,
        sleep=lambda _: None,
    )

#Test successful ingestion
@respx.mock
def test_ingests_station_snapshot() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    station_payload = (
        load_station_fixture()
    )

    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "next": None,
                "results": [
                    station_payload,
                ],
            },
        )
    )

    ingestion_time = datetime(
        2026,
        9,
        28,
        12,
        0,
        tzinfo=UTC,
    )

    logger = logging.getLogger(
        "riverwatch.test.ingestion"
    )

    with create_client() as client:
        service = (
            RiverStationIngestionService(
                client=client,
                logger=logger,
                now=lambda: ingestion_time,
            )
        )

        batch = service.run()

    assert len(
        batch.stations
    ) == 1

    assert len(
        batch.observations
    ) == 1

    assert (
        batch.stations[0].station_id
        == "282"
    )

    assert (
        batch.observations[0].station_id
        == "282"
    )

    assert (
        batch.observations[0].ingested_at
        == ingestion_time
    )

    assert (
        batch.run_result.metrics.records_received
        == 1
    )

    assert (
        batch.run_result.metrics.records_valid
        == 1
    )

    assert (
        batch.run_result.metrics.stations_emitted
        == 1
    )

    assert (
        batch.run_result.metrics.observations_emitted
        == 1
    )

#Test station without observation
@respx.mock
def test_station_without_observation_is_counted() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    station_payload = (
        load_station_fixture()
    )

    station_payload[
        "waterLevel"
    ] = None

    station_payload[
        "waterLevelOn"
    ] = None

    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "next": None,
                "results": [
                    station_payload,
                ],
            },
        )
    )

    logger = logging.getLogger(
        "riverwatch.test.ingestion"
    )

    with create_client() as client:
        service = (
            RiverStationIngestionService(
                client=client,
                logger=logger,
            )
        )

        batch = service.run()

    assert len(
        batch.stations
    ) == 1

    assert len(
        batch.observations
    ) == 0

    assert (
        batch.run_result.metrics
        .records_without_observation
        == 1
    )

#Test quarantine doesn't stop ingestion

@respx.mock
def test_invalid_station_is_quarantined_without_stopping_batch() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    first = load_station_fixture()

    invalid = load_station_fixture()
    invalid.pop(
        "point"
    )

    third = load_station_fixture()
    third["id"] = 999
    third["title"] = (
        "Second Valid Station"
    )

    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "next": None,
                "results": [
                    first,
                    invalid,
                    third,
                ],
            },
        )
    )

    logger = logging.getLogger(
        "riverwatch.test.ingestion"
    )

    with create_client() as client:
        service = (
            RiverStationIngestionService(
                client=client,
                logger=logger,
            )
        )

        batch = service.run()

    assert len(
        batch.stations
    ) == 2

    assert len(
        batch.quarantined_records
    ) == 1

    metrics = (
        batch.run_result.metrics
    )

    assert (
        metrics.records_received
        == 3
    )

    assert (
        metrics.records_valid
        == 2
    )

    assert (
        metrics.records_invalid
        == 1
    )

    assert (
        metrics.stations_emitted
        == 2
    )

#Test pagination + ingestion together
@respx.mock
def test_ingestion_processes_multiple_pages() -> None:
    first_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    second_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
        "?offset=1"
    )

    first_station = (
        load_station_fixture()
    )

    second_station = (
        load_station_fixture()
    )

    second_station["id"] = 999
    second_station[
        "stationSeriesId"
    ] = 99999

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        offset = request.url.params.get(
            "offset"
        )

        if offset is None:
            return httpx.Response(
                200,
                json={
                    "next": second_url,
                    "results": [
                        first_station,
                    ],
                },
                request=request,
            )

        if offset == "1":
            return httpx.Response(
                200,
                json={
                    "next": None,
                    "results": [
                        second_station,
                    ],
                },
                request=request,
            )

        raise AssertionError(
            f"Unexpected request URL: {request.url}"
        )

    route = respx.get(
        first_url
    ).mock(
        side_effect=responder
    )

    logger = logging.getLogger(
        "riverwatch.test.ingestion"
    )

    with create_client() as client:
        service = (
            RiverStationIngestionService(
                client=client,
                logger=logger,
            )
        )

        batch = service.run()

    assert len(
        batch.stations
    ) == 2

    assert {
        station.station_id
        for station in batch.stations
    } == {
        "282",
        "999",
    }

    assert len(route.calls) == 2

#Test common ingestion timestamp
@respx.mock
def test_batch_uses_single_ingestion_timestamp() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    first = load_station_fixture()

    second = load_station_fixture()
    second["id"] = 999

    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "next": None,
                "results": [
                    first,
                    second,
                ],
            },
        )
    )

    ingestion_time = datetime(
        2026,
        9,
        28,
        15,
        30,
        tzinfo=UTC,
    )

    logger = logging.getLogger(
        "riverwatch.test.ingestion"
    )

    with create_client() as client:
        service = (
            RiverStationIngestionService(
                client=client,
                logger=logger,
                now=lambda: ingestion_time,
            )
        )

        batch = service.run()

    assert all(
        observation.ingested_at
        == ingestion_time
        for observation in (
            batch.observations
        )
    )

#persistence test

@respx.mock
def test_station_ingestion_archives_raw_pages(
    tmp_path: Path,
) -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    second_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
        "?limit=1000&offset=1000"
    )

    station = load_station_fixture()

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        offset = request.url.params.get(
            "offset"
        )

        if offset is None:
            return httpx.Response(
                200,
                json={
                    "count": 9223372036854775807,
                    "next": second_url,
                    "previous": None,
                    "results": [
                        station,
                    ],
                },
                request=request,
            )

        if offset == "1000":
            return httpx.Response(
                200,
                json={
                    "count": 9223372036854775807,
                    "next": (
                        "https://bipadportal.gov.np/"
                        "api/v1/river-stations/"
                        "?limit=1000&offset=2000"
                    ),
                    "previous": url,
                    "results": [],
                },
                request=request,
            )

        raise AssertionError(
            f"Unexpected URL: {request.url}"
        )

    respx.get(
        url
    ).mock(
        side_effect=responder
    )

    store = LocalObjectStore(
        root=tmp_path
    )

    raw_writer = BipadRawPageWriter(
        store=store
    )

    ingestion_time = datetime(
        2026,
        9,
        29,
        2,
        0,
        tzinfo=UTC,
    )

    logger = logging.getLogger(
        "riverwatch.test.ingestion"
    )

    with create_client() as client:
        service = RiverStationIngestionService(
            client=client,
            logger=logger,
            raw_writer=raw_writer,
            now=lambda: ingestion_time,
        )

        batch = service.run()

    assert len(
        batch.stations
    ) == 1

    payload_files = sorted(
        tmp_path.rglob(
            "payload.json"
        )
    )

    metadata_files = sorted(
        tmp_path.rglob(
            "metadata.json"
        )
    )

    assert len(
        payload_files
    ) == 2

    assert len(
        metadata_files
    ) == 2

    assert (
        "page=000000"
        in str(payload_files[0])
    )

    assert (
        "page=000001"
        in str(payload_files[1])
    )