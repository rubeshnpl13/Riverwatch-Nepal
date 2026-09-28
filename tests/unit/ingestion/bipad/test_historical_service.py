import json
import logging
from datetime import UTC, datetime
from pathlib import Path
import pytest
import httpx
import respx

from riverwatch.ingestion.bipad.client import BipadClient
from riverwatch.ingestion.bipad.historical_service import (
    HistoricalRiverBackfillService,
)
from riverwatch.ingestion.models import (
    HistoricalRiverIngestionBatch,
)

BASE_URL = "https://bipadportal.gov.np"

FIXTURE_PATH = Path(
    "tests/fixtures/bipad/river_record.json"
)


def load_river_fixture() -> dict[str, object]:
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


@respx.mock
def test_backfill_processes_historical_page() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
    )

    record = load_river_fixture()

    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "next": None,
                "results": [
                    record,
                ],
            },
        )
    )

    batches: list[
        HistoricalRiverIngestionBatch
    ] = []

    ingestion_time = datetime(
        2026,
        9,
        28,
        16,
        0,
        tzinfo=UTC,
    )

    logger = logging.getLogger(
        "riverwatch.test.backfill"
    )

    with create_client() as client:
        service = HistoricalRiverBackfillService(
            client=client,
            logger=logger,
            batch_handler=batches.append,
            now=lambda: ingestion_time,
        )

        result = service.run(
            page_limit=1
        )

    assert len(batches) == 1

    batch = batches[0]

    assert batch.records_received == 1

    assert len(
        batch.observations
    ) == 1

    assert (
        batch.observations[0].station_id
        == "44"
    )

    assert (
        batch.observations[0].ingested_at
        == ingestion_time
    )

    assert (
        result.metrics.records_received
        == 1
    )

    assert (
        result.metrics.observations_emitted
        == 1
    )


@respx.mock
def test_backfill_emits_one_batch_per_page() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
    )

    second_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
        "?limit=1&offset=1"
    )

    first = load_river_fixture()

    second = load_river_fixture()
    second["id"] = 99999999

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        offset = request.url.params.get(
            "offset"
        )

        if offset in {
            None,
            "0",
        }:
            return httpx.Response(
                200,
                json={
                    "next": second_url,
                    "results": [
                        first,
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
                        second,
                    ],
                },
                request=request,
            )

        raise AssertionError(
            f"Unexpected URL: {request.url}"
        )

    route = respx.get(
        url
    ).mock(
        side_effect=responder
    )

    batches: list[
        HistoricalRiverIngestionBatch
    ] = []

    logger = logging.getLogger(
        "riverwatch.test.backfill"
    )

    with create_client() as client:
        service = HistoricalRiverBackfillService(
            client=client,
            logger=logger,
            batch_handler=batches.append,
        )

        result = service.run(
            page_limit=2,
            page_size=1,
        )

    assert len(batches) == 2

    assert (
        batches[0].batch_index
        == 0
    )

    assert (
        batches[1].batch_index
        == 1
    )

    assert (
        result.metrics.records_received
        == 2
    )

    assert (
        result.metrics.observations_emitted
        == 2
    )

    assert len(route.calls) == 2


@respx.mock
def test_backfill_quarantines_invalid_historical_record() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
    )

    valid = load_river_fixture()

    invalid = load_river_fixture()
    invalid.pop(
        "waterLevelOn"
    )

    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "next": None,
                "results": [
                    valid,
                    invalid,
                ],
            },
        )
    )

    batches: list[
        HistoricalRiverIngestionBatch
    ] = []

    logger = logging.getLogger(
        "riverwatch.test.backfill"
    )

    with create_client() as client:
        service = HistoricalRiverBackfillService(
            client=client,
            logger=logger,
            batch_handler=batches.append,
        )

        result = service.run(
            page_limit=1
        )

    assert len(batches) == 1

    assert len(
        batches[0].observations
    ) == 1

    assert len(
        batches[0].quarantined_records
    ) == 1

    assert (
        result.metrics.records_received
        == 2
    )

    assert (
        result.metrics.records_valid
        == 1
    )

    assert (
        result.metrics.records_invalid
        == 1
    )


@respx.mock
def test_backfill_page_limit_stops_without_requesting_next_page() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
    )

    second_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
        "?limit=1&offset=1"
    )

    third_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
        "?limit=1&offset=2"
    )

    first = load_river_fixture()

    second = load_river_fixture()
    second["id"] = 99999999

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        offset = request.url.params.get(
            "offset"
        )

        if offset in {
            None,
            "0",
        }:
            return httpx.Response(
                200,
                json={
                    "next": second_url,
                    "results": [
                        first,
                    ],
                },
                request=request,
            )

        if offset == "1":
            return httpx.Response(
                200,
                json={
                    "next": third_url,
                    "results": [
                        second,
                    ],
                },
                request=request,
            )

        if offset == "2":
            raise AssertionError(
                "Third page must not be requested"
            )

        raise AssertionError(
            f"Unexpected URL: {request.url}"
        )

    route = respx.get(
        url
    ).mock(
        side_effect=responder
    )

    batches: list[
        HistoricalRiverIngestionBatch
    ] = []

    logger = logging.getLogger(
        "riverwatch.test.backfill"
    )

    with create_client() as client:
        service = HistoricalRiverBackfillService(
            client=client,
            logger=logger,
            batch_handler=batches.append,
        )

        result = service.run(
            page_limit=2,
            page_size=1,
        )

    assert len(batches) == 2

    assert (
        result.metrics.records_received
        == 2
    )

    assert (
        result.metrics.observations_emitted
        == 2
    )

    assert len(route.calls) == 2

def test_backfill_rejects_invalid_page_limit() -> None:
    logger = logging.getLogger(
        "riverwatch.test.backfill"
    )

    with create_client() as client:
        service = HistoricalRiverBackfillService(
            client=client,
            logger=logger,
            batch_handler=lambda _: None,
        )

        with pytest.raises(
            ValueError,
            match="page_limit",
        ):
            service.run(
                page_limit=0
            )

def test_backfill_rejects_page_limit_above_safety_limit() -> None:
    logger = logging.getLogger(
        "riverwatch.test.backfill"
    )

    with create_client() as client:
        service = HistoricalRiverBackfillService(
            client=client,
            logger=logger,
            batch_handler=lambda _: None,
        )

        with pytest.raises(
            ValueError,
            match="page_limit",
        ):
            service.run(
                page_limit=11,
                pagination_safety_limit=10,
            )