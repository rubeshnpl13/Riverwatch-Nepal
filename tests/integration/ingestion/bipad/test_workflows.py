import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import httpx
import respx

from riverwatch.ingestion.bipad.client import (
    BipadClient,
)
from riverwatch.ingestion.bipad.historical_service import (
    HistoricalRiverBackfillService,
)
from riverwatch.ingestion.bipad.service import (
    RiverStationIngestionService,
)
from riverwatch.ingestion.models import (
    HistoricalRiverIngestionBatch,
)


BASE_URL = "https://bipadportal.gov.np"

STATION_FIXTURE = Path(
    "tests/fixtures/bipad/river_station.json"
)

RIVER_FIXTURE = Path(
    "tests/fixtures/bipad/river_record.json"
)


def load_fixture(
    path: Path,
) -> dict[str, object]:
    return json.loads(
        path.read_text(
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
def test_current_station_ingestion_workflow_end_to_end() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    second_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
        "?limit=1000&offset=1000"
    )

    third_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
        "?limit=1000&offset=2000"
    )

    valid = load_fixture(
        STATION_FIXTURE
    )

    without_observation = load_fixture(
        STATION_FIXTURE
    )

    without_observation["id"] = 999
    without_observation[
        "stationSeriesId"
    ] = 99999
    without_observation[
        "waterLevel"
    ] = None
    without_observation[
        "waterLevelOn"
    ] = None

    invalid = load_fixture(
        STATION_FIXTURE
    )

    invalid["id"] = 1000
    invalid.pop(
        "point"
    )

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
                        valid,
                        without_observation,
                        invalid,
                    ],
                },
                request=request,
            )

        if offset == "1000":
            return httpx.Response(
                200,
                json={
                    "count": 9223372036854775807,
                    "next": third_url,
                    "previous": url,
                    "results": [],
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

    ingestion_time = datetime(
        2026,
        9,
        28,
        18,
        0,
        tzinfo=UTC,
    )

    logger = logging.getLogger(
        "riverwatch.integration.station"
    )

    with create_client() as client:
        service = RiverStationIngestionService(
            client=client,
            logger=logger,
            now=lambda: ingestion_time,
        )

        batch = service.run()

    assert len(route.calls) == 2

    assert len(
        batch.stations
    ) == 2

    assert len(
        batch.observations
    ) == 1

    assert len(
        batch.quarantined_records
    ) == 1

    metrics = batch.run_result.metrics

    assert metrics.records_received == 3
    assert metrics.records_valid == 2
    assert metrics.records_invalid == 1

    assert metrics.stations_emitted == 2

    assert (
        metrics.observations_emitted
        == 1
    )

    assert (
        metrics.records_without_observation
        == 1
    )

    assert (
        batch.observations[0].ingested_at
        == ingestion_time
    )

    assert (
        batch.quarantined_records[0]
        .record_index
        == 2
    )


@respx.mock
def test_historical_backfill_workflow_end_to_end() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
    )

    second_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
        "?limit=2&offset=2"
    )

    third_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
        "?limit=2&offset=4"
    )

    first = load_fixture(
        RIVER_FIXTURE
    )

    second = load_fixture(
        RIVER_FIXTURE
    )
    second["id"] = 20000001

    invalid = load_fixture(
        RIVER_FIXTURE
    )
    invalid["id"] = 20000002
    invalid.pop(
        "waterLevelOn"
    )

    fourth = load_fixture(
        RIVER_FIXTURE
    )
    fourth["id"] = 20000003

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
                        second,
                    ],
                },
                request=request,
            )

        if offset == "2":
            return httpx.Response(
                200,
                json={
                    "next": third_url,
                    "results": [
                        invalid,
                        fourth,
                    ],
                },
                request=request,
            )

        if offset == "4":
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

    ingestion_time = datetime(
        2026,
        9,
        28,
        18,
        30,
        tzinfo=UTC,
    )

    logger = logging.getLogger(
        "riverwatch.integration.backfill"
    )

    with create_client() as client:
        service = HistoricalRiverBackfillService(
            client=client,
            logger=logger,
            batch_handler=batches.append,
            now=lambda: ingestion_time,
        )

        result = service.run(
            page_limit=2,
            page_size=2,
        )

    assert len(route.calls) == 2

    assert len(batches) == 2

    assert len(
        batches[0].observations
    ) == 2

    assert len(
        batches[0].quarantined_records
    ) == 0

    assert len(
        batches[1].observations
    ) == 1

    assert len(
        batches[1].quarantined_records
    ) == 1

    quarantine = (
        batches[1]
        .quarantined_records[0]
    )

    # Page 1 contained indices 0 and 1.
    # Therefore the first record on page 2
    # must have global index 2.
    assert quarantine.record_index == 2

    assert result.metrics.records_received == 4
    assert result.metrics.records_valid == 3
    assert result.metrics.records_invalid == 1

    assert (
        result.metrics.observations_emitted
        == 3
    )

    all_observations = [
        observation
        for batch in batches
        for observation in batch.observations
    ]

    assert all(
        observation.ingested_at
        == ingestion_time
        for observation in all_observations
    )