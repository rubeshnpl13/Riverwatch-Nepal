import json
from pathlib import Path

import httpx
import pytest
import respx
from pydantic import ValidationError

from riverwatch.ingestion.bipad.client import (
    BipadClient,
    is_retryable_bipad_exception,
)
from riverwatch.ingestion.bipad.endpoints import BipadEndpoint
from riverwatch.ingestion.bipad.errors import (
    BipadHTTPError,
    BipadPaginationLimitError,
    BipadPaginationLoopError,
    BipadRequestError,
    BipadResponseDecodeError,
    BipadTimeoutError,
)
from riverwatch.ingestion.bipad.schemas import (
    BipadRiverRecord,
    BipadRiverStation,
)

RIVER_STATION_FIXTURE = Path(
    "tests/fixtures/bipad/river_station.json"
)

RIVER_RECORD_FIXTURE = Path(
    "tests/fixtures/bipad/river_record.json"
)

BASE_URL = "https://bipadportal.gov.np"
def load_json_fixture(
    path: Path,
) -> dict[str, object]:
    return json.loads(
        path.read_text(
            encoding="utf-8",
        )
    )

def create_client(
    *,
    max_retries: int = 0,
) -> BipadClient:
    return BipadClient(
        base_url=BASE_URL,
        api_version="v1",
        timeout_seconds=10,
        max_retries=max_retries,
        sleep=lambda _: None,
    )


def test_builds_river_stations_url() -> None:
    with create_client() as client:
        url = client.build_endpoint_url(
            BipadEndpoint.RIVER_STATIONS
        )

    assert url == (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )


@respx.mock
def test_get_json_returns_payload() -> None:
    route = respx.get(
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 1,
                "next": None,
                "previous": None,
                "results": [
                    {
                        "id": 282,
                        "title": (
                            "Banara River at EW Highway"
                        ),
                    }
                ],
            },
        )
    )

    with create_client() as client:
        payload = client.get_json(
            BipadEndpoint.RIVER_STATIONS
        )

    assert route.called
    assert payload["count"] == 1
    assert (
        payload["results"][0]["id"]
        == 282
    )


@respx.mock
def test_get_json_sends_query_parameters() -> None:
    route = respx.get(
        "https://bipadportal.gov.np/"
        "api/v1/river/"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 0,
                "next": None,
                "previous": None,
                "results": [],
            },
        )
    )

    with create_client() as client:
        client.get_json(
            BipadEndpoint.RIVER,
            params={
                "limit": 1000,
                "offset": 0,
            },
        )

    request = route.calls.last.request

    assert request.url.params["limit"] == "1000"
    assert request.url.params["offset"] == "0"


@respx.mock
def test_http_error_is_translated() -> None:
    respx.get(
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    ).mock(
        return_value=httpx.Response(
            503,
            json={
                "detail": "Service unavailable",
            },
        )
    )

    with create_client() as client:
        with pytest.raises(
            BipadHTTPError
        ) as error:
            client.get_json(
                BipadEndpoint.RIVER_STATIONS
            )

    assert error.value.status_code == 503


@respx.mock
def test_invalid_json_is_rejected() -> None:
    respx.get(
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    ).mock(
        return_value=httpx.Response(
            200,
            text="<html>Not JSON</html>",
            headers={
                "Content-Type": "text/html",
            },
        )
    )

    with create_client() as client:
        with pytest.raises(
            BipadResponseDecodeError
        ):
            client.get_json(
                BipadEndpoint.RIVER_STATIONS
            )


@respx.mock
def test_timeout_is_translated() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    respx.get(url).mock(
        side_effect=httpx.ReadTimeout(
            "Request timed out",
            request=httpx.Request(
                "GET",
                url,
            ),
        )
    )

    with create_client() as client:
        with pytest.raises(
            BipadTimeoutError
        ):
            client.get_json(
                BipadEndpoint.RIVER_STATIONS
            )


@respx.mock
def test_network_error_is_translated() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    respx.get(url).mock(
        side_effect=httpx.ConnectError(
            "Unable to connect",
            request=httpx.Request(
                "GET",
                url,
            ),
        )
    )

    with create_client() as client:
        with pytest.raises(
            BipadRequestError
        ):
            client.get_json(
                BipadEndpoint.RIVER_STATIONS
            )


@respx.mock
def test_retries_retryable_http_error() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    attempts = 0

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempts

        attempts += 1

        if attempts < 3:
            return httpx.Response(
                503,
                request=request,
            )

        return httpx.Response(
            200,
            json={
                "results": [],
            },
            request=request,
        )

    route = respx.get(url).mock(
        side_effect=responder
    )

    with create_client(
        max_retries=2
    ) as client:
        payload = client.get_json(
            BipadEndpoint.RIVER_STATIONS
        )

    assert payload == {
        "results": [],
    }
    assert len(route.calls) == 3


@respx.mock
def test_does_not_retry_non_retryable_http_error() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    route = respx.get(url).mock(
        return_value=httpx.Response(
            400,
            json={
                "detail": "Bad request",
            },
        )
    )

    with create_client(
        max_retries=3
    ) as client:
        with pytest.raises(
            BipadHTTPError
        ):
            client.get_json(
                BipadEndpoint.RIVER_STATIONS
            )

    assert len(route.calls) == 1


@respx.mock
def test_retries_timeout_then_succeeds() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    attempts = 0

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempts

        attempts += 1

        if attempts == 1:
            raise httpx.ReadTimeout(
                "Request timed out",
                request=request,
            )

        return httpx.Response(
            200,
            json={
                "results": [],
            },
            request=request,
        )

    route = respx.get(url).mock(
        side_effect=responder
    )

    with create_client(
        max_retries=1
    ) as client:
        payload = client.get_json(
            BipadEndpoint.RIVER_STATIONS
        )

    assert payload == {
        "results": [],
    }
    assert len(route.calls) == 2


@respx.mock
def test_retryable_error_fails_after_retries_exhausted() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    route = respx.get(url).mock(
        return_value=httpx.Response(
            503,
            json={
                "detail": "Unavailable",
            },
        )
    )

    with create_client(
        max_retries=2
    ) as client:
        with pytest.raises(
            BipadHTTPError
        ) as error:
            client.get_json(
                BipadEndpoint.RIVER_STATIONS
            )

    assert error.value.status_code == 503
    assert len(route.calls) == 3


@respx.mock
def test_invalid_json_is_not_retried() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    route = respx.get(url).mock(
        return_value=httpx.Response(
            200,
            text="<html>broken</html>",
        )
    )

    with create_client(
        max_retries=3
    ) as client:
        with pytest.raises(
            BipadResponseDecodeError
        ):
            client.get_json(
                BipadEndpoint.RIVER_STATIONS
            )

    assert len(route.calls) == 1


@pytest.mark.parametrize(
    "status_code",
    [
        408,
        429,
        500,
        502,
        503,
        504,
    ],
)
def test_retryable_http_status_codes(
    status_code: int,
) -> None:
    error = BipadHTTPError(
        status_code=status_code,
        url="https://example.com",
    )

    assert is_retryable_bipad_exception(
        error
    )


@pytest.mark.parametrize(
    "status_code",
    [
        400,
        401,
        403,
        404,
    ],
)
def test_non_retryable_http_status_codes(
    status_code: int,
) -> None:
    error = BipadHTTPError(
        status_code=status_code,
        url="https://example.com",
    )

    assert not is_retryable_bipad_exception(
        error
    )


@respx.mock
def test_iter_pages_follows_next_links() -> None:
    first_url = (
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

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        offset = request.url.params.get("offset")

        if offset is None:
            return httpx.Response(
                200,
                json={
                    "count": 9223372036854775807,
                    "next": second_url,
                    "previous": None,
                    "results": [
                        {"id": 1},
                        {"id": 2},
                    ],
                },
                request=request,
            )

        if offset == "2":
            return httpx.Response(
                200,
                json={
                    "count": 9223372036854775807,
                    "next": third_url,
                    "previous": first_url,
                    "results": [
                        {"id": 3},
                        {"id": 4},
                    ],
                },
                request=request,
            )

        if offset == "4":
            return httpx.Response(
                200,
                json={
                    "count": 9223372036854775807,
                    "next": None,
                    "previous": second_url,
                    "results": [
                        {"id": 5},
                    ],
                },
                request=request,
            )

        raise AssertionError(
            f"Unexpected request URL: {request.url}"
        )

    route = respx.get(first_url).mock(
        side_effect=responder
    )

    with create_client() as client:
        pages = list(
            client.iter_pages(
                BipadEndpoint.RIVER,
            )
        )

    assert len(pages) == 3

    assert pages[0].results == [
        {"id": 1},
        {"id": 2},
    ]

    assert pages[1].results == [
        {"id": 3},
        {"id": 4},
    ]

    assert pages[2].results == [
        {"id": 5},
    ]

    assert len(route.calls) == 3


@respx.mock
def test_iter_records_flattens_pages() -> None:
    first_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
    )

    second_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
        "?offset=2"
    )

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        offset = request.url.params.get("offset")

        if offset is None:
            return httpx.Response(
                200,
                json={
                    "next": second_url,
                    "results": [
                        {"id": 1},
                        {"id": 2},
                    ],
                },
                request=request,
            )

        if offset == "2":
            return httpx.Response(
                200,
                json={
                    "next": None,
                    "results": [
                        {"id": 3},
                    ],
                },
                request=request,
            )

        raise AssertionError(
            f"Unexpected request URL: {request.url}"
        )

    route = respx.get(first_url).mock(
        side_effect=responder
    )

    with create_client() as client:
        records = list(
            client.iter_records(
                BipadEndpoint.RIVER
            )
        )

    assert records == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]

    assert len(route.calls) == 2


@respx.mock
def test_iter_records_handles_empty_dataset() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/streamflow/"
    )

    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 0,
                "next": None,
                "previous": None,
                "results": [],
            },
        )
    )

    with create_client() as client:
        records = list(
            client.iter_records(
                BipadEndpoint.STREAMFLOW
            )
        )

    assert records == []


@respx.mock
def test_pagination_stops_at_max_pages() -> None:
    first_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
    )

    second_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/?offset=1"
    )

    third_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/?offset=2"
    )

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        offset = request.url.params.get("offset")

        if offset is None:
            return httpx.Response(
                200,
                json={
                    "next": second_url,
                    "results": [
                        {"id": 1},
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
                        {"id": 2},
                    ],
                },
                request=request,
            )

        raise AssertionError(
            f"Unexpected request URL: {request.url}"
        )

    respx.get(first_url).mock(
        side_effect=responder
    )

    with create_client() as client:
        with pytest.raises(
            BipadPaginationLimitError
        ):
            list(
                client.iter_pages(
                    BipadEndpoint.RIVER,
                    max_pages=2,
                )
            )


@respx.mock
def test_pagination_detects_repeated_url() -> None:
    first_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
    )

    second_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/?offset=1"
    )

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        offset = request.url.params.get("offset")

        if offset is None:
            return httpx.Response(
                200,
                json={
                    "next": second_url,
                    "results": [
                        {"id": 1},
                    ],
                },
                request=request,
            )

        if offset == "1":
            return httpx.Response(
                200,
                json={
                    "next": second_url,
                    "results": [
                        {"id": 2},
                    ],
                },
                request=request,
            )

        raise AssertionError(
            f"Unexpected request URL: {request.url}"
        )

    respx.get(first_url).mock(
        side_effect=responder
    )

    with create_client() as client:
        with pytest.raises(
            BipadPaginationLoopError
        ):
            list(
                client.iter_pages(
                    BipadEndpoint.RIVER,
                )
            )


def test_pagination_rejects_invalid_max_pages() -> None:
    with create_client() as client:
        with pytest.raises(
            ValueError
        ):
            list(
                client.iter_pages(
                    BipadEndpoint.RIVER,
                    max_pages=0,
                )
            )

@respx.mock
def test_iter_river_stations_returns_typed_models() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    station_payload = load_json_fixture(
        RIVER_STATION_FIXTURE
    )

    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 1,
                "next": None,
                "previous": None,
                "results": [
                    station_payload,
                ],
            },
        )
    )

    with create_client() as client:
        stations = list(
            client.iter_river_stations()
        )

    assert len(stations) == 1

    station = stations[0]

    assert isinstance(
        station,
        BipadRiverStation,
    )

    assert station.id == 282
    assert station.station_series_id == 36640
    assert station.water_level == 1.215

#Test historical typed records
@respx.mock
def test_iter_river_records_returns_typed_models() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
    )

    river_payload = load_json_fixture(
        RIVER_RECORD_FIXTURE
    )

    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 1,
                "next": None,
                "previous": None,
                "results": [
                    river_payload,
                ],
            },
        )
    )

    with create_client() as client:
        records = list(
            client.iter_river_records()
        )

    assert len(records) == 1

    record = records[0]

    assert isinstance(
        record,
        BipadRiverRecord,
    )

    assert record.id == 18707479

    assert record.station == 44

    assert record.station_series_id == 19260

@respx.mock
def test_typed_station_iteration_follows_pagination() -> None:
    first_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    second_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/?offset=1"
    )

    station_payload = load_json_fixture(
        RIVER_STATION_FIXTURE
    )

    second_station = {
        **station_payload,
        "id": 999,
        "title": "Test River Station",
        "stationSeriesId": 99999,
    }

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        offset = request.url.params.get("offset")

        if offset is None:
            return httpx.Response(
                200,
                json={
                    "next": second_url,
                    "results": [
                        station_payload,
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

    route = respx.get(first_url).mock(
        side_effect=responder
    )

    with create_client() as client:
        stations = list(
            client.iter_river_stations()
        )

    assert len(stations) == 2

    assert isinstance(
        stations[0],
        BipadRiverStation,
    )
    assert isinstance(
        stations[1],
        BipadRiverStation,
    )

    assert stations[0].id == station_payload["id"]
    assert stations[1].id == 999
    assert stations[1].title == "Test River Station"
    assert stations[1].station_series_id == 99999

    assert len(route.calls) == 2

#malformed records
@respx.mock
def test_typed_station_iteration_rejects_invalid_record() -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river-stations/"
    )

    invalid_station = {
        "id": 282,
        "title": "Broken Station",
    }

    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "next": None,
                "results": [
                    invalid_station,
                ],
            },
        )
    )

    with create_client() as client:
        with pytest.raises(
            ValidationError
        ):
            list(
                client.iter_river_stations()
            )

#regression test
@respx.mock
def test_pagination_stops_on_empty_page_even_with_next_url() -> None:
    first_url = (
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
                        {
                            "id": 1,
                        }
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
                    "previous": first_url,
                    "results": [],
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

    with create_client() as client:
        pages = list(
            client.iter_pages(
                BipadEndpoint.RIVER_STATIONS
            )
        )

    assert len(pages) == 2

    assert pages[0].results == [
        {
            "id": 1,
        }
    ]

    assert pages[1].results == []

    # Most importantly, offset=2000 was never fetched.
    assert len(route.calls) == 2