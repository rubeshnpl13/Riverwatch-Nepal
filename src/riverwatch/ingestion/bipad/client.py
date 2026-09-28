import time
from collections.abc import Callable, Iterator
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from riverwatch.config import Settings
from riverwatch.ingestion.bipad.endpoints import BipadEndpoint
from riverwatch.ingestion.bipad.errors import (
    BipadHTTPError,
    BipadPaginationLimitError,
    BipadPaginationLoopError,
    BipadRequestError,
    BipadResponseDecodeError,
    BipadTimeoutError,
)
from riverwatch.ingestion.bipad.pagination import BipadRawPage
from riverwatch.ingestion.bipad.schemas import (
    BipadRiverRecord,
    BipadRiverStation,
)

RETRYABLE_HTTP_STATUS_CODES = frozenset(
    {
        408,
        429,
        500,
        502,
        503,
        504,
    }
)


ModelT = TypeVar(
    "ModelT",
    bound=BaseModel,
)


def is_retryable_bipad_exception(
    exception: BaseException,
) -> bool:
    """Return whether a BIPAD exception should be retried."""

    if isinstance(
        exception,
        BipadRequestError,
    ):
        return True

    if isinstance(
        exception,
        BipadHTTPError,
    ):
        return (
            exception.status_code
            in RETRYABLE_HTTP_STATUS_CODES
        )

    return False


class BipadClient:
    """HTTP client for interacting with the BIPAD API."""

    def __init__(
        self,
        *,
        base_url: str,
        api_version: str,
        timeout_seconds: float,
        max_retries: int,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_retries < 0:
            raise ValueError(
                "max_retries must be greater than "
                "or equal to zero"
            )

        self._base_url = base_url.rstrip("/")
        self._api_version = api_version.strip("/")
        self._max_retries = max_retries
        self._sleep = sleep

        self._client = httpx.Client(
            timeout=timeout_seconds,
            follow_redirects=True,
            headers={
                "Accept": "application/json",
                "User-Agent": "RiverWatch-Nepal/0.1",
            },
        )

    def __enter__(self) -> "BipadClient":
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self._client.close()

    def build_endpoint_url(
        self,
        endpoint: BipadEndpoint,
    ) -> str:
        """Build the full URL for a BIPAD endpoint."""

        return (
            f"{self._base_url}/api/"
            f"{self._api_version}/"
            f"{endpoint.value}/"
        )

    def get_json(
        self,
        endpoint: BipadEndpoint,
        *,
        params: dict[str, str | int] | None = None,
    ) -> Any:
        """Fetch JSON from a known BIPAD endpoint."""

        url = self.build_endpoint_url(
            endpoint
        )

        return self.get_url_json(
            url,
            params=params,
        )

    def _get_url_json_once(
        self,
        url: str,
        *,
        params: dict[str, str | int] | None = None,
    ) -> Any:
        """Perform exactly one HTTP request."""

        try:
            response = self._client.get(
                url,
                params=params,
            )

        except httpx.TimeoutException as exc:
            raise BipadTimeoutError(
                f"BIPAD request timed out: {url}"
            ) from exc

        except httpx.RequestError as exc:
            raise BipadRequestError(
                f"Unable to reach BIPAD: {url}"
            ) from exc

        if response.is_error:
            raise BipadHTTPError(
                status_code=response.status_code,
                url=str(response.url),
            )

        try:
            return response.json()

        except ValueError as exc:
            raise BipadResponseDecodeError(
                "BIPAD returned invalid JSON: "
                f"{response.url}"
            ) from exc

    def get_url_json(
        self,
        url: str,
        *,
        params: dict[str, str | int] | None = None,
    ) -> Any:
        """
        Fetch JSON from a URL with retry and exponential
        backoff for transient failures.
        """

        retrying = Retrying(
            stop=stop_after_attempt(
                self._max_retries + 1
            ),
            wait=wait_exponential(
                multiplier=0.5,
                min=0.5,
                max=8.0,
            ),
            retry=retry_if_exception(
                is_retryable_bipad_exception
            ),
            reraise=True,
            sleep=self._sleep,
        )

        return retrying(
            self._get_url_json_once,
            url,
            params=params,
        )

    def iter_pages(
        self,
        endpoint: BipadEndpoint,
        *,
        params: dict[str, str | int] | None = None,
        max_pages: int = 100,
    ) -> Iterator[BipadRawPage]:
        """
        Lazily iterate through paginated BIPAD API responses.

        BIPAD currently reports an unreliable count value and
        may continue returning a next URL after the dataset has
        been exhausted. Therefore an empty results page is also
        treated as the end of pagination.
        """

        if max_pages <= 0:
            raise ValueError(
                "max_pages must be greater than zero"
            )

        first_url = self.build_endpoint_url(
            endpoint
        )

        next_url: str | None = first_url
        next_params = params

        visited_urls: set[str] = set()

        page_number = 0

        while next_url is not None:
            if page_number >= max_pages:
                raise BipadPaginationLimitError(
                    "Pagination exceeded maximum "
                    f"of {max_pages} pages"
                )

            if next_url in visited_urls:
                raise BipadPaginationLoopError(
                    "Pagination loop detected: "
                    f"{next_url}"
                )

            visited_urls.add(
                next_url
            )

            payload = self.get_url_json(
                next_url,
                params=next_params,
            )

            page = BipadRawPage.model_validate(
                payload
            )

            yield page

            page_number += 1

            # BIPAD currently has broken pagination metadata:
            # it can return another `next` URL even when there
            # are no records left.
            #
            # Therefore an empty page marks the actual end of
            # the available dataset.
            if not page.results:
                return

            next_url = page.next

            # The next URL returned by BIPAD already contains
            # its pagination query parameters, so the original
            # params must not be applied again.
            next_params = None

    def iter_records(
        self,
        endpoint: BipadEndpoint,
        *,
        params: dict[str, str | int] | None = None,
        max_pages: int = 100,
    ) -> Iterator[Any]:
        """Lazily yield raw records across all API pages."""

        for page in self.iter_pages(
            endpoint,
            params=params,
            max_pages=max_pages,
        ):
            yield from page.results

    def _iter_typed_records(
        self,
        endpoint: BipadEndpoint,
        model_type: type[ModelT],
        *,
        params: dict[str, str | int] | None = None,
        max_pages: int = 100,
    ) -> Iterator[ModelT]:
        """
        Validate raw records against a supplied Pydantic model.
        """

        for raw_record in self.iter_records(
            endpoint,
            params=params,
            max_pages=max_pages,
        ):
            yield model_type.model_validate(
                raw_record
            )

    def iter_river_stations(
        self,
        *,
        params: dict[str, str | int] | None = None,
        max_pages: int = 100,
    ) -> Iterator[BipadRiverStation]:
        """Yield typed current river-station snapshots."""

        yield from self._iter_typed_records(
            BipadEndpoint.RIVER_STATIONS,
            BipadRiverStation,
            params=params,
            max_pages=max_pages,
        )

    def iter_river_records(
        self,
        *,
        params: dict[str, str | int] | None = None,
        max_pages: int = 100,
    ) -> Iterator[BipadRiverRecord]:
        """Yield typed historical river observations."""

        yield from self._iter_typed_records(
            BipadEndpoint.RIVER,
            BipadRiverRecord,
            params=params,
            max_pages=max_pages,
        )


def create_bipad_client(
    settings: Settings,
) -> BipadClient:
    """Create a configured BIPAD client."""

    return BipadClient(
        base_url=settings.bipad_base_url,
        api_version=settings.bipad_api_version,
        timeout_seconds=(
            settings.http_timeout_seconds
        ),
        max_retries=(
            settings.http_max_retries
        ),
    )