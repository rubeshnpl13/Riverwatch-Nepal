class BipadError(Exception):
    """Base exception for BIPAD ingestion errors."""


class BipadRequestError(BipadError):
    """Raised when BIPAD cannot be reached."""


class BipadTimeoutError(BipadRequestError):
    """Raised when a BIPAD request exceeds its timeout."""


class BipadResponseDecodeError(BipadError):
    """Raised when BIPAD returns a response that is not valid JSON."""


class BipadHTTPError(BipadError):
    """Raised when BIPAD returns an unsuccessful HTTP status."""

    def __init__(
        self,
        *,
        status_code: int,
        url: str,
    ) -> None:
        self.status_code = status_code
        self.url = url

        super().__init__(
            f"BIPAD request failed with HTTP "
            f"{status_code}: {url}"
        )


class BipadPaginationError(BipadError):
    """Raised when BIPAD pagination cannot be completed safely."""


class BipadPaginationLimitError(BipadPaginationError):
    """Raised when pagination exceeds the configured page limit."""


class BipadPaginationLoopError(BipadPaginationError):
    """Raised when BIPAD returns a previously visited next URL."""