from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID


class ProcessingRetryStateError(
    RuntimeError
):
    """Stored processing retry state is invalid."""


@dataclass(
    frozen=True,
    slots=True,
)
class ProcessingRetryState:
    ingestion_event_id: UUID
    attempts: int
    first_failed_at: datetime
    last_failed_at: datetime
    last_error_type: str
    last_error_message: str

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError(
                "attempts must be at least 1"
            )

        if (
            self.first_failed_at.tzinfo
            is None
            or self.last_failed_at.tzinfo
            is None
        ):
            raise ValueError(
                "failure timestamps must "
                "be timezone-aware"
            )

        first_failed_at = (
            self.first_failed_at
            .astimezone(
                UTC
            )
        )

        last_failed_at = (
            self.last_failed_at
            .astimezone(
                UTC
            )
        )

        if (
            last_failed_at
            < first_failed_at
        ):
            raise ValueError(
                "last_failed_at cannot be "
                "earlier than first_failed_at"
            )

        error_type = (
            self.last_error_type.strip()
        )

        if not error_type:
            raise ValueError(
                "last_error_type must "
                "not be blank"
            )

        object.__setattr__(
            self,
            "first_failed_at",
            first_failed_at,
        )

        object.__setattr__(
            self,
            "last_failed_at",
            last_failed_at,
        )

        object.__setattr__(
            self,
            "last_error_type",
            error_type,
        )

        object.__setattr__(
            self,
            "last_error_message",
            self
            .last_error_message
            .strip(),
        )


class ProcessingRetryStore(
    Protocol
):
    def get(
        self,
        ingestion_event_id: UUID,
    ) -> ProcessingRetryState | None:
        ...

    def record_failure(
        self,
        *,
        ingestion_event_id: UUID,
        failed_at: datetime,
        error_type: str,
        error_message: str,
    ) -> ProcessingRetryState:
        ...

    def clear(
        self,
        ingestion_event_id: UUID,
    ) -> None:
        ...


class LocalProcessingRetryStore:
    def __init__(
        self,
        *,
        events_root: str | Path,
    ) -> None:
        self.events_root = Path(
            events_root
        )

    def get(
        self,
        ingestion_event_id: UUID,
    ) -> ProcessingRetryState | None:
        path = self._path(
            ingestion_event_id
        )

        if not path.is_file():
            return None

        try:
            document = json.loads(
                path.read_text(
                    encoding="utf-8"
                )
            )

        except (
            OSError,
            json.JSONDecodeError,
        ) as exc:
            raise ProcessingRetryStateError(
                "Unable to read processing "
                f"retry state: {path}"
            ) from exc

        if not isinstance(
            document,
            dict,
        ):
            raise ProcessingRetryStateError(
                "Processing retry state must "
                "be a JSON object"
            )

        try:
            return ProcessingRetryState(
                ingestion_event_id=UUID(
                    _required_string(
                        document,
                        "ingestion_event_id",
                    )
                ),
                attempts=_required_int(
                    document,
                    "attempts",
                ),
                first_failed_at=(
                    _parse_datetime(
                        _required_string(
                            document,
                            "first_failed_at",
                        )
                    )
                ),
                last_failed_at=(
                    _parse_datetime(
                        _required_string(
                            document,
                            "last_failed_at",
                        )
                    )
                ),
                last_error_type=(
                    _required_string(
                        document,
                        "last_error_type",
                    )
                ),
                last_error_message=(
                    _required_string_allow_blank(
                        document,
                        "last_error_message",
                    )
                ),
            )

        except (
            ValueError,
            TypeError,
        ) as exc:
            raise ProcessingRetryStateError(
                "Invalid processing retry "
                f"state: {path}"
            ) from exc

    def record_failure(
        self,
        *,
        ingestion_event_id: UUID,
        failed_at: datetime,
        error_type: str,
        error_message: str,
    ) -> ProcessingRetryState:
        existing = self.get(
            ingestion_event_id
        )

        state = ProcessingRetryState(
            ingestion_event_id=(
                ingestion_event_id
            ),
            attempts=(
                1
                if existing is None
                else existing.attempts + 1
            ),
            first_failed_at=(
                failed_at
                if existing is None
                else existing.first_failed_at
            ),
            last_failed_at=failed_at,
            last_error_type=error_type,
            last_error_message=(
                error_message
            ),
        )

        self._write(
            state
        )

        return state

    def clear(
        self,
        ingestion_event_id: UUID,
    ) -> None:
        self._path(
            ingestion_event_id
        ).unlink(
            missing_ok=True
        )

    def _write(
        self,
        state: ProcessingRetryState,
    ) -> None:
        path = self._path(
            state.ingestion_event_id
        )

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        document = {
            "ingestion_event_id": str(
                state.ingestion_event_id
            ),
            "attempts": state.attempts,
            "first_failed_at": (
                _format_datetime(
                    state.first_failed_at
                )
            ),
            "last_failed_at": (
                _format_datetime(
                    state.last_failed_at
                )
            ),
            "last_error_type": (
                state.last_error_type
            ),
            "last_error_message": (
                state.last_error_message
            ),
        }

        data = (
            json.dumps(
                document,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )

        temporary_path = (
            path.with_suffix(
                ".tmp"
            )
        )

        try:
            with temporary_path.open(
                "w",
                encoding="utf-8",
            ) as file:
                file.write(
                    data
                )
                file.flush()
                os.fsync(
                    file.fileno()
                )

            os.replace(
                temporary_path,
                path,
            )

        except OSError as exc:
            temporary_path.unlink(
                missing_ok=True
            )

            raise ProcessingRetryStateError(
                "Unable to write processing "
                f"retry state: {path}"
            ) from exc

    def _path(
        self,
        ingestion_event_id: UUID,
    ) -> Path:
        return (
            self.events_root
            / "retries"
            / "processing"
            / f"{ingestion_event_id}.json"
        )


def _required_string(
    document: dict[object, object],
    key: str,
) -> str:
    value = document.get(
        key
    )

    if not isinstance(
        value,
        str,
    ):
        raise ValueError(
            f"{key} must be a string"
        )

    if not value.strip():
        raise ValueError(
            f"{key} must not be blank"
        )

    return value


def _required_string_allow_blank(
    document: dict[object, object],
    key: str,
) -> str:
    value = document.get(
        key
    )

    if not isinstance(
        value,
        str,
    ):
        raise ValueError(
            f"{key} must be a string"
        )

    return value


def _required_int(
    document: dict[object, object],
    key: str,
) -> int:
    value = document.get(
        key
    )

    if (
        not isinstance(
            value,
            int,
        )
        or isinstance(
            value,
            bool,
        )
    ):
        raise ValueError(
            f"{key} must be an integer"
        )

    return value


def _parse_datetime(
    value: str,
) -> datetime:
    return datetime.fromisoformat(
        value.replace(
            "Z",
            "+00:00",
        )
    )


def _format_datetime(
    value: datetime,
) -> str:
    return (
        value.astimezone(
            UTC
        )
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )