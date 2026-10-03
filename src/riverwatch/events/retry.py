from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID


class RetryStateError(
    RuntimeError,
):
    """A durable retry state could not be read."""


@dataclass(
    frozen=True,
    slots=True,
)
class RetryPolicy:
    max_attempts: int = 3

    def __post_init__(
        self,
    ) -> None:
        if self.max_attempts < 1:
            raise ValueError(
                "max_attempts must be "
                "at least 1"
            )


@dataclass(
    frozen=True,
    slots=True,
)
class IngestionRetryState:
    request_event_id: UUID

    attempts: int

    first_failed_at: datetime
    last_failed_at: datetime

    last_error_type: str
    last_error_message: str

    def __post_init__(
        self,
    ) -> None:
        if self.attempts < 1:
            raise ValueError(
                "attempts must be "
                "at least 1"
            )

        for name, value in (
            (
                "first_failed_at",
                self.first_failed_at,
            ),
            (
                "last_failed_at",
                self.last_failed_at,
            ),
        ):
            if (
                value.tzinfo is None
                or value.utcoffset()
                is None
            ):
                raise ValueError(
                    f"{name} must be "
                    "timezone-aware"
                )

        first_failed_at = (
            self.first_failed_at
            .astimezone(UTC)
        )

        last_failed_at = (
            self.last_failed_at
            .astimezone(UTC)
        )

        if (
            last_failed_at
            < first_failed_at
        ):
            raise ValueError(
                "last_failed_at must not "
                "be before first_failed_at"
            )

        if not self.last_error_type.strip():
            raise ValueError(
                "last_error_type must not "
                "be blank"
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
            self.last_error_type.strip(),
        )

        object.__setattr__(
            self,
            "last_error_message",
            self.last_error_message.strip(),
        )


class IngestionRetryStore(
    Protocol,
):
    def get(
        self,
        request_event_id: UUID,
    ) -> (
        IngestionRetryState
        | None
    ):
        """Return retry state for a request."""
        ...

    def record_failure(
        self,
        *,
        request_event_id: UUID,
        failed_at: datetime,
        error_type: str,
        error_message: str,
    ) -> IngestionRetryState:
        """Record one failed processing attempt."""
        ...

    def clear(
        self,
        request_event_id: UUID,
    ) -> None:
        """Remove retry state after success."""
        ...


class LocalIngestionRetryStore:
    def __init__(
        self,
        *,
        events_root: Path,
    ) -> None:
        self._root = (
            events_root
            / "retries"
            / "ingestion"
        )

        self._root.mkdir(
            parents=True,
            exist_ok=True,
        )

    def get(
        self,
        request_event_id: UUID,
    ) -> (
        IngestionRetryState
        | None
    ):
        path = self._path_for(
            request_event_id
        )

        if not path.is_file():
            return None

        try:
            document = json.loads(
                path.read_text(
                    encoding="utf-8"
                )
            )

            return self._deserialize(
                document
            )

        except (
            OSError,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            raise RetryStateError(
                "Unable to read ingestion "
                "retry state: "
                f"{path}"
            ) from exc

    def record_failure(
        self,
        *,
        request_event_id: UUID,
        failed_at: datetime,
        error_type: str,
        error_message: str,
    ) -> IngestionRetryState:
        if (
            failed_at.tzinfo is None
            or failed_at.utcoffset()
            is None
        ):
            raise ValueError(
                "failed_at must be "
                "timezone-aware"
            )

        normalized_failed_at = (
            failed_at.astimezone(
                UTC
            )
        )

        existing = self.get(
            request_event_id
        )

        if existing is None:
            state = (
                IngestionRetryState(
                    request_event_id=(
                        request_event_id
                    ),
                    attempts=1,
                    first_failed_at=(
                        normalized_failed_at
                    ),
                    last_failed_at=(
                        normalized_failed_at
                    ),
                    last_error_type=(
                        error_type
                    ),
                    last_error_message=(
                        error_message
                    ),
                )
            )

        else:
            state = (
                IngestionRetryState(
                    request_event_id=(
                        request_event_id
                    ),
                    attempts=(
                        existing.attempts
                        + 1
                    ),
                    first_failed_at=(
                        existing
                        .first_failed_at
                    ),
                    last_failed_at=(
                        normalized_failed_at
                    ),
                    last_error_type=(
                        error_type
                    ),
                    last_error_message=(
                        error_message
                    ),
                )
            )

        self._write(
            state
        )

        return state

    def clear(
        self,
        request_event_id: UUID,
    ) -> None:
        self._path_for(
            request_event_id
        ).unlink(
            missing_ok=True
        )

    def _write(
        self,
        state: IngestionRetryState,
    ) -> None:
        path = self._path_for(
            state.request_event_id
        )

        temporary_path = (
            path.with_name(
                f".{path.name}.tmp"
            )
        )

        document = {
            "request_event_id":
                str(
                    state.request_event_id
                ),

            "attempts":
                state.attempts,

            "first_failed_at":
                self._serialize_datetime(
                    state.first_failed_at
                ),

            "last_failed_at":
                self._serialize_datetime(
                    state.last_failed_at
                ),

            "last_error_type":
                state.last_error_type,

            "last_error_message":
                state.last_error_message,
        }

        serialized = (
            json.dumps(
                document,
                sort_keys=True,
                separators=(
                    ",",
                    ":",
                ),
            )
            + "\n"
        )

        try:
            with temporary_path.open(
                "w",
                encoding="utf-8",
            ) as handle:
                handle.write(
                    serialized
                )

                handle.flush()

                os.fsync(
                    handle.fileno()
                )

            os.replace(
                temporary_path,
                path,
            )

        finally:
            temporary_path.unlink(
                missing_ok=True
            )

    def _path_for(
        self,
        request_event_id: UUID,
    ) -> Path:
        return (
            self._root
            / (
                f"{request_event_id}"
                ".json"
            )
        )

    @staticmethod
    def _serialize_datetime(
        value: datetime,
    ) -> str:
        return (
            value
            .astimezone(UTC)
            .isoformat()
            .replace(
                "+00:00",
                "Z",
            )
        )

    @staticmethod
    def _deserialize(
        document: object,
    ) -> IngestionRetryState:
        if not isinstance(
            document,
            dict,
        ):
            raise ValueError(
                "Retry state must "
                "be an object"
            )

        first_failed_at = (
            document[
                "first_failed_at"
            ]
        )

        last_failed_at = (
            document[
                "last_failed_at"
            ]
        )

        if not isinstance(
            first_failed_at,
            str,
        ):
            raise ValueError(
                "first_failed_at must "
                "be a string"
            )

        if not isinstance(
            last_failed_at,
            str,
        ):
            raise ValueError(
                "last_failed_at must "
                "be a string"
            )

        return (
            IngestionRetryState(
                request_event_id=UUID(
                    str(
                        document[
                            "request_event_id"
                        ]
                    )
                ),
                attempts=int(
                    document[
                        "attempts"
                    ]
                ),
                first_failed_at=(
                    datetime.fromisoformat(
                        first_failed_at
                        .replace(
                            "Z",
                            "+00:00",
                        )
                    )
                ),
                last_failed_at=(
                    datetime.fromisoformat(
                        last_failed_at
                        .replace(
                            "Z",
                            "+00:00",
                        )
                    )
                ),
                last_error_type=str(
                    document[
                        "last_error_type"
                    ]
                ),
                last_error_message=str(
                    document[
                        "last_error_message"
                    ]
                ),
            )
        )