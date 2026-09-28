from __future__ import annotations

import json
import logging
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import Enum
from typing import Any, TextIO

from pydantic import BaseModel

_RESERVED_FIELDS = frozenset(
    {
        "timestamp",
        "level",
        "logger",
        "event",
        "exception",
    }
)


def _json_default(
    value: Any,
) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, BaseModel):
        return value.model_dump(
            mode="json"
        )

    return str(value)


class JsonFormatter(logging.Formatter):
    def format(
        self,
        record: logging.LogRecord,
    ) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                tz=UTC,
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": getattr(
                record,
                "event_name",
                record.getMessage(),
            ),
        }

        event_fields = getattr(
            record,
            "event_fields",
            None,
        )

        if isinstance(
            event_fields,
            Mapping,
        ):
            payload.update(
                event_fields
            )

        if record.exc_info:
            payload["exception"] = (
                self.formatException(
                    record.exc_info
                )
            )

        return json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            default=_json_default,
        )


def configure_logging(
    *,
    log_level: str = "INFO",
    stream: TextIO | None = None,
) -> logging.Logger:
    numeric_level = getattr(
        logging,
        log_level.upper(),
        None,
    )

    if not isinstance(
        numeric_level,
        int,
    ):
        raise ValueError(
            "Invalid log level: "
            f"{log_level}"
        )

    logger = logging.getLogger(
        "riverwatch"
    )

    logger.setLevel(
        numeric_level
    )

    logger.propagate = False

    for handler in list(
        logger.handlers
    ):
        logger.removeHandler(
            handler
        )
        handler.close()

    handler = logging.StreamHandler(
        stream
        if stream is not None
        else sys.stdout
    )

    handler.setFormatter(
        JsonFormatter()
    )

    logger.addHandler(
        handler
    )

    return logger


def log_event(
    logger: logging.Logger,
    log_level: int,
    event: str,
    **fields: Any,
) -> None:
    conflicting_fields = (
        _RESERVED_FIELDS
        & fields.keys()
    )

    if conflicting_fields:
        names = ", ".join(
            sorted(
                conflicting_fields
            )
        )

        raise ValueError(
            "Structured log fields use "
            f"reserved names: {names}"
        )

    logger.log(
        log_level,
        event,
        extra={
            "event_name": event,
            "event_fields": fields,
        },
    )