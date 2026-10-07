from __future__ import annotations

import json
from datetime import datetime
from typing import cast
from uuid import UUID

from riverwatch.events.model import (
    EventEnvelope,
    EventScalar,
    EventType,
)


class EventDecodeError(
    ValueError
):
    """Serialized RiverWatch event is invalid."""


def encode_event(
    event: EventEnvelope,
) -> bytes:
    payload = {
        "event_id": str(
            event.event_id
        ),
        "event_type": (
            event.event_type.value
        ),
        "occurred_at": (
            event.occurred_at
            .isoformat()
            .replace(
                "+00:00",
                "Z",
            )
        ),
        "correlation_id": str(
            event.correlation_id
        ),
        "schema_version": (
            event.schema_version
        ),
        "data": dict(
            event.data
        ),
    }

    return json.dumps(
        payload,
        sort_keys=True,
        separators=(
            ",",
            ":",
        ),
        ensure_ascii=False,
    ).encode(
        "utf-8"
    )


def decode_event(
    data: bytes,
) -> EventEnvelope:
    try:
        text = data.decode(
            "utf-8"
        )

    except UnicodeDecodeError as exc:
        raise EventDecodeError(
            "Event payload must be UTF-8"
        ) from exc

    try:
        raw: object = json.loads(
            text
        )

    except json.JSONDecodeError as exc:
        raise EventDecodeError(
            "Event payload must be valid JSON"
        ) from exc

    payload = _string_object_mapping(
        raw,
        field="event payload",
    )

    event_id_value = _required_string(
        payload,
        "event_id",
    )

    event_type_value = _required_string(
        payload,
        "event_type",
    )

    occurred_at_value = _required_string(
        payload,
        "occurred_at",
    )

    correlation_id_value = (
        _required_string(
            payload,
            "correlation_id",
        )
    )

    schema_version = _required_int(
        payload,
        "schema_version",
    )

    event_data = _event_data(
        payload.get(
            "data"
        )
    )

    try:
        occurred_at = (
            datetime.fromisoformat(
                occurred_at_value.replace(
                    "Z",
                    "+00:00",
                )
            )
        )

        return EventEnvelope(
            event_id=UUID(
                event_id_value
            ),
            event_type=EventType(
                event_type_value
            ),
            occurred_at=occurred_at,
            correlation_id=UUID(
                correlation_id_value
            ),
            schema_version=(
                schema_version
            ),
            data=event_data,
        )

    except ValueError as exc:
        raise EventDecodeError(
            "Event payload contains "
            "invalid values"
        ) from exc


def _string_object_mapping(
    value: object,
    *,
    field: str,
) -> dict[str, object]:
    if not isinstance(
        value,
        dict,
    ):
        raise EventDecodeError(
            f"{field} must be an object"
        )

    raw_mapping = cast(
        dict[object, object],
        value,
    )

    result: dict[
        str,
        object,
    ] = {}

    for key, item in (
        raw_mapping.items()
    ):
        if not isinstance(
            key,
            str,
        ):
            raise EventDecodeError(
                f"{field} keys must "
                "be strings"
            )

        result[key] = item

    return result


def _required_string(
    payload: dict[str, object],
    field: str,
) -> str:
    value = payload.get(
        field
    )

    if (
        not isinstance(
            value,
            str,
        )
        or not value.strip()
    ):
        raise EventDecodeError(
            f"{field} must be "
            "a non-empty string"
        )

    return value


def _required_int(
    payload: dict[str, object],
    field: str,
) -> int:
    value = payload.get(
        field
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
        raise EventDecodeError(
            f"{field} must be an integer"
        )

    return value


def _event_data(
    value: object,
) -> dict[str, EventScalar]:
    raw = _string_object_mapping(
        value,
        field="data",
    )

    result: dict[
        str,
        EventScalar,
    ] = {}

    for key, item in raw.items():
        if (
            item is None
            or isinstance(
                item,
                (
                    str,
                    int,
                    float,
                    bool,
                ),
            )
        ):
            result[key] = item
            continue

        raise EventDecodeError(
            "Event data values must "
            "be scalar"
        )

    return result