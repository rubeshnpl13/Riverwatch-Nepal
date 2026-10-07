from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping
from datetime import datetime
from typing import cast
from uuid import (
    NAMESPACE_URL,
    uuid5,
)

from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.serialization import (
    EventDecodeError,
    decode_event,
)

SCHEDULED_INGESTION_MESSAGE_KIND = (
    "scheduled_ingestion_trigger"
)

SUPPORTED_TRANSPORT_SCHEMA_VERSION = 1


class PubSubPushDecodeError(
    ValueError
):
    """Pub/Sub push request is invalid."""


def decode_pubsub_push(
    payload: Mapping[str, object],
) -> EventEnvelope:
    message = _mapping(
        payload.get(
            "message"
        ),
        field="message",
    )

    subscription = _non_empty_string(
        payload.get(
            "subscription"
        ),
        field="subscription",
    )

    message_id = _message_field(
        message,
        primary="messageId",
        fallback="message_id",
    )

    publish_time = _message_field(
        message,
        primary="publishTime",
        fallback="publish_time",
    )

    encoded_data = _non_empty_string(
        message.get(
            "data"
        ),
        field="message.data",
    )

    try:
        decoded_data = (
            base64.b64decode(
                encoded_data,
                validate=True,
            )
        )

    except (
        binascii.Error,
        ValueError,
    ) as exc:
        raise PubSubPushDecodeError(
            "message.data must be "
            "valid base64"
        ) from exc

    document = _json_object(
        decoded_data
    )

    if (
        "event_id" in document
        or "event_type" in document
    ):
        try:
            return decode_event(
                decoded_data
            )

        except EventDecodeError as exc:
            raise PubSubPushDecodeError(
                "Invalid RiverWatch event"
            ) from exc

    message_kind = document.get(
        "message_kind"
    )

    if (
        message_kind
        == SCHEDULED_INGESTION_MESSAGE_KIND
    ):
        return _scheduled_ingestion_event(
            document=document,
            subscription=subscription,
            message_id=message_id,
            publish_time=publish_time,
        )

    raise PubSubPushDecodeError(
        "Unsupported Pub/Sub "
        "message payload"
    )


def _scheduled_ingestion_event(
    *,
    document: dict[str, object],
    subscription: str,
    message_id: str,
    publish_time: str,
) -> EventEnvelope:
    schema_version = (
        document.get(
            "schema_version"
        )
    )

    if (
        not isinstance(
            schema_version,
            int,
        )
        or isinstance(
            schema_version,
            bool,
        )
        or (
            schema_version
            != SUPPORTED_TRANSPORT_SCHEMA_VERSION
        )
    ):
        raise PubSubPushDecodeError(
            "Unsupported scheduled "
            "trigger schema_version"
        )

    provider = _non_empty_string(
        document.get(
            "provider"
        ),
        field="provider",
    )

    endpoint = _non_empty_string(
        document.get(
            "endpoint"
        ),
        field="endpoint",
    )

    try:
        occurred_at = (
            datetime.fromisoformat(
                publish_time.replace(
                    "Z",
                    "+00:00",
                )
            )
        )

    except ValueError as exc:
        raise PubSubPushDecodeError(
            "publishTime must be "
            "an ISO-8601 timestamp"
        ) from exc

    if (
        occurred_at.tzinfo is None
        or occurred_at.utcoffset()
        is None
    ):
        raise PubSubPushDecodeError(
            "publishTime must be "
            "timezone-aware"
        )

    identity = (
        f"{subscription}:"
        f"{message_id}"
    )

    event_id = uuid5(
        NAMESPACE_URL,
        (
            "riverwatch:"
            "scheduled-ingestion:"
            f"event:{identity}"
        ),
    )

    correlation_id = uuid5(
        NAMESPACE_URL,
        (
            "riverwatch:"
            "scheduled-ingestion:"
            f"correlation:{identity}"
        ),
    )

    return EventEnvelope(
        event_id=event_id,
        event_type=(
            EventType
            .INGESTION_REQUESTED
        ),
        occurred_at=occurred_at,
        correlation_id=(
            correlation_id
        ),
        schema_version=1,
        data={
            "provider": provider,
            "endpoint": endpoint,
        },
    )


def _json_object(
    data: bytes,
) -> dict[str, object]:
    try:
        text = data.decode(
            "utf-8"
        )

    except UnicodeDecodeError as exc:
        raise PubSubPushDecodeError(
            "Pub/Sub message data "
            "must be UTF-8"
        ) from exc

    try:
        raw: object = json.loads(
            text
        )

    except json.JSONDecodeError as exc:
        raise PubSubPushDecodeError(
            "Pub/Sub message data "
            "must be valid JSON"
        ) from exc

    return _mapping(
        raw,
        field="message data",
    )


def _mapping(
    value: object,
    *,
    field: str,
) -> dict[str, object]:
    if not isinstance(
        value,
        dict,
    ):
        raise PubSubPushDecodeError(
            f"{field} must be an object"
        )

    raw = cast(
        dict[object, object],
        value,
    )

    result: dict[
        str,
        object,
    ] = {}

    for key, item in raw.items():
        if not isinstance(
            key,
            str,
        ):
            raise PubSubPushDecodeError(
                f"{field} keys must "
                "be strings"
            )

        result[key] = item

    return result


def _non_empty_string(
    value: object,
    *,
    field: str,
) -> str:
    if (
        not isinstance(
            value,
            str,
        )
        or not value.strip()
    ):
        raise PubSubPushDecodeError(
            f"{field} must be a "
            "non-empty string"
        )

    return value


def _message_field(
    message: dict[str, object],
    *,
    primary: str,
    fallback: str,
) -> str:
    value = message.get(
        primary
    )

    if value is None:
        value = message.get(
            fallback
        )

    return _non_empty_string(
        value,
        field=primary,
    )