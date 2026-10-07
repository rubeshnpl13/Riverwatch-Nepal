from __future__ import annotations

import base64
import json
from datetime import (
    UTC,
    datetime,
)
from uuid import UUID

import pytest

from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)
from riverwatch.events.pubsub_push import (
    PubSubPushDecodeError,
    decode_pubsub_push,
)
from riverwatch.events.serialization import (
    encode_event,
)

SUBSCRIPTION = (
    "projects/test-project/"
    "subscriptions/"
    "riverwatch-dev-ingestion-worker"
)

MESSAGE_ID = "123456789"

PUBLISH_TIME = (
    "2026-10-05T00:15:00Z"
)


def _push_payload(
    data: bytes,
    *,
    message_id: str = MESSAGE_ID,
    publish_time: str = PUBLISH_TIME,
) -> dict[str, object]:
    return {
        "message": {
            "data": (
                base64.b64encode(
                    data
                ).decode(
                    "ascii"
                )
            ),
            "messageId": message_id,
            "publishTime": publish_time,
        },
        "subscription": SUBSCRIPTION,
    }


def _scheduled_payload() -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "message_kind": (
                "scheduled_ingestion_trigger"
            ),
            "provider": "bipad",
            "endpoint": "river-stations",
        }
    ).encode(
        "utf-8"
    )


def test_decodes_riverwatch_event() -> None:
    expected = EventEnvelope(
        event_id=UUID(
            "11111111-1111-1111-1111-111111111111"
        ),
        event_type=(
            EventType
            .INGESTION_COMPLETED
        ),
        occurred_at=datetime(
            2026,
            10,
            5,
            0,
            0,
            tzinfo=UTC,
        ),
        correlation_id=UUID(
            "22222222-2222-2222-2222-222222222222"
        ),
        data={
            "provider": "bipad",
            "endpoint": "river-stations",
            "run_id": "run-123",
        },
    )

    result = decode_pubsub_push(
        _push_payload(
            encode_event(
                expected
            )
        )
    )

    assert result == expected


def test_decodes_scheduled_ingestion_trigger() -> None:
    result = decode_pubsub_push(
        _push_payload(
            _scheduled_payload()
        )
    )

    assert (
        result.event_type
        is EventType.INGESTION_REQUESTED
    )

    assert result.occurred_at == (
        datetime(
            2026,
            10,
            5,
            0,
            15,
            tzinfo=UTC,
        )
    )

    assert dict(
        result.data
    ) == {
        "provider": "bipad",
        "endpoint": "river-stations",
    }

    assert (
        result.event_id
        != result.correlation_id
    )


def test_scheduled_trigger_identity_is_deterministic() -> None:
    payload = _push_payload(
        _scheduled_payload()
    )

    first = decode_pubsub_push(
        payload
    )

    second = decode_pubsub_push(
        payload
    )

    assert (
        first.event_id
        == second.event_id
    )

    assert (
        first.correlation_id
        == second.correlation_id
    )


def test_different_message_id_changes_identity() -> None:
    first = decode_pubsub_push(
        _push_payload(
            _scheduled_payload(),
            message_id="100",
        )
    )

    second = decode_pubsub_push(
        _push_payload(
            _scheduled_payload(),
            message_id="200",
        )
    )

    assert (
        first.event_id
        != second.event_id
    )


def test_rejects_invalid_base64() -> None:
    payload: dict[
        str,
        object,
    ] = {
        "message": {
            "data": "%%%",
            "messageId": MESSAGE_ID,
            "publishTime": PUBLISH_TIME,
        },
        "subscription": SUBSCRIPTION,
    }

    with pytest.raises(
        PubSubPushDecodeError,
        match="valid base64",
    ):
        decode_pubsub_push(
            payload
        )


def test_rejects_missing_message() -> None:
    with pytest.raises(
        PubSubPushDecodeError,
        match="message must be an object",
    ):
        decode_pubsub_push(
            {
                "subscription": (
                    SUBSCRIPTION
                )
            }
        )


def test_rejects_unsupported_message_payload() -> None:
    raw = json.dumps(
        {
            "hello": "world"
        }
    ).encode(
        "utf-8"
    )

    with pytest.raises(
        PubSubPushDecodeError,
        match="Unsupported Pub/Sub",
    ):
        decode_pubsub_push(
            _push_payload(
                raw
            )
        )


def test_rejects_unsupported_trigger_schema() -> None:
    raw = json.dumps(
        {
            "schema_version": 2,
            "message_kind": (
                "scheduled_ingestion_trigger"
            ),
            "provider": "bipad",
            "endpoint": "river-stations",
        }
    ).encode(
        "utf-8"
    )

    with pytest.raises(
        PubSubPushDecodeError,
        match="schema_version",
    ):
        decode_pubsub_push(
            _push_payload(
                raw
            )
        )


def test_rejects_missing_publish_time() -> None:
    payload = _push_payload(
        _scheduled_payload()
    )

    message = payload[
        "message"
    ]

    assert isinstance(
        message,
        dict,
    )

    message.pop(
        "publishTime"
    )

    with pytest.raises(
        PubSubPushDecodeError,
        match="publishTime",
    ):
        decode_pubsub_push(
            payload
        )


def test_rejects_invalid_riverwatch_event() -> None:
    raw = json.dumps(
        {
            "event_id": "not-a-uuid",
            "event_type": (
                "ingestion.completed"
            ),
        }
    ).encode(
        "utf-8"
    )

    with pytest.raises(
        PubSubPushDecodeError,
        match="Invalid RiverWatch event",
    ):
        decode_pubsub_push(
            _push_payload(
                raw
            )
        )