from __future__ import annotations

from concurrent.futures import (
    TimeoutError as FutureTimeoutError,
)
from typing import Protocol

import google.cloud.pubsub_v1 as pubsub_v1  # type: ignore[import-untyped]
from google.api_core.exceptions import (
    GoogleAPIError,
)

from riverwatch.events.model import (
    EventEnvelope,
)
from riverwatch.events.serialization import (
    encode_event,
)

DEFAULT_PUBLISH_TIMEOUT_SECONDS = 30.0


class PubSubPublishError(
    RuntimeError
):
    """RiverWatch could not publish an event to Pub/Sub."""


class _PublishFuture(Protocol):
    def result(
        self,
        timeout: float | None = None,
    ) -> str:
        ...


class _PublisherClient(Protocol):
    def publish(
        self,
        topic: str,
        data: bytes,
        **attributes: str,
    ) -> _PublishFuture:
        ...


def _validate_topic_path(
    topic: str,
) -> str:
    cleaned = topic.strip()

    if not cleaned:
        raise ValueError(
            "topic cannot be empty"
        )

    parts = cleaned.split(
        "/"
    )

    if (
        len(parts) != 4
        or parts[0] != "projects"
        or not parts[1]
        or parts[2] != "topics"
        or not parts[3]
    ):
        raise ValueError(
            "topic must be a fully qualified "
            "Pub/Sub topic path"
        )

    return cleaned


class PubSubEventPublisher:
    """Publish RiverWatch EventEnvelope objects to Pub/Sub."""

    def __init__(
        self,
        *,
        topic: str,
        client: _PublisherClient | None = None,
        publish_timeout_seconds: float = (
            DEFAULT_PUBLISH_TIMEOUT_SECONDS
        ),
    ) -> None:
        if publish_timeout_seconds <= 0:
            raise ValueError(
                "publish_timeout_seconds must "
                "be greater than zero"
            )

        self._topic = (
            _validate_topic_path(
                topic
            )
        )

        self._publish_timeout_seconds = (
            publish_timeout_seconds
        )

        if client is None:
            self._client: _PublisherClient = (
                pubsub_v1.PublisherClient()
            )
        else:
            self._client = client

    @property
    def topic(
        self,
    ) -> str:
        return self._topic

    def publish(
        self,
        event: EventEnvelope,
    ) -> None:
        try:
            future = self._client.publish(
                self._topic,
                encode_event(
                    event
                ),
                event_id=str(
                    event.event_id
                ),
                event_type=(
                    event.event_type.value
                ),
                correlation_id=str(
                    event.correlation_id
                ),
                schema_version=str(
                    event.schema_version
                ),
            )

            message_id = future.result(
                timeout=(
                    self
                    ._publish_timeout_seconds
                )
            )

        except (
            GoogleAPIError,
            FutureTimeoutError,
        ) as exc:
            raise PubSubPublishError(
                "Unable to publish RiverWatch "
                f"event {event.event_id}"
            ) from exc

        if not message_id.strip():
            raise PubSubPublishError(
                "Pub/Sub returned an empty "
                "message ID"
            )