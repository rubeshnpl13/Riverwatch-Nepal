from __future__ import annotations

from fastapi import (
    FastAPI,
    HTTPException,
    Response,
    status,
)

from riverwatch.cloud.config import (
    ProcessingCompletionRelayCloudConfig,
)
from riverwatch.cloud.processing_completion_relay import (
    CompletionReceiptStore,
    ProcessingCompletionRelayError,
    decode_gcs_finalize_push,
    relay_processing_completion,
)
from riverwatch.events.bus import (
    EventPublisher,
)
from riverwatch.events.pubsub import (
    PubSubEventPublisher,
)
from riverwatch.storage.gcs import (
    GcsObjectStore,
)


def create_app(
    *,
    config: (
        ProcessingCompletionRelayCloudConfig
        | None
    ) = None,
    store: CompletionReceiptStore | None = None,
    publisher: EventPublisher | None = None,
) -> FastAPI:
    resolved_config = config

    if (
        store is None
        or publisher is None
    ):
        resolved_config = (
            ProcessingCompletionRelayCloudConfig
            .from_environment()
            if resolved_config is None
            else resolved_config
        )

    if store is None:
        assert resolved_config is not None

        resolved_store: CompletionReceiptStore = (
            GcsObjectStore(
                bucket_name=(
                    resolved_config
                    .lake_bucket
                ),
            )
        )

    else:
        resolved_store = store

    if publisher is None:
        assert resolved_config is not None

        resolved_publisher: EventPublisher = (
            PubSubEventPublisher(
                topic=(
                    resolved_config
                    .processing_completed_topic
                ),
            )
        )

    else:
        resolved_publisher = publisher

    app = FastAPI(
        title=(
            "RiverWatch Processing "
            "Completion Relay"
        ),
        description=(
            "Internal service for relaying "
            "durable processing completion "
            "receipts to the RiverWatch "
            "event bus."
        ),
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.get(
        "/health/live",
        status_code=(
            status.HTTP_200_OK
        ),
    )
    def health_live(
    ) -> dict[str, str]:
        return {
            "status": "ok",
        }

    @app.post(
        "/events/pubsub",
        status_code=(
            status.HTTP_204_NO_CONTENT
        ),
        response_class=Response,
    )
    def receive_storage_notification(
        payload: dict[str, object],
    ) -> Response:
        try:
            notification = (
                decode_gcs_finalize_push(
                    payload
                )
            )

        except (
            ProcessingCompletionRelayError
        ) as exc:
            raise HTTPException(
                status_code=(
                    status
                    .HTTP_400_BAD_REQUEST
                ),
                detail=str(exc),
            ) from exc

        try:
            relay_processing_completion(
                notification=notification,
                store=resolved_store,
                publisher=(
                    resolved_publisher
                ),
            )

        except (
            ProcessingCompletionRelayError
        ) as exc:
            raise HTTPException(
                status_code=(
                    status
                    .HTTP_503_SERVICE_UNAVAILABLE
                ),
                detail=str(exc),
            ) from exc

        return Response(
            status_code=(
                status.HTTP_204_NO_CONTENT
            )
        )

    return app