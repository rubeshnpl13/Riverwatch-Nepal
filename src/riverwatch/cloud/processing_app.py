from __future__ import annotations

from typing import Protocol

from fastapi import (
    FastAPI,
    HTTPException,
    Response,
    status,
)

from riverwatch.cloud.dataproc import (
    DataprocBatchSubmission,
)
from riverwatch.cloud.processing_gateway import (
    build_processing_gateway_handler,
)
from riverwatch.events.model import (
    EventEnvelope,
)
from riverwatch.events.pubsub_push import (
    PubSubPushDecodeError,
    decode_pubsub_push,
)


class ProcessingEventHandler(
    Protocol,
):
    def handle(
        self,
        event: EventEnvelope,
    ) -> DataprocBatchSubmission:
        ...


def create_app(
    *,
    handler: (
        ProcessingEventHandler
        | None
    ) = None,
) -> FastAPI:
    resolved_handler = (
        build_processing_gateway_handler()
        if handler is None
        else handler
    )

    app = FastAPI(
        title=(
            "RiverWatch Processing "
            "Gateway Service"
        ),
        description=(
            "Internal Cloud Run service "
            "for submitting RiverWatch "
            "processing workloads to "
            "Managed Spark."
        ),
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    app.state.processing_handler = (
        resolved_handler
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
    def receive_pubsub_event(
        payload: dict[str, object],
    ) -> Response:
        try:
            event = decode_pubsub_push(
                payload
            )

        except (
            PubSubPushDecodeError
        ) as exc:
            raise HTTPException(
                status_code=(
                    status
                    .HTTP_400_BAD_REQUEST
                ),
                detail=str(
                    exc
                ),
            ) from exc

        event_handler: (
            ProcessingEventHandler
        ) = (
            app.state
            .processing_handler
        )

        event_handler.handle(
            event
        )

        return Response(
            status_code=(
                status
                .HTTP_204_NO_CONTENT
            )
        )

    return app