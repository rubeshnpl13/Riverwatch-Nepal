from __future__ import annotations

from typing import Protocol

from fastapi import (
    FastAPI,
    HTTPException,
    Response,
    status,
)

from riverwatch.cloud.config import (
    IngestionCloudConfig,
)
from riverwatch.cloud.ingestion import (
    CloudIngestionHandler,
)
from riverwatch.config import (
    get_settings,
)
from riverwatch.events.model import (
    EventEnvelope,
)
from riverwatch.events.pubsub import (
    PubSubEventPublisher,
)
from riverwatch.events.pubsub_push import (
    PubSubPushDecodeError,
    decode_pubsub_push,
)
from riverwatch.ingestion.bipad.event_runner import (
    BipadEventIngestionRunner,
)
from riverwatch.ingestion.bipad.gcs_run_repository import (
    GcsIngestionRunRepository,
)
from riverwatch.storage.gcs import (
    GcsObjectStore,
)


class IngestionEventHandler(
    Protocol,
):
    def handle(
        self,
        event: EventEnvelope,
    ) -> EventEnvelope:
        ...


def build_cloud_ingestion_handler(
) -> CloudIngestionHandler:
    config = (
        IngestionCloudConfig
        .from_environment()
    )

    store = GcsObjectStore(
        bucket_name=(
            config.lake_bucket
        ),
    )

    run_repository = (
        GcsIngestionRunRepository(
            bucket_name=(
                config.lake_bucket
            ),
        )
    )

    runner = (
        BipadEventIngestionRunner(
            settings=get_settings(),
            store=store,
            run_repository=(
                run_repository
            ),
        )
    )

    publisher = (
        PubSubEventPublisher(
            topic=(
                config
                .ingestion_completed_topic
            ),
        )
    )

    return CloudIngestionHandler(
        runner=runner,
        publisher=publisher,
    )


def create_app(
    *,
    handler: (
        IngestionEventHandler
        | None
    ) = None,
) -> FastAPI:
    resolved_handler = (
        build_cloud_ingestion_handler()
        if handler is None
        else handler
    )

    app = FastAPI(
        title=(
            "RiverWatch Ingestion "
            "Event Service"
        ),
        description=(
            "Internal Cloud Run service "
            "for RiverWatch ingestion "
            "event processing."
        ),
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    app.state.ingestion_handler = (
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
            event = (
                decode_pubsub_push(
                    payload
                )
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
            IngestionEventHandler
        ) = (
            app.state
            .ingestion_handler
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