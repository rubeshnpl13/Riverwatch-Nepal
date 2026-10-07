from __future__ import annotations

import logging
from pathlib import Path
from uuid import UUID

from riverwatch.config import (
    Settings,
    get_settings,
)
from riverwatch.events.worker import (
    IngestionRunReference,
)
from riverwatch.ingestion.bipad.client import (
    create_bipad_client,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.manifest_writer import (
    BipadRunManifestWriter,
)
from riverwatch.ingestion.bipad.quarantine_writer import (
    BipadQuarantineWriter,
)
from riverwatch.ingestion.bipad.raw_writer import (
    BipadRawPageWriter,
)
from riverwatch.ingestion.bipad.run_repository import (
    BipadEventRunnerError,
    IngestionRunRepository,
    LocalIngestionRunRepository,
)
from riverwatch.ingestion.bipad.service import (
    RiverStationIngestionService,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)
from riverwatch.storage.object_store import (
    ObjectStore,
)

LOGGER_NAME = (
    "riverwatch.ingestion.bipad.event_runner"
)

SUPPORTED_PROVIDER = "bipad"

SUPPORTED_ENDPOINT = (
    BipadEndpoint.RIVER_STATIONS.value
)



class UnsupportedIngestionRequestError(
    ValueError
):
    """The event runner cannot handle the request."""


class BipadEventIngestionRunner:

    def __init__(
            self,
            *,
            lake_root: Path | None = None,
            settings: Settings,
            logger: logging.Logger | None = None,
            store: ObjectStore | None = None,
            run_repository: (
                    IngestionRunRepository | None
            ) = None,
    ) -> None:
        self._settings = settings

        self._logger = (
            logger
            if logger is not None
            else logging.getLogger(
                LOGGER_NAME
            )
        )
        if (
                (store is None)
                != (run_repository is None)
        ):
            raise ValueError(
                "store and run_repository must "
                "be provided together"
            )

        if store is None:
            if lake_root is None:
                raise ValueError(
                    "lake_root must be provided "
                    "for local storage"
                )

            resolved_lake_root = (
                lake_root
                .expanduser()
                .resolve()
            )

            resolved_store: ObjectStore = (
                LocalObjectStore(
                    root=resolved_lake_root,
                )
            )

            resolved_repository: (
                IngestionRunRepository
            ) = LocalIngestionRunRepository(
                lake_root=resolved_lake_root,
            )

        else:
            assert run_repository is not None

            resolved_store = store
            resolved_repository = (
                run_repository
            )

        self._run_repository = (
            resolved_repository
        )

        self._raw_writer = (
            BipadRawPageWriter(
                store=resolved_store
            )
        )

        self._quarantine_writer = (
            BipadQuarantineWriter(
                store=resolved_store
            )
        )

        self._manifest_writer = (
            BipadRunManifestWriter(
                store=resolved_store
            )
        )

    def run(
        self,
        *,
        provider: str,
        endpoint: str,
        idempotency_key: UUID,
    ) -> IngestionRunReference:
        self._validate_request(
            provider=provider,
            endpoint=endpoint,
        )
        run_prefix = self._run_prefix(
            idempotency_key
        )
        existing = (
            self._run_repository
            .find_completed_run(
                run_prefix=run_prefix,
            )
        )

        if existing is not None:
            return existing

        attempt_number = (
            self._run_repository
            .next_attempt_number(
                run_prefix=run_prefix,
            )
        )

        run_id = self._build_run_id(
            idempotency_key=(
                idempotency_key
            ),
            attempt_number=(
                attempt_number
            ),
        )

        with create_bipad_client(
            self._settings
        ) as client:
            service = (
                RiverStationIngestionService(
                    client=client,
                    logger=self._logger,
                    raw_writer=(
                        self._raw_writer
                    ),
                    quarantine_writer=(
                        self
                        ._quarantine_writer
                    ),
                    manifest_writer=(
                        self
                        ._manifest_writer
                    ),
                )
            )

            batch = service.run(
                run_id=run_id,
            )

        if (
            batch.run_result.run_id
            != run_id
        ):
            raise BipadEventRunnerError(
                "Ingestion service returned "
                "an unexpected run_id"
            )

        reference = (
            self._run_repository
            .reference_for_run_id(
                run_id=run_id,
            )
        )

        if reference is None:
            raise BipadEventRunnerError(
                "Ingestion completed without "
                "a completed manifest for "
                f"run_id={run_id}"
            )

        return reference

    def _validate_request(
        self,
        *,
        provider: str,
        endpoint: str,
    ) -> None:
        if provider != SUPPORTED_PROVIDER:
            raise (
                UnsupportedIngestionRequestError(
                    "Unsupported ingestion "
                    f"provider: {provider}"
                )
            )

        if endpoint != SUPPORTED_ENDPOINT:
            raise (
                UnsupportedIngestionRequestError(
                    "Unsupported BIPAD "
                    "ingestion endpoint: "
                    f"{endpoint}"
                )
            )

    @staticmethod
    def _run_prefix(
            idempotency_key: UUID,
    ) -> str:
        return (
            "event-"
            f"{idempotency_key}"
            "-attempt-"
        )

    @classmethod
    def _build_run_id(
            cls,
            *,
            idempotency_key: UUID,
            attempt_number: int,
    ) -> str:
        if attempt_number < 1:
            raise ValueError(
                "attempt_number must be "
                "at least 1"
            )

        return (
                cls._run_prefix(
                    idempotency_key
                )
                + f"{attempt_number:04d}"
        )



def build_runner(
    lake_root: Path,
) -> BipadEventIngestionRunner:
    return BipadEventIngestionRunner(
        lake_root=lake_root,
        settings=get_settings(),
    )