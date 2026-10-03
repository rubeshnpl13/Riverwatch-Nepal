from __future__ import annotations

import logging
from pathlib import Path
from uuid import UUID

from pydantic import ValidationError

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
from riverwatch.ingestion.bipad.service import (
    RiverStationIngestionService,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)
from riverwatch.storage.manifest import (
    BipadRunManifest,
)

LOGGER_NAME = (
    "riverwatch.ingestion.bipad.event_runner"
)

SUPPORTED_PROVIDER = "bipad"

SUPPORTED_ENDPOINT = (
    BipadEndpoint.RIVER_STATIONS.value
)


class BipadEventRunnerError(
    RuntimeError
):
    """BIPAD event runner state is invalid."""


class UnsupportedIngestionRequestError(
    ValueError
):
    """The event runner cannot handle the request."""


class BipadEventIngestionRunner:
    def __init__(
        self,
        *,
        lake_root: Path,
        settings: Settings,
        logger: logging.Logger | None = None,
    ) -> None:
        self._lake_root = (
            lake_root.expanduser().resolve()
        )

        self._settings = settings

        self._logger = (
            logger
            if logger is not None
            else logging.getLogger(
                LOGGER_NAME
            )
        )

        store = LocalObjectStore(
            root=self._lake_root,
        )

        self._raw_writer = (
            BipadRawPageWriter(
                store=store
            )
        )

        self._quarantine_writer = (
            BipadQuarantineWriter(
                store=store
            )
        )

        self._manifest_writer = (
            BipadRunManifestWriter(
                store=store
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

        existing = (
            self._find_completed_run(
                idempotency_key
            )
        )

        if existing is not None:
            return existing

        attempt_number = (
            self._next_attempt_number(
                idempotency_key
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
            self._reference_for_run_id(
                run_id
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

    def _find_completed_run(
        self,
        idempotency_key: UUID,
    ) -> IngestionRunReference | None:
        prefix = self._run_prefix(
            idempotency_key
        )

        manifest_root = (
            self._lake_root
            / "manifests"
            / "provider=bipad"
            / (
                "endpoint="
                f"{SUPPORTED_ENDPOINT}"
            )
        )

        if not manifest_root.is_dir():
            return None

        references: list[
            IngestionRunReference
        ] = []

        for path in manifest_root.rglob(
            "manifest.json"
        ):
            parent_name = (
                path.parent.name
            )

            if not parent_name.startswith(
                "run_id="
            ):
                continue

            run_id = parent_name.removeprefix(
                "run_id="
            )

            if not run_id.startswith(
                prefix
            ):
                continue

            reference = (
                self
                ._load_completed_manifest(
                    path=path,
                    expected_run_id=run_id,
                )
            )

            references.append(
                reference
            )

        if not references:
            return None

        if len(references) > 1:
            raise BipadEventRunnerError(
                "Multiple completed ingestion "
                "runs exist for request "
                f"{idempotency_key}"
            )

        return references[0]

    def _reference_for_run_id(
        self,
        run_id: str,
    ) -> IngestionRunReference | None:
        manifest_root = (
            self._lake_root
            / "manifests"
            / "provider=bipad"
            / (
                "endpoint="
                f"{SUPPORTED_ENDPOINT}"
            )
        )

        if not manifest_root.is_dir():
            return None

        matches = [
            path
            for path in (
                manifest_root.rglob(
                    "manifest.json"
                )
            )
            if (
                path.parent.name
                == f"run_id={run_id}"
            )
        ]

        if not matches:
            return None

        if len(matches) > 1:
            raise BipadEventRunnerError(
                "Multiple manifests exist for "
                f"run_id={run_id}"
            )

        return self._load_completed_manifest(
            path=matches[0],
            expected_run_id=run_id,
        )

    def _load_completed_manifest(
        self,
        *,
        path: Path,
        expected_run_id: str,
    ) -> IngestionRunReference:
        try:
            manifest = (
                BipadRunManifest
                .model_validate_json(
                    path.read_text(
                        encoding="utf-8"
                    )
                )
            )

        except (
            OSError,
            ValidationError,
        ) as exc:
            raise BipadEventRunnerError(
                "Unable to load ingestion "
                f"manifest: {path}"
            ) from exc

        if (
            manifest.provider
            != DataProvider.BIPAD
        ):
            raise BipadEventRunnerError(
                "Recovered manifest has "
                "unexpected provider"
            )

        if (
            manifest.endpoint
            != BipadEndpoint
            .RIVER_STATIONS
        ):
            raise BipadEventRunnerError(
                "Recovered manifest has "
                "unexpected endpoint"
            )

        if (
            manifest.run_id
            != expected_run_id
        ):
            raise BipadEventRunnerError(
                "Recovered manifest run_id "
                "does not match its path"
            )

        if manifest.status != "completed":
            raise BipadEventRunnerError(
                "Recovered ingestion manifest "
                "is not completed"
            )

        try:
            relative_path = (
                path.relative_to(
                    self._lake_root
                )
            )

        except ValueError as exc:
            raise BipadEventRunnerError(
                "Manifest is outside "
                "the lake root"
            ) from exc

        return IngestionRunReference(
            run_id=manifest.run_id,
            manifest_path=(
                relative_path.as_posix()
            ),
        )

    def _next_attempt_number(
        self,
        idempotency_key: UUID,
    ) -> int:
        prefix = self._run_prefix(
            idempotency_key
        )

        highest_attempt = 0

        for top_level in (
            "raw",
            "quarantine",
            "manifests",
        ):
            root = (
                self._lake_root
                / top_level
            )

            if not root.is_dir():
                continue

            for path in root.rglob(
                f"run_id={prefix}*"
            ):
                if not path.is_dir():
                    continue

                run_id = (
                    path.name
                    .removeprefix(
                        "run_id="
                    )
                )

                attempt = (
                    self
                    ._parse_attempt_number(
                        run_id=run_id,
                        prefix=prefix,
                    )
                )

                if attempt is None:
                    continue

                highest_attempt = max(
                    highest_attempt,
                    attempt,
                )

        return highest_attempt + 1

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

    @staticmethod
    def _parse_attempt_number(
        *,
        run_id: str,
        prefix: str,
    ) -> int | None:
        if not run_id.startswith(
            prefix
        ):
            return None

        raw_attempt = run_id[
            len(prefix):
        ]

        if (
            len(raw_attempt) != 4
            or not raw_attempt.isdigit()
        ):
            return None

        attempt = int(
            raw_attempt
        )

        if attempt < 1:
            return None

        return attempt


def build_runner(
    lake_root: Path,
) -> BipadEventIngestionRunner:
    return BipadEventIngestionRunner(
        lake_root=lake_root,
        settings=get_settings(),
    )