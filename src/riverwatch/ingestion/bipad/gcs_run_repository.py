from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

import google.cloud.storage as storage  # type: ignore[import-untyped]
from google.api_core.exceptions import (
    GoogleAPIError,
)
from pydantic import ValidationError

from riverwatch.events.worker import (
    IngestionRunReference,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.run_repository import (
    BipadEventRunnerError,
    parse_attempt_number,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.manifest import (
    BipadRunManifest,
)

MANIFEST_PREFIX = (
    "manifests/"
    "provider=bipad/"
    "endpoint=river-stations/"
)

RUN_PREFIXES = (
    (
        "raw/"
        "provider=bipad/"
        "endpoint=river-stations/"
    ),
    (
        "quarantine/"
        "provider=bipad/"
        "endpoint=river-stations/"
    ),
    MANIFEST_PREFIX,
)


class _ReadableBlob(Protocol):
    name: str

    def download_as_bytes(
        self,
    ) -> bytes:
        ...


class _StorageClient(Protocol):
    def list_blobs(
        self,
        bucket_name: str,
        *,
        prefix: str,
    ) -> Iterable[_ReadableBlob]:
        ...


def _extract_run_id(
    object_name: str,
) -> str | None:
    for part in object_name.split(
        "/"
    ):
        if not part.startswith(
            "run_id="
        ):
            continue

        run_id = part.removeprefix(
            "run_id="
        )

        if run_id:
            return run_id

    return None


class GcsIngestionRunRepository:
    """Recover BIPAD ingestion runs from GCS."""

    def __init__(
        self,
        *,
        bucket_name: str,
        client: _StorageClient | None = None,
    ) -> None:
        cleaned_bucket_name = (
            bucket_name.strip()
        )

        if not cleaned_bucket_name:
            raise ValueError(
                "bucket_name cannot be empty"
            )

        self._bucket_name = (
            cleaned_bucket_name
        )

        if client is None:
            self._client: _StorageClient = (
                storage.Client()
            )
        else:
            self._client = client

    @property
    def bucket_name(
        self,
    ) -> str:
        return self._bucket_name

    def find_completed_run(
        self,
        *,
        run_prefix: str,
    ) -> IngestionRunReference | None:
        references: list[
            IngestionRunReference
        ] = []

        for blob in self._list_blobs(
            prefix=MANIFEST_PREFIX,
        ):
            if not blob.name.endswith(
                "/manifest.json"
            ):
                continue

            run_id = _extract_run_id(
                blob.name
            )

            if (
                run_id is None
                or not run_id.startswith(
                    run_prefix
                )
            ):
                continue

            references.append(
                self._load_completed_manifest(
                    blob=blob,
                    expected_run_id=run_id,
                )
            )

        if not references:
            return None

        if len(references) > 1:
            raise BipadEventRunnerError(
                "Multiple completed ingestion "
                "runs exist for run prefix "
                f"{run_prefix}"
            )

        return references[0]

    def reference_for_run_id(
        self,
        *,
        run_id: str,
    ) -> IngestionRunReference | None:
        matches: list[
            _ReadableBlob
        ] = []

        for blob in self._list_blobs(
            prefix=MANIFEST_PREFIX,
        ):
            if not blob.name.endswith(
                "/manifest.json"
            ):
                continue

            if (
                _extract_run_id(
                    blob.name
                )
                != run_id
            ):
                continue

            matches.append(
                blob
            )

        if not matches:
            return None

        if len(matches) > 1:
            raise BipadEventRunnerError(
                "Multiple manifests exist for "
                f"run_id={run_id}"
            )

        return self._load_completed_manifest(
            blob=matches[0],
            expected_run_id=run_id,
        )

    def next_attempt_number(
        self,
        *,
        run_prefix: str,
    ) -> int:
        highest_attempt = 0

        for prefix in RUN_PREFIXES:
            for blob in self._list_blobs(
                prefix=prefix,
            ):
                run_id = _extract_run_id(
                    blob.name
                )

                if run_id is None:
                    continue

                attempt = (
                    parse_attempt_number(
                        run_id=run_id,
                        run_prefix=run_prefix,
                    )
                )

                if attempt is None:
                    continue

                highest_attempt = max(
                    highest_attempt,
                    attempt,
                )

        return highest_attempt + 1

    def _list_blobs(
        self,
        *,
        prefix: str,
    ) -> tuple[_ReadableBlob, ...]:
        try:
            return tuple(
                self._client.list_blobs(
                    self._bucket_name,
                    prefix=prefix,
                )
            )

        except GoogleAPIError as exc:
            raise BipadEventRunnerError(
                "Unable to list GCS ingestion "
                f"objects under {prefix}"
            ) from exc

    def _load_completed_manifest(
        self,
        *,
        blob: _ReadableBlob,
        expected_run_id: str,
    ) -> IngestionRunReference:
        try:
            manifest_bytes = (
                blob.download_as_bytes()
            )

        except GoogleAPIError as exc:
            raise BipadEventRunnerError(
                "Unable to load ingestion "
                f"manifest: {blob.name}"
            ) from exc

        try:
            manifest = (
                BipadRunManifest
                .model_validate_json(
                    manifest_bytes
                )
            )

        except ValidationError as exc:
            raise BipadEventRunnerError(
                "Unable to load ingestion "
                f"manifest: {blob.name}"
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
            != BipadEndpoint.RIVER_STATIONS
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

        return IngestionRunReference(
            run_id=manifest.run_id,
            manifest_path=blob.name,
            completed_at=(
                manifest.finished_at
            ),
        )