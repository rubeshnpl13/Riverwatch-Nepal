from __future__ import annotations

import json
from typing import Protocol

from pydantic import ValidationError

from riverwatch.processing.manifest_loader import (
    ProcessingManifestError,
)
from riverwatch.processing.models import (
    ProcessingInputPage,
    ProcessingInputRun,
)
from riverwatch.storage.errors import (
    ObjectNotFoundError,
    StorageError,
)
from riverwatch.storage.manifest import (
    BipadRunManifest,
)
from riverwatch.storage.serialization import (
    sha256_hex,
)


class ProcessingObjectReader(
    Protocol,
):
    @property
    def bucket_name(
        self,
    ) -> str:
        ...

    def read_bytes(
        self,
        *,
        key: str,
    ) -> bytes:
        ...


def gcs_uri(
    *,
    bucket_name: str,
    key: str,
) -> str:
    normalized_bucket = (
        bucket_name.strip()
    )

    if not normalized_bucket:
        raise ValueError(
            "bucket_name must not be blank"
        )

    normalized_key = key.strip()

    if not normalized_key:
        raise ValueError(
            "key must not be blank"
        )

    if normalized_key != key:
        raise ValueError(
            "key must not have leading "
            "or trailing whitespace"
        )

    return (
        f"gs://{normalized_bucket}/"
        f"{normalized_key}"
    )


def load_gcs_processing_input(
    *,
    store: ProcessingObjectReader,
    manifest_key: str,
) -> ProcessingInputRun:
    manifest_uri = gcs_uri(
        bucket_name=store.bucket_name,
        key=manifest_key,
    )

    try:
        manifest_bytes = (
            store.read_bytes(
                key=manifest_key
            )
        )

    except ObjectNotFoundError as exc:
        raise ProcessingManifestError(
            "Ingestion manifest does "
            "not exist: "
            f"{manifest_uri}"
        ) from exc

    except StorageError as exc:
        raise ProcessingManifestError(
            "Unable to read manifest: "
            f"{manifest_uri}"
        ) from exc

    try:
        raw_document = json.loads(
            manifest_bytes
        )

    except (
        json.JSONDecodeError,
        UnicodeDecodeError,
    ) as exc:
        raise ProcessingManifestError(
            "Unable to read manifest: "
            f"{manifest_uri}"
        ) from exc

    try:
        manifest = (
            BipadRunManifest
            .model_validate(
                raw_document
            )
        )

    except ValidationError as exc:
        raise ProcessingManifestError(
            "Invalid manifest: "
            f"{manifest_uri}"
        ) from exc

    if manifest.status != "completed":
        raise ProcessingManifestError(
            "Only completed ingestion runs "
            "can be processed"
        )

    pages: list[
        ProcessingInputPage
    ] = []

    for raw_page in manifest.raw_pages:
        payload_uri = gcs_uri(
            bucket_name=(
                store.bucket_name
            ),
            key=raw_page.payload_key,
        )

        try:
            payload_bytes = (
                store.read_bytes(
                    key=(
                        raw_page
                        .payload_key
                    )
                )
            )

        except ObjectNotFoundError as exc:
            raise ProcessingManifestError(
                "Raw payload does not exist: "
                f"{payload_uri}"
            ) from exc

        except StorageError as exc:
            raise ProcessingManifestError(
                "Unable to read raw payload: "
                f"{payload_uri}"
            ) from exc

        actual_sha256 = sha256_hex(
            payload_bytes
        )

        if (
            actual_sha256
            != raw_page.payload_sha256
        ):
            raise ProcessingManifestError(
                "Raw payload checksum "
                "mismatch: "
                f"{payload_uri}"
            )

        pages.append(
            ProcessingInputPage(
                page_index=(
                    raw_page.page_index
                ),
                record_count=(
                    raw_page.record_count
                ),
                payload_key=(
                    raw_page.payload_key
                ),
                payload_path=(
                    payload_uri
                ),
                payload_sha256=(
                    raw_page
                    .payload_sha256
                ),
            )
        )

    return ProcessingInputRun(
        run_id=manifest.run_id,
        endpoint=manifest.endpoint,
        captured_at=(
            manifest.captured_at
        ),
        manifest_path=manifest_uri,
        pages=tuple(
            sorted(
                pages,
                key=lambda page: (
                    page.page_index
                ),
            )
        ),
    )