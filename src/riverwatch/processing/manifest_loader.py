import json
from pathlib import Path

from pydantic import ValidationError

from riverwatch.processing.models import (
    ProcessingInputPage,
    ProcessingInputRun,
)
from riverwatch.storage.manifest import (
    BipadRunManifest,
)
from riverwatch.storage.serialization import (
    sha256_hex,
)


class ProcessingManifestError(Exception):
    """Raised when processing input cannot be resolved."""


def load_processing_input(
    *,
    lake_root: Path,
    manifest_path: Path,
) -> ProcessingInputRun:
    try:
        raw_document = json.loads(
            manifest_path.read_text(
                encoding="utf-8",
            )
        )

    except (
        OSError,
        json.JSONDecodeError,
    ) as exc:
        raise ProcessingManifestError(
            "Unable to read manifest: "
            f"{manifest_path}"
        ) from exc

    try:
        manifest = (
            BipadRunManifest.model_validate(
                raw_document
            )
        )

    except ValidationError as exc:
        raise ProcessingManifestError(
            "Invalid manifest: "
            f"{manifest_path}"
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
        payload_path = (
            lake_root
            / raw_page.payload_key
        )

        if not payload_path.is_file():
            raise ProcessingManifestError(
                "Raw payload does not exist: "
                f"{payload_path}"
            )

        try:
            payload_bytes = (
                payload_path.read_bytes()
            )

        except OSError as exc:
            raise ProcessingManifestError(
                "Unable to read raw payload: "
                f"{payload_path}"
            ) from exc

        actual_sha256 = sha256_hex(
            payload_bytes
        )

        if (
            actual_sha256
            != raw_page.payload_sha256
        ):
            raise ProcessingManifestError(
                "Raw payload checksum mismatch: "
                f"{payload_path}"
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
                    payload_path
                ),
                payload_sha256=(
                    raw_page.payload_sha256
                ),
            )
        )

    return ProcessingInputRun(
        run_id=manifest.run_id,
        endpoint=manifest.endpoint,
        captured_at=manifest.captured_at,
        manifest_path=manifest_path,
        pages=tuple(
            sorted(
                pages,
                key=lambda page: (
                    page.page_index
                ),
            )
        ),
    )