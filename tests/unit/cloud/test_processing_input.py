from __future__ import annotations

from datetime import (
    UTC,
    datetime,
    timedelta,
)

import pytest

from riverwatch.cloud.processing_input import (
    load_gcs_processing_input,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.metrics import (
    IngestionMetrics,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.processing.manifest_loader import (
    ProcessingManifestError,
)
from riverwatch.storage.errors import (
    ObjectNotFoundError,
    StorageError,
)
from riverwatch.storage.manifest import (
    BipadRunManifest,
    RawPageLineage,
)
from riverwatch.storage.serialization import (
    sha256_hex,
)

BUCKET_NAME = "riverwatch-dev-lake"

MANIFEST_KEY = (
    "manifests/provider=bipad/"
    "endpoint=river-stations/"
    "run_id=run-123/"
    "manifest.json"
)

PAYLOAD_KEY = (
    "raw/provider=bipad/"
    "endpoint=river-stations/"
    "run_id=run-123/"
    "page=000000/"
    "payload.json"
)

CAPTURED_AT = datetime(
    2026,
    10,
    5,
    12,
    0,
    tzinfo=UTC,
)


class FakeObjectReader:
    def __init__(
        self,
        *,
        objects: dict[
            str,
            bytes,
        ],
    ) -> None:
        self.bucket_name = (
            BUCKET_NAME
        )

        self.objects = dict(
            objects
        )

        self.failures: dict[
            str,
            StorageError,
        ] = {}

    def read_bytes(
        self,
        *,
        key: str,
    ) -> bytes:
        failure = (
            self.failures.get(
                key
            )
        )

        if failure is not None:
            raise failure

        try:
            return self.objects[
                key
            ]

        except KeyError as exc:
            raise ObjectNotFoundError(
                "Object does not exist: "
                f"{key}"
            ) from exc


def build_manifest_bytes(
    *,
    payload: bytes,
    payload_sha256: str | None = None,
) -> bytes:
    manifest = BipadRunManifest(
        provider=DataProvider.BIPAD,
        endpoint=(
            BipadEndpoint
            .RIVER_STATIONS
        ),
        run_id="run-123",
        captured_at=CAPTURED_AT,
        started_at=CAPTURED_AT,
        finished_at=(
            CAPTURED_AT
            + timedelta(
                seconds=1
            )
        ),
        duration_ms=1000,
        metrics=IngestionMetrics(
            records_received=1,
            records_valid=1,
            records_invalid=0,
            stations_emitted=1,
            observations_emitted=1,
            records_without_observation=0,
        ),
        raw_pages=(
            RawPageLineage(
                page_index=0,
                record_count=1,
                payload_key=(
                    PAYLOAD_KEY
                ),
                metadata_key=(
                    "raw/provider=bipad/"
                    "metadata.json"
                ),
                payload_size_bytes=(
                    len(payload)
                ),
                payload_sha256=(
                    payload_sha256
                    if payload_sha256
                    is not None
                    else sha256_hex(
                        payload
                    )
                ),
            ),
        ),
        quarantine_objects=(),
    )

    return (
        manifest
        .model_dump_json()
        .encode(
            "utf-8"
        )
    )


def test_loads_gcs_processing_input(
) -> None:
    payload = (
        b'{"count":1,'
        b'"results":[]}'
    )

    store = FakeObjectReader(
        objects={
            MANIFEST_KEY:
                build_manifest_bytes(
                    payload=payload
                ),
            PAYLOAD_KEY:
                payload,
        }
    )

    result = (
        load_gcs_processing_input(
            store=store,
            manifest_key=MANIFEST_KEY,
        )
    )

    assert result.run_id == "run-123"

    assert result.endpoint is (
        BipadEndpoint
        .RIVER_STATIONS
    )

    assert result.manifest_path == (
        f"gs://{BUCKET_NAME}/"
        f"{MANIFEST_KEY}"
    )

    assert len(
        result.pages
    ) == 1

    assert (
        result.pages[0]
        .payload_path
        == (
            f"gs://{BUCKET_NAME}/"
            f"{PAYLOAD_KEY}"
        )
    )

    assert isinstance(
        result.pages[0].payload_path,
        str,
    )


def test_rejects_missing_manifest(
) -> None:
    store = FakeObjectReader(
        objects={}
    )

    with pytest.raises(
        ProcessingManifestError,
        match=(
            "manifest does not exist"
        ),
    ):
        load_gcs_processing_input(
            store=store,
            manifest_key=MANIFEST_KEY,
        )


def test_rejects_invalid_manifest_json(
) -> None:
    store = FakeObjectReader(
        objects={
            MANIFEST_KEY:
                b"{not-json",
        }
    )

    with pytest.raises(
        ProcessingManifestError,
        match="Unable to read manifest",
    ):
        load_gcs_processing_input(
            store=store,
            manifest_key=MANIFEST_KEY,
        )


def test_rejects_missing_raw_payload(
) -> None:
    payload = b'{"results":[]}'

    store = FakeObjectReader(
        objects={
            MANIFEST_KEY:
                build_manifest_bytes(
                    payload=payload
                ),
        }
    )

    with pytest.raises(
        ProcessingManifestError,
        match=(
            "Raw payload does not exist"
        ),
    ):
        load_gcs_processing_input(
            store=store,
            manifest_key=MANIFEST_KEY,
        )


def test_rejects_payload_checksum_mismatch(
) -> None:
    payload = b'{"results":[]}'

    store = FakeObjectReader(
        objects={
            MANIFEST_KEY:
                build_manifest_bytes(
                    payload=payload
                ),
            PAYLOAD_KEY:
                b'{"tampered":true}',
        }
    )

    with pytest.raises(
        ProcessingManifestError,
        match="checksum mismatch",
    ):
        load_gcs_processing_input(
            store=store,
            manifest_key=MANIFEST_KEY,
        )


def test_maps_payload_storage_failure(
) -> None:
    payload = b'{"results":[]}'

    store = FakeObjectReader(
        objects={
            MANIFEST_KEY:
                build_manifest_bytes(
                    payload=payload
                ),
            PAYLOAD_KEY:
                payload,
        }
    )

    store.failures[
        PAYLOAD_KEY
    ] = StorageError(
        "temporary failure"
    )

    with pytest.raises(
        ProcessingManifestError,
        match=(
            "Unable to read raw payload"
        ),
    ):
        load_gcs_processing_input(
            store=store,
            manifest_key=MANIFEST_KEY,
        )