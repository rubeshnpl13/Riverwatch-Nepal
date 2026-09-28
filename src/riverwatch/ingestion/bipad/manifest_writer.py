from collections.abc import Sequence
from datetime import datetime

from riverwatch.ingestion.metrics import (
    IngestionRunResult,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.manifest import (
    BipadRunManifest,
    ManifestWriteResult,
    QuarantineLineage,
    RawPageLineage,
)
from riverwatch.storage.object_store import (
    ObjectStore,
)
from riverwatch.storage.paths import (
    build_run_manifest_key,
)
from riverwatch.storage.quarantine import (
    QuarantineWriteResult,
)
from riverwatch.storage.raw import (
    RawPageWriteResult,
)
from riverwatch.storage.serialization import (
    serialize_json,
)


class BipadRunManifestWriter:
    def __init__(
        self,
        *,
        store: ObjectStore,
    ) -> None:
        self._store = store

    def write_manifest(
        self,
        *,
        run_result: IngestionRunResult,
        captured_at: datetime,
        raw_pages: Sequence[
            RawPageWriteResult
        ],
        quarantine_objects: Sequence[
            QuarantineWriteResult
        ],
    ) -> ManifestWriteResult:
        raw_lineage = tuple(
            RawPageLineage(
                page_index=(
                    result
                    .page_metadata
                    .page_index
                ),
                record_count=(
                    result
                    .page_metadata
                    .record_count
                ),
                payload_key=(
                    result.payload.key
                ),
                metadata_key=(
                    result.metadata.key
                ),
                payload_size_bytes=(
                    result
                    .payload
                    .size_bytes
                ),
                payload_sha256=(
                    result
                    .payload
                    .sha256
                ),
            )
            for result in raw_pages
        )

        quarantine_lineage = tuple(
            QuarantineLineage(
                page_index=(
                    result
                    .envelope
                    .page_index
                ),
                record_index=(
                    result
                    .envelope
                    .record_index
                ),
                object_key=(
                    result.object.key
                ),
            )
            for result in quarantine_objects
        )

        manifest = BipadRunManifest(
            provider=DataProvider.BIPAD,
            endpoint=run_result.endpoint,
            run_id=run_result.run_id,
            captured_at=captured_at,
            started_at=(
                run_result.started_at
            ),
            finished_at=(
                run_result.finished_at
            ),
            duration_ms=(
                run_result.duration_ms
            ),
            metrics=run_result.metrics,
            raw_pages=raw_lineage,
            quarantine_objects=(
                quarantine_lineage
            ),
        )

        key = build_run_manifest_key(
            provider=DataProvider.BIPAD,
            endpoint=run_result.endpoint,
            captured_at=captured_at,
            run_id=run_result.run_id,
        )

        stored_object = (
            self._store.create_bytes(
                key=key,
                data=serialize_json(
                    manifest
                ),
                content_type=(
                    "application/json"
                ),
            )
        )

        return ManifestWriteResult(
            object=stored_object,
            manifest=manifest,
        )