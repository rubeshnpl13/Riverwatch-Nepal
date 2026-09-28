from datetime import datetime

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.pagination import (
    BipadRawPage,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.object_store import (
    ObjectStore,
)
from riverwatch.storage.paths import (
    build_raw_metadata_key,
    build_raw_payload_key,
)
from riverwatch.storage.raw import (
    RawPageMetadata,
    RawPageWriteResult,
)
from riverwatch.storage.serialization import (
    serialize_json,
)


class BipadRawPageWriter:
    def __init__(
        self,
        *,
        store: ObjectStore,
    ) -> None:
        self._store = store

    def write_page(
        self,
        *,
        page: BipadRawPage,
        endpoint: BipadEndpoint,
        run_id: str,
        captured_at: datetime,
        page_index: int,
    ) -> RawPageWriteResult:
        payload_key = build_raw_payload_key(
            provider=DataProvider.BIPAD,
            endpoint=endpoint,
            captured_at=captured_at,
            run_id=run_id,
            page_index=page_index,
        )

        metadata_key = build_raw_metadata_key(
            provider=DataProvider.BIPAD,
            endpoint=endpoint,
            captured_at=captured_at,
            run_id=run_id,
            page_index=page_index,
        )

        payload_bytes = serialize_json(
            page.model_dump(
                mode="json"
            )
        )

        payload_object = self._store.create_bytes(
            key=payload_key,
            data=payload_bytes,
            content_type="application/json",
        )

        page_metadata = RawPageMetadata(
            provider=DataProvider.BIPAD,
            endpoint=endpoint,
            run_id=run_id,
            page_index=page_index,
            captured_at=captured_at,
            record_count=len(
                page.results
            ),
            payload_key=payload_object.key,
            payload_size_bytes=(
                payload_object.size_bytes
            ),
            payload_sha256=(
                payload_object.sha256
            ),
        )

        metadata_object = self._store.create_bytes(
            key=metadata_key,
            data=serialize_json(
                page_metadata
            ),
            content_type="application/json",
        )

        return RawPageWriteResult(
            payload=payload_object,
            metadata=metadata_object,
            page_metadata=page_metadata,
        )