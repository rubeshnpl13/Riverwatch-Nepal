from datetime import datetime

from riverwatch.ingestion.bipad.validation import (
    QuarantinedRecord,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.object_store import (
    ObjectStore,
)
from riverwatch.storage.paths import (
    build_quarantine_record_key,
)
from riverwatch.storage.quarantine import (
    QuarantineRecordEnvelope,
    QuarantineWriteResult,
)
from riverwatch.storage.serialization import (
    serialize_json,
)


class BipadQuarantineWriter:
    def __init__(
        self,
        *,
        store: ObjectStore,
    ) -> None:
        self._store = store

    def write_record(
        self,
        *,
        record: QuarantinedRecord,
        run_id: str,
        captured_at: datetime,
        page_index: int,
    ) -> QuarantineWriteResult:
        key = build_quarantine_record_key(
            provider=DataProvider.BIPAD,
            endpoint=record.endpoint,
            captured_at=captured_at,
            run_id=run_id,
            page_index=page_index,
            record_index=record.record_index,
        )

        envelope = QuarantineRecordEnvelope(
            provider=DataProvider.BIPAD,
            endpoint=record.endpoint,
            run_id=run_id,
            captured_at=captured_at,
            page_index=page_index,
            record_index=record.record_index,
            raw_record=record.raw_record,
            issues=tuple(
                record.issues
            ),
        )

        stored_object = (
            self._store.create_bytes(
                key=key,
                data=serialize_json(
                    envelope
                ),
                content_type=(
                    "application/json"
                ),
            )
        )

        return QuarantineWriteResult(
            object=stored_object,
            envelope=envelope,
        )