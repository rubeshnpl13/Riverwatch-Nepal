from dataclasses import asdict

from riverwatch.quality.model import (
    QualityRunReport,
)
from riverwatch.storage.models import (
    StoredObject,
)
from riverwatch.storage.object_store import (
    ObjectStore,
)
from riverwatch.storage.paths import (
    build_quality_report_key,
)
from riverwatch.storage.serialization import (
    serialize_json,
)


class QualityReportWriter:
    def __init__(
        self,
        *,
        store: ObjectStore,
    ) -> None:
        self._store = store

    def write_report(
        self,
        *,
        report: QualityRunReport,
    ) -> StoredObject:
        key = build_quality_report_key(
            endpoint=report.endpoint,
            captured_at=report.captured_at,
            run_id=report.run_id,
        )

        data = serialize_json(
            asdict(
                report
            )
        )

        return self._store.create_bytes(
            key=key,
            data=data,
            content_type=(
                "application/json"
            ),
        )