import json
from datetime import (
    UTC,
    datetime,
    timedelta,
)
from pathlib import Path

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.manifest_writer import (
    BipadRunManifestWriter,
)
from riverwatch.ingestion.bipad.pagination import (
    BipadRawPage,
)
from riverwatch.ingestion.bipad.raw_writer import (
    BipadRawPageWriter,
)
from riverwatch.ingestion.metrics import (
    IngestionMetrics,
    IngestionRunResult,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)


def test_writes_run_manifest(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    raw_writer = BipadRawPageWriter(
        store=store
    )

    manifest_writer = (
        BipadRunManifestWriter(
            store=store
        )
    )

    captured_at = datetime(
        2026,
        9,
        29,
        3,
        0,
        tzinfo=UTC,
    )

    raw_page = raw_writer.write_page(
        page=BipadRawPage(
            count=2,
            next=None,
            previous=None,
            results=[
                {
                    "id": 1,
                },
                {
                    "id": 2,
                },
            ],
        ),
        endpoint=BipadEndpoint.RIVER,
        run_id="run-123",
        captured_at=captured_at,
        page_index=0,
    )

    finished_at = (
        captured_at
        + timedelta(
            seconds=2,
        )
    )

    run_result = IngestionRunResult(
        run_id="run-123",
        endpoint=BipadEndpoint.RIVER,
        started_at=captured_at,
        finished_at=finished_at,
        duration_ms=2000.0,
        metrics=IngestionMetrics(
            records_received=2,
            records_valid=2,
            records_invalid=0,
            stations_emitted=0,
            observations_emitted=2,
            records_without_observation=0,
        ),
        quarantined_records=(),
    )

    result = manifest_writer.write_manifest(
        run_result=run_result,
        captured_at=captured_at,
        raw_pages=[
            raw_page,
        ],
        quarantine_objects=[],
    )

    path = (
        tmp_path
        / result.object.key
    )

    assert path.exists()

    document = json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )

    assert (
        document["manifest_version"]
        == 1
    )

    assert (
        document["status"]
        == "completed"
    )

    assert (
        document["run_id"]
        == "run-123"
    )

    assert (
        document["endpoint"]
        == "river"
    )

    assert (
        document["metrics"][
            "records_received"
        ]
        == 2
    )

    assert (
        len(
            document["raw_pages"]
        )
        == 1
    )

    assert (
        document["raw_pages"][0][
            "payload_key"
        ]
        == raw_page.payload.key
    )

    assert (
        document["raw_pages"][0][
            "payload_sha256"
        ]
        == raw_page.payload.sha256
    )

    assert (
        document[
            "quarantine_objects"
        ]
        == []
    )