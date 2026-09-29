from datetime import (
    UTC,
    datetime,
    timedelta,
)
from pathlib import Path

import pytest

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
from riverwatch.processing.manifest_loader import (
    ProcessingManifestError,
    load_processing_input,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)


def test_loads_completed_run_from_manifest(
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
        7,
        0,
        tzinfo=UTC,
    )

    raw_result = raw_writer.write_page(
        page=BipadRawPage(
            count=1,
            next=None,
            previous=None,
            results=[
                {
                    "id": 44,
                }
            ],
        ),
        endpoint=BipadEndpoint.RIVER,
        run_id="processing-run",
        captured_at=captured_at,
        page_index=0,
    )

    run_result = IngestionRunResult(
        run_id="processing-run",
        endpoint=BipadEndpoint.RIVER,
        started_at=captured_at,
        finished_at=(
            captured_at
            + timedelta(
                seconds=1,
            )
        ),
        duration_ms=1000,
        metrics=IngestionMetrics(
            records_received=1,
            records_valid=1,
            records_invalid=0,
            stations_emitted=0,
            observations_emitted=1,
            records_without_observation=0,
        ),
        quarantined_records=(),
    )

    manifest_result = (
        manifest_writer.write_manifest(
            run_result=run_result,
            captured_at=captured_at,
            raw_pages=[
                raw_result,
            ],
            quarantine_objects=[],
        )
    )

    manifest_path = (
        tmp_path
        / manifest_result.object.key
    )

    processing_input = (
        load_processing_input(
            lake_root=tmp_path,
            manifest_path=(
                manifest_path
            ),
        )
    )

    assert (
        processing_input.run_id
        == "processing-run"
    )

    assert (
        processing_input.endpoint
        == BipadEndpoint.RIVER
    )

    assert len(
        processing_input.pages
    ) == 1

    assert (
        processing_input.pages[0]
        .payload_path
        .exists()
    )

#Test missing payload detection
def test_rejects_manifest_when_payload_is_missing(
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
        7,
        0,
        tzinfo=UTC,
    )

    raw_result = raw_writer.write_page(
        page=BipadRawPage(
            count=1,
            next=None,
            previous=None,
            results=[
                {
                    "id": 44,
                }
            ],
        ),
        endpoint=BipadEndpoint.RIVER,
        run_id="missing-file-run",
        captured_at=captured_at,
        page_index=0,
    )

    run_result = IngestionRunResult(
        run_id="missing-file-run",
        endpoint=BipadEndpoint.RIVER,
        started_at=captured_at,
        finished_at=(
            captured_at
            + timedelta(
                seconds=1,
            )
        ),
        duration_ms=1000,
        metrics=IngestionMetrics(
            records_received=1,
            records_valid=1,
            records_invalid=0,
            stations_emitted=0,
            observations_emitted=1,
            records_without_observation=0,
        ),
        quarantined_records=(),
    )

    manifest_result = (
        manifest_writer.write_manifest(
            run_result=run_result,
            captured_at=captured_at,
            raw_pages=[
                raw_result,
            ],
            quarantine_objects=[],
        )
    )

    payload_path = (
        tmp_path
        / raw_result.payload.key
    )

    payload_path.unlink()

    manifest_path = (
        tmp_path
        / manifest_result.object.key
    )

    with pytest.raises(
        ProcessingManifestError,
        match="does not exist",
    ):
        load_processing_input(
            lake_root=tmp_path,
            manifest_path=(
                manifest_path
            ),
        )

#Test checksum mismatch detection
def test_rejects_modified_raw_payload(
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
        7,
        0,
        tzinfo=UTC,
    )

    raw_result = raw_writer.write_page(
        page=BipadRawPage(
            count=1,
            next=None,
            previous=None,
            results=[
                {
                    "id": 44,
                }
            ],
        ),
        endpoint=BipadEndpoint.RIVER,
        run_id="modified-run",
        captured_at=captured_at,
        page_index=0,
    )

    run_result = IngestionRunResult(
        run_id="modified-run",
        endpoint=BipadEndpoint.RIVER,
        started_at=captured_at,
        finished_at=(
            captured_at
            + timedelta(
                seconds=1,
            )
        ),
        duration_ms=1000,
        metrics=IngestionMetrics(
            records_received=1,
            records_valid=1,
            records_invalid=0,
            stations_emitted=0,
            observations_emitted=1,
            records_without_observation=0,
        ),
        quarantined_records=(),
    )

    manifest_result = (
        manifest_writer.write_manifest(
            run_result=run_result,
            captured_at=captured_at,
            raw_pages=[
                raw_result,
            ],
            quarantine_objects=[],
        )
    )

    payload_path = (
        tmp_path
        / raw_result.payload.key
    )

    payload_path.write_text(
        '{"tampered":true}\n',
        encoding="utf-8",
    )

    manifest_path = (
        tmp_path
        / manifest_result.object.key
    )

    with pytest.raises(
        ProcessingManifestError,
        match="checksum mismatch",
    ):
        load_processing_input(
            lake_root=tmp_path,
            manifest_path=(
                manifest_path
            ),
        )