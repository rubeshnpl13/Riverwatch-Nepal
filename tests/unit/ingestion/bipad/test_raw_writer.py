import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.pagination import (
    BipadRawPage,
)
from riverwatch.ingestion.bipad.raw_writer import (
    BipadRawPageWriter,
)
from riverwatch.storage.errors import (
    ObjectAlreadyExistsError,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)


def test_writes_raw_payload_and_metadata(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    writer = BipadRawPageWriter(
        store=store
    )

    captured_at = datetime(
        2026,
        9,
        29,
        1,
        0,
        tzinfo=UTC,
    )

    page = BipadRawPage(
        count=2,
        next=None,
        previous=None,
        results=[
            {
                "id": 1,
                "title": "Station One",
            },
            {
                "id": 2,
                "title": "Station Two",
            },
        ],
    )

    result = writer.write_page(
        page=page,
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
        run_id="test-run",
        captured_at=captured_at,
        page_index=0,
    )

    payload_path = (
        tmp_path
        / result.payload.key
    )

    metadata_path = (
        tmp_path
        / result.metadata.key
    )

    assert payload_path.exists()
    assert metadata_path.exists()

    payload = json.loads(
        payload_path.read_text(
            encoding="utf-8"
        )
    )

    metadata = json.loads(
        metadata_path.read_text(
            encoding="utf-8"
        )
    )

    assert (
        len(payload["results"])
        == 2
    )

    assert (
        metadata["provider"]
        == "bipad"
    )

    assert (
        metadata["endpoint"]
        == "river-stations"
    )

    assert (
        metadata["record_count"]
        == 2
    )

    assert (
        metadata["payload_key"]
        == result.payload.key
    )

    assert (
        metadata["payload_sha256"]
        == result.payload.sha256
    )

#Test immutable behavior through the writer
def test_same_raw_page_cannot_be_overwritten(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    writer = BipadRawPageWriter(
        store=store
    )

    captured_at = datetime(
        2026,
        9,
        29,
        1,
        0,
        tzinfo=UTC,
    )

    page = BipadRawPage(
        count=1,
        next=None,
        previous=None,
        results=[
            {
                "id": 1,
            }
        ],
    )

    writer.write_page(
        page=page,
        endpoint=BipadEndpoint.RIVER,
        run_id="same-run",
        captured_at=captured_at,
        page_index=0,
    )

    with pytest.raises(
        ObjectAlreadyExistsError
    ):
        writer.write_page(
            page=page,
            endpoint=BipadEndpoint.RIVER,
            run_id="same-run",
            captured_at=captured_at,
            page_index=0,
        )

#Test page-specific keys
def test_different_pages_get_different_keys(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    writer = BipadRawPageWriter(
        store=store
    )

    captured_at = datetime(
        2026,
        9,
        29,
        1,
        0,
        tzinfo=UTC,
    )

    page = BipadRawPage(
        count=1,
        next=None,
        previous=None,
        results=[
            {
                "id": 1,
            }
        ],
    )

    first = writer.write_page(
        page=page,
        endpoint=BipadEndpoint.RIVER,
        run_id="run-1",
        captured_at=captured_at,
        page_index=0,
    )

    second = writer.write_page(
        page=page,
        endpoint=BipadEndpoint.RIVER,
        run_id="run-1",
        captured_at=captured_at,
        page_index=1,
    )

    assert (
        first.payload.key
        != second.payload.key
    )

    assert (
        "page=000000"
        in first.payload.key
    )

    assert (
        "page=000001"
        in second.payload.key
    )