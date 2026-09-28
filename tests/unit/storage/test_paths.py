from datetime import (
    datetime,
    timedelta,
    timezone,
)

import pytest

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.errors import (
    InvalidObjectKeyError,
)
from riverwatch.storage.paths import (
    build_quarantine_record_key,
    build_raw_metadata_key,
    build_raw_page_prefix,
    build_raw_payload_key,
    build_run_manifest_key,
)


def test_builds_raw_page_prefix_using_utc() -> None:
    nepal_timezone = timezone(
        timedelta(
            hours=5,
            minutes=45,
        )
    )

    captured_at = datetime(
        2026,
        9,
        29,
        0,
        15,
        tzinfo=nepal_timezone,
    )

    prefix = build_raw_page_prefix(
        provider=DataProvider.BIPAD,
        endpoint=BipadEndpoint.RIVER,
        captured_at=captured_at,
        run_id="run-123",
        page_index=4,
    )

    assert prefix == (
        "raw/"
        "provider=bipad/"
        "endpoint=river/"
        "year=2026/"
        "month=09/"
        "day=28/"
        "hour=18/"
        "run_id=run-123/"
        "page=000004"
    )


def test_builds_payload_and_metadata_keys() -> None:
    captured_at = datetime.fromisoformat(
        "2026-09-29T00:00:00+00:00"
    )

    payload_key = build_raw_payload_key(
        provider=DataProvider.BIPAD,
        endpoint=BipadEndpoint.RIVER_STATIONS,
        captured_at=captured_at,
        run_id="run-abc",
        page_index=0,
    )

    metadata_key = build_raw_metadata_key(
        provider=DataProvider.BIPAD,
        endpoint=BipadEndpoint.RIVER_STATIONS,
        captured_at=captured_at,
        run_id="run-abc",
        page_index=0,
    )

    assert payload_key.endswith(
        "/page=000000/payload.json"
    )

    assert metadata_key.endswith(
        "/page=000000/metadata.json"
    )

    assert payload_key != metadata_key


def test_rejects_run_id_containing_slash() -> None:
    captured_at = datetime.fromisoformat(
        "2026-09-29T00:00:00+00:00"
    )

    with pytest.raises(
        InvalidObjectKeyError,
        match="run_id",
    ):
        build_raw_page_prefix(
            provider=DataProvider.BIPAD,
            endpoint=BipadEndpoint.RIVER,
            captured_at=captured_at,
            run_id="../../bad/run",
            page_index=0,
        )


def test_rejects_negative_page_index() -> None:
    captured_at = datetime.fromisoformat(
        "2026-09-29T00:00:00+00:00"
    )

    with pytest.raises(
        ValueError,
        match="page_index",
    ):
        build_raw_page_prefix(
            provider=DataProvider.BIPAD,
            endpoint=BipadEndpoint.RIVER,
            captured_at=captured_at,
            run_id="run-123",
            page_index=-1,
        )

def test_builds_quarantine_record_key() -> None:
    captured_at = datetime.fromisoformat(
        "2026-09-29T03:00:00+00:00"
    )

    key = build_quarantine_record_key(
        provider=DataProvider.BIPAD,
        endpoint=BipadEndpoint.RIVER,
        captured_at=captured_at,
        run_id="run-123",
        page_index=1,
        record_index=1042,
    )

    assert key == (
        "quarantine/"
        "provider=bipad/"
        "endpoint=river/"
        "year=2026/"
        "month=09/"
        "day=29/"
        "hour=03/"
        "run_id=run-123/"
        "page=000001/"
        "record=000001042.json"
    )

def test_builds_run_manifest_key() -> None:
    captured_at = datetime.fromisoformat(
        "2026-09-29T03:00:00+00:00"
    )

    key = build_run_manifest_key(
        provider=DataProvider.BIPAD,
        endpoint=BipadEndpoint.RIVER,
        captured_at=captured_at,
        run_id="run-123",
    )

    assert key == (
        "manifests/"
        "provider=bipad/"
        "endpoint=river/"
        "year=2026/"
        "month=09/"
        "day=29/"
        "hour=03/"
        "run_id=run-123/"
        "manifest.json"
    )