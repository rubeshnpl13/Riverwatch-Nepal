import os
from pathlib import Path

import pytest

from riverwatch.ingestion.bipad.endpoints import BipadEndpoint
from riverwatch.processing.discovery import (
    ProcessingManifestError,
    find_latest_manifest,
)


def test_finds_latest_manifest(
    tmp_path: Path,
) -> None:
    first = (
        tmp_path
        / "manifests"
        / "provider=bipad"
        / "endpoint=river"
        / "year=2026"
        / "run_id=first"
        / "manifest.json"
    )

    second = (
        tmp_path
        / "manifests"
        / "provider=bipad"
        / "endpoint=river"
        / "year=2026"
        / "run_id=second"
        / "manifest.json"
    )

    first.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    second.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    first.write_text(
        "{}",
        encoding="utf-8",
    )
    second.write_text(
        "{}",
        encoding="utf-8",
    )

    os.utime(
        first,
        (
            1_000_000,
            1_000_000,
        ),
    )

    os.utime(
        second,
        (
            2_000_000,
            2_000_000,
        ),
    )

    latest = find_latest_manifest(
        lake_root=tmp_path,
        endpoint=BipadEndpoint.RIVER,
    )

    assert latest == second


def test_latest_manifest_requires_existing_run(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ProcessingManifestError,
        match="No completed manifest",
    ):
        find_latest_manifest(
            lake_root=tmp_path,
            endpoint=BipadEndpoint.RIVER,
        )