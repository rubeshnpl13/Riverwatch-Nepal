from datetime import (
    UTC,
    datetime,
)
from pathlib import Path

import pytest

from riverwatch.events.worker import (
    IngestionRunReference,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.run_repository import (
    BipadEventRunnerError,
    LocalIngestionRunRepository,
)
from riverwatch.ingestion.metrics import (
    IngestionMetrics,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.manifest import (
    BipadRunManifest,
)


def _write_manifest(
    *,
    lake_root: Path,
    run_id: str,
) -> Path:
    captured_at = datetime(
        2026,
        10,
        5,
        0,
        0,
        tzinfo=UTC,
    )

    manifest = BipadRunManifest(
        provider=DataProvider.BIPAD,
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
        run_id=run_id,
        captured_at=captured_at,
        started_at=captured_at,
        finished_at=captured_at,
        duration_ms=0,
        metrics=IngestionMetrics(
            records_received=0,
            records_valid=0,
            records_invalid=0,
            stations_emitted=0,
            observations_emitted=0,
            records_without_observation=0,
        ),
        raw_pages=(),
        quarantine_objects=(),
    )

    path = (
        lake_root
        / "manifests"
        / "provider=bipad"
        / "endpoint=river-stations"
        / "year=2026"
        / "month=10"
        / "day=05"
        / "hour=00"
        / f"run_id={run_id}"
        / "manifest.json"
    )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        manifest.model_dump_json(),
        encoding="utf-8",
    )

    return path


def test_find_completed_run(
    tmp_path: Path,
) -> None:
    run_prefix = (
        "event-test-request-attempt-"
    )

    run_id = (
        f"{run_prefix}0001"
    )

    path = _write_manifest(
        lake_root=tmp_path,
        run_id=run_id,
    )

    repository = (
        LocalIngestionRunRepository(
            lake_root=tmp_path,
        )
    )

    result = repository.find_completed_run(
        run_prefix=run_prefix,
    )

    assert result == IngestionRunReference(
        run_id=run_id,
        manifest_path=(
            path
            .relative_to(tmp_path)
            .as_posix()
        ),
        completed_at=datetime(
            2026,
            10,
            5,
            0,
            0,
            tzinfo=UTC,
        ),
    )


def test_reference_for_run_id(
    tmp_path: Path,
) -> None:
    run_id = (
        "event-test-request-attempt-0001"
    )

    _write_manifest(
        lake_root=tmp_path,
        run_id=run_id,
    )

    repository = (
        LocalIngestionRunRepository(
            lake_root=tmp_path,
        )
    )

    result = (
        repository.reference_for_run_id(
            run_id=run_id,
        )
    )

    assert result.completed_at == datetime(
        2026,
        10,
        5,
        0,
        0,
        tzinfo=UTC,
    )


def test_next_attempt_number_detects_partial_run(
    tmp_path: Path,
) -> None:
    run_prefix = (
        "event-test-request-attempt-"
    )

    partial = (
        tmp_path
        / "raw"
        / "provider=bipad"
        / "endpoint=river-stations"
        / "year=2026"
        / "month=10"
        / "day=05"
        / "hour=00"
        / (
            "run_id="
            f"{run_prefix}0003"
        )
    )

    partial.mkdir(
        parents=True,
    )

    repository = (
        LocalIngestionRunRepository(
            lake_root=tmp_path,
        )
    )

    assert (
        repository.next_attempt_number(
            run_prefix=run_prefix,
        )
        == 4
    )


def test_next_attempt_number_defaults_to_one(
    tmp_path: Path,
) -> None:
    repository = (
        LocalIngestionRunRepository(
            lake_root=tmp_path,
        )
    )

    assert (
        repository.next_attempt_number(
            run_prefix=(
                "event-test-request-attempt-"
            ),
        )
        == 1
    )


def test_multiple_completed_runs_are_rejected(
    tmp_path: Path,
) -> None:
    run_prefix = (
        "event-test-request-attempt-"
    )

    _write_manifest(
        lake_root=tmp_path,
        run_id=f"{run_prefix}0001",
    )

    _write_manifest(
        lake_root=tmp_path,
        run_id=f"{run_prefix}0002",
    )

    repository = (
        LocalIngestionRunRepository(
            lake_root=tmp_path,
        )
    )

    with pytest.raises(
        BipadEventRunnerError,
        match="Multiple completed",
    ):
        repository.find_completed_run(
            run_prefix=run_prefix,
        )