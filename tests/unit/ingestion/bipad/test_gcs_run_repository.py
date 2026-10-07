
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import (
    UTC,
    datetime,
)

import pytest
from google.api_core.exceptions import (
    ServiceUnavailable,
)

from riverwatch.events.worker import (
    IngestionRunReference,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.gcs_run_repository import (
    GcsIngestionRunRepository,
)
from riverwatch.ingestion.bipad.run_repository import (
    BipadEventRunnerError,
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


@dataclass
class FakeBlob:
    name: str
    data: bytes = b""
    error: Exception | None = None

    def download_as_bytes(
        self,
    ) -> bytes:
        if self.error is not None:
            raise self.error

        return self.data


class FakeStorageClient:
    def __init__(
        self,
        *,
        blobs: Iterable[FakeBlob] = (),
        error: Exception | None = None,
    ) -> None:
        self.blobs = tuple(
            blobs
        )

        self.error = error

        self.list_calls: list[
            tuple[str, str]
        ] = []

    def list_blobs(
        self,
        bucket_name: str,
        *,
        prefix: str,
    ) -> Iterable[FakeBlob]:
        self.list_calls.append(
            (
                bucket_name,
                prefix,
            )
        )

        if self.error is not None:
            raise self.error

        return (
            blob
            for blob in self.blobs
            if blob.name.startswith(
                prefix
            )
        )


def _manifest_blob(
    *,
    run_id: str,
) -> FakeBlob:
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

    return FakeBlob(
        name=(
            "manifests/"
            "provider=bipad/"
            "endpoint=river-stations/"
            "year=2026/"
            "month=10/"
            "day=05/"
            "hour=00/"
            f"run_id={run_id}/"
            "manifest.json"
        ),
        data=(
            manifest
            .model_dump_json()
            .encode("utf-8")
        ),
    )


def test_find_completed_run() -> None:
    run_prefix = (
        "event-test-request-attempt-"
    )

    run_id = (
        f"{run_prefix}0001"
    )

    blob = _manifest_blob(
        run_id=run_id
    )

    repository = (
        GcsIngestionRunRepository(
            bucket_name=(
                "riverwatch-dev-lake"
            ),
            client=FakeStorageClient(
                blobs=[blob]
            ),
        )
    )

    result = repository.find_completed_run(
        run_prefix=run_prefix,
    )

    assert result == IngestionRunReference(
        run_id=run_id,
        manifest_path=blob.name,
        completed_at=datetime(
            2026,
            10,
            5,
            0,
            0,
            tzinfo=UTC,
        ),
    )


def test_reference_for_run_id() -> None:
    run_id = (
        "event-test-request-attempt-0001"
    )

    blob = _manifest_blob(
        run_id=run_id
    )

    repository = (
        GcsIngestionRunRepository(
            bucket_name=(
                "riverwatch-dev-lake"
            ),
            client=FakeStorageClient(
                blobs=[blob]
            ),
        )
    )

    result = (
        repository.reference_for_run_id(
            run_id=run_id,
        )
    )

    assert result == IngestionRunReference(
        run_id=run_id,
        manifest_path=blob.name,
        completed_at=datetime(
            2026,
            10,
            5,
            0,
            0,
            tzinfo=UTC,
        ),
    )


def test_next_attempt_number_scans_lake_prefixes() -> None:
    run_prefix = (
        "event-test-request-attempt-"
    )

    client = FakeStorageClient(
        blobs=[
            FakeBlob(
                name=(
                    "raw/"
                    "provider=bipad/"
                    "endpoint=river-stations/"
                    "year=2026/"
                    "run_id="
                    f"{run_prefix}0001/"
                    "page=000000/"
                    "payload.json"
                )
            ),
            FakeBlob(
                name=(
                    "quarantine/"
                    "provider=bipad/"
                    "endpoint=river-stations/"
                    "year=2026/"
                    "run_id="
                    f"{run_prefix}0003/"
                    "record=000001.json"
                )
            ),
        ]
    )

    repository = (
        GcsIngestionRunRepository(
            bucket_name=(
                "riverwatch-dev-lake"
            ),
            client=client,
        )
    )

    assert (
        repository.next_attempt_number(
            run_prefix=run_prefix,
        )
        == 4
    )

    assert len(
        client.list_calls
    ) == 3


def test_next_attempt_number_defaults_to_one() -> None:
    repository = (
        GcsIngestionRunRepository(
            bucket_name=(
                "riverwatch-dev-lake"
            ),
            client=FakeStorageClient(),
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


def test_multiple_completed_runs_are_rejected() -> None:
    run_prefix = (
        "event-test-request-attempt-"
    )

    repository = (
        GcsIngestionRunRepository(
            bucket_name=(
                "riverwatch-dev-lake"
            ),
            client=FakeStorageClient(
                blobs=[
                    _manifest_blob(
                        run_id=(
                            f"{run_prefix}0001"
                        )
                    ),
                    _manifest_blob(
                        run_id=(
                            f"{run_prefix}0002"
                        )
                    ),
                ]
            ),
        )
    )

    with pytest.raises(
        BipadEventRunnerError,
        match="Multiple completed",
    ):
        repository.find_completed_run(
            run_prefix=run_prefix,
        )


def test_invalid_manifest_is_rejected() -> None:
    run_id = (
        "event-test-request-attempt-0001"
    )

    blob = FakeBlob(
        name=(
            "manifests/"
            "provider=bipad/"
            "endpoint=river-stations/"
            f"run_id={run_id}/"
            "manifest.json"
        ),
        data=b"not-json",
    )

    repository = (
        GcsIngestionRunRepository(
            bucket_name=(
                "riverwatch-dev-lake"
            ),
            client=FakeStorageClient(
                blobs=[blob]
            ),
        )
    )

    with pytest.raises(
        BipadEventRunnerError,
        match="Unable to load ingestion manifest",
    ):
        repository.reference_for_run_id(
            run_id=run_id,
        )


def test_list_failure_is_mapped_to_runner_error() -> None:
    error = ServiceUnavailable(  # type: ignore[no-untyped-call]
        "storage unavailable"
    )

    repository = (
        GcsIngestionRunRepository(
            bucket_name=(
                "riverwatch-dev-lake"
            ),
            client=FakeStorageClient(
                error=error
            ),
        )
    )

    with pytest.raises(
        BipadEventRunnerError,
        match="Unable to list GCS",
    ):
        repository.find_completed_run(
            run_prefix=(
                "event-test-request-attempt-"
            ),
        )


def test_download_failure_is_mapped_to_runner_error() -> None:
    run_id = (
        "event-test-request-attempt-0001"
    )

    blob = _manifest_blob(
        run_id=run_id
    )

    blob.error = ServiceUnavailable(  # type: ignore[no-untyped-call]
        "storage unavailable"
    )

    repository = (
        GcsIngestionRunRepository(
            bucket_name=(
                "riverwatch-dev-lake"
            ),
            client=FakeStorageClient(
                blobs=[blob]
            ),
        )
    )

    with pytest.raises(
        BipadEventRunnerError,
        match="Unable to load ingestion manifest",
    ):
        repository.reference_for_run_id(
            run_id=run_id,
        )


def test_rejects_blank_bucket_name() -> None:
    with pytest.raises(
        ValueError,
        match="bucket_name cannot be empty",
    ):
        GcsIngestionRunRepository(
            bucket_name="   ",
            client=FakeStorageClient(),
        )