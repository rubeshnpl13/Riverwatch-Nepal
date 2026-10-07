from __future__ import annotations

from datetime import (
    UTC,
    datetime,
)

import pytest

from riverwatch.cloud.processing_output import (
    CloudProcessedOutput,
    CloudProcessingOutputError,
    GcsProcessedOutputState,
    GcsProcessedOutputTarget,
    build_gcs_processed_output_target,
    create_or_verify_gcs_object,
    inspect_gcs_processed_output,
    write_or_recover_gcs_processed_output,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.models import (
    ProcessedDataset,
    ProcessingInputRun,
)
from riverwatch.storage.errors import (
    ObjectAlreadyExistsError,
    StorageError,
)
from riverwatch.storage.models import (
    StoredObject,
)
from riverwatch.storage.serialization import (
    sha256_hex,
)


class FakeCloudStore:
    def __init__(
        self,
    ) -> None:
        self.bucket_name = (
            "riverwatch-dev-lake"
        )

        self.objects: set[str] = set()

        self.object_data: dict[
            str,
            bytes,
        ] = {}

        self.failure: (
            StorageError | None
        ) = None

    def object_exists(
        self,
        *,
        key: str,
    ) -> bool:
        if self.failure is not None:
            raise self.failure

        return (
            key in self.objects
            or key in self.object_data
        )

    def prefix_exists(
        self,
        *,
        prefix: str,
    ) -> bool:
        if self.failure is not None:
            raise self.failure

        normalized = (
            f"{prefix.rstrip('/')}/"
        )

        keys = (
            self.objects
            | set(
                self.object_data
            )
        )

        return any(
            key.startswith(
                normalized
            )
            for key in keys
        )

    def read_bytes(
        self,
        *,
        key: str,
    ) -> bytes:
        if self.failure is not None:
            raise self.failure

        try:
            return self.object_data[
                key
            ]

        except KeyError as exc:
            raise StorageError(
                "missing object"
            ) from exc

    def create_bytes(
        self,
        *,
        key: str,
        data: bytes,
        content_type: str,
    ) -> StoredObject:
        if self.failure is not None:
            raise self.failure

        if (
            key in self.objects
            or key in self.object_data
        ):
            raise ObjectAlreadyExistsError(
                f"Object already exists: {key}"
            )

        self.object_data[
            key
        ] = data

        return StoredObject(
            key=key,
            size_bytes=len(data),
            content_type=content_type,
            sha256=sha256_hex(
                data
            ),
        )


class FakeFrameWriter:
    def __init__(
        self,
        *,
        store: FakeCloudStore,
        target_key: str,
        complete: bool,
    ) -> None:
        self._store = store
        self._target_key = target_key
        self._complete = complete

        self.mode_value: (
            str | None
        ) = None

        self.options: dict[
            str,
            str,
        ] = {}

        self.paths: list[str] = []

    def mode(
        self,
        save_mode: str,
    ) -> FakeFrameWriter:
        self.mode_value = save_mode

        return self

    def option(
        self,
        key: str,
        value: str,
    ) -> FakeFrameWriter:
        self.options[
            key
        ] = value

        return self

    def parquet(
        self,
        path: str,
    ) -> None:
        self.paths.append(
            path
        )

        self._store.objects.add(
            f"{self._target_key}/"
            "part-00000.parquet"
        )

        if self._complete:
            self._store.objects.add(
                f"{self._target_key}/"
                "_SUCCESS"
            )


class FakeWritableFrame:
    def __init__(
        self,
        *,
        writer: FakeFrameWriter,
    ) -> None:
        self._writer = writer

    @property
    def write(
        self,
    ) -> FakeFrameWriter:
        return self._writer


class FakeReadableFrame:
    def __init__(
        self,
        *,
        row_count: int,
    ) -> None:
        self._row_count = (
            row_count
        )

    def count(
        self,
    ) -> int:
        return self._row_count


class FakeFrameReader:
    def __init__(
        self,
        *,
        row_count: int,
    ) -> None:
        self._row_count = (
            row_count
        )

        self.paths: list[str] = []

    def parquet(
        self,
        path: str,
    ) -> FakeReadableFrame:
        self.paths.append(
            path
        )

        return FakeReadableFrame(
            row_count=(
                self._row_count
            )
        )


class FakeSpark:
    def __init__(
        self,
        *,
        row_count: int,
    ) -> None:
        self._reader = (
            FakeFrameReader(
                row_count=row_count
            )
        )

    @property
    def read(
        self,
    ) -> FakeFrameReader:
        return self._reader


def processing_input(
) -> ProcessingInputRun:
    return ProcessingInputRun(
        run_id="run-123",
        endpoint=(
            BipadEndpoint
            .RIVER_STATIONS
        ),
        captured_at=datetime(
            2026,
            10,
            5,
            12,
            0,
            tzinfo=UTC,
        ),
        manifest_path=(
            "gs://riverwatch-dev-lake/"
            "manifests/run-123/"
            "manifest.json"
        ),
        pages=(),
    )

def build_target(
    *,
    store: FakeCloudStore,
    dataset: ProcessedDataset = (
        ProcessedDataset.OBSERVATIONS
    ),
) -> GcsProcessedOutputTarget:
    return (
        build_gcs_processed_output_target(
            bucket_name=store.bucket_name,
            dataset=dataset,
            processing_input=(
                processing_input()
            ),
        )
    )



def test_builds_deterministic_target(
) -> None:
    store = FakeCloudStore()

    target = build_target(
        store=store,
        dataset=(
            ProcessedDataset.STATIONS
        ),
    )

    assert target.dataset is (
        ProcessedDataset.STATIONS
    )

    assert target.key.startswith(
        "processed/dataset=stations/"
    )

    assert (
        "endpoint=river-stations"
        in target.key
    )

    assert (
        "run-run-123"
        in target.key
    )

    assert target.uri == (
        "gs://riverwatch-dev-lake/"
        f"{target.key}"
    )

    assert (
        target.success_marker_key
        == f"{target.key}/_SUCCESS"
    )


def test_output_state_is_absent(
) -> None:
    store = FakeCloudStore()

    target = build_target(
        store=store
    )

    assert (
        inspect_gcs_processed_output(
            store=store,
            target=target,
        )
        is GcsProcessedOutputState.ABSENT
    )


def test_output_state_is_incomplete(
) -> None:
    store = FakeCloudStore()

    target = build_target(
        store=store
    )

    store.objects.add(
        f"{target.key}/"
        "part-00000.parquet"
    )

    assert (
        inspect_gcs_processed_output(
            store=store,
            target=target,
        )
        is (
            GcsProcessedOutputState
            .INCOMPLETE
        )
    )


def test_output_state_is_complete(
) -> None:
    store = FakeCloudStore()

    target = build_target(
        store=store
    )

    store.objects.add(
        target.success_marker_key
    )

    assert (
        inspect_gcs_processed_output(
            store=store,
            target=target,
        )
        is (
            GcsProcessedOutputState
            .COMPLETE
        )
    )


def test_state_failure_is_mapped(
) -> None:
    store = FakeCloudStore()

    store.failure = StorageError(
        "unavailable"
    )

    target = build_target(
        store=store
    )

    with pytest.raises(
        CloudProcessingOutputError,
        match=(
            "Unable to inspect "
            "processed output state"
        ),
    ):
        inspect_gcs_processed_output(
            store=store,
            target=target,
        )


def test_writes_absent_processed_output(
) -> None:
    store = FakeCloudStore()

    target = build_target(
        store=store
    )

    writer = FakeFrameWriter(
        store=store,
        target_key=target.key,
        complete=True,
    )

    dataframe = FakeWritableFrame(
        writer=writer
    )

    spark = FakeSpark(
        row_count=7
    )

    result = (
        write_or_recover_gcs_processed_output(
            spark=spark,
            dataframe=dataframe,
            store=store,
            target=target,
        )
    )

    assert result == CloudProcessedOutput(
        dataset=(
            ProcessedDataset.OBSERVATIONS
        ),
        uri=target.uri,
        row_count=7,
    )

    assert writer.mode_value == (
        "errorifexists"
    )

    assert writer.options == {
        "compression": "snappy",
    }

    assert writer.paths == [
        target.uri
    ]

    assert (
        target.success_marker_key
        in store.objects
    )


def test_recovers_complete_processed_output(
) -> None:
    store = FakeCloudStore()

    target = build_target(
        store=store
    )

    store.objects.add(
        target.success_marker_key
    )

    writer = FakeFrameWriter(
        store=store,
        target_key=target.key,
        complete=True,
    )

    spark = FakeSpark(
        row_count=4
    )

    result = (
        write_or_recover_gcs_processed_output(
            spark=spark,
            dataframe=FakeWritableFrame(
                writer=writer
            ),
            store=store,
            target=target,
        )
    )

    assert result.row_count == 4

    assert writer.paths == []

    assert spark.read.paths == [
        target.uri
    ]


def test_rejects_incomplete_processed_output(
) -> None:
    store = FakeCloudStore()

    target = build_target(
        store=store
    )

    store.objects.add(
        f"{target.key}/"
        "part-00000.parquet"
    )

    writer = FakeFrameWriter(
        store=store,
        target_key=target.key,
        complete=True,
    )

    with pytest.raises(
        CloudProcessingOutputError,
        match="without a Spark success marker",
    ):
        write_or_recover_gcs_processed_output(
            spark=FakeSpark(
                row_count=1
            ),
            dataframe=FakeWritableFrame(
                writer=writer
            ),
            store=store,
            target=target,
        )

    assert writer.paths == []


def test_rejects_write_without_success_marker(
) -> None:
    store = FakeCloudStore()

    target = build_target(
        store=store
    )

    writer = FakeFrameWriter(
        store=store,
        target_key=target.key,
        complete=False,
    )

    with pytest.raises(
        CloudProcessingOutputError,
        match=(
            "did not produce a complete "
            "processed output"
        ),
    ):
        write_or_recover_gcs_processed_output(
            spark=FakeSpark(
                row_count=1
            ),
            dataframe=FakeWritableFrame(
                writer=writer
            ),
            store=store,
            target=target,
        )


def test_creates_immutable_cloud_object(
) -> None:
    store = FakeCloudStore()

    data = b'{"status":"ok"}'

    uri = create_or_verify_gcs_object(
        store=store,
        key="quality/run/report.json",
        data=data,
        content_type="application/json",
    )

    assert uri == (
        "gs://riverwatch-dev-lake/"
        "quality/run/report.json"
    )

    assert store.object_data[
        "quality/run/report.json"
    ] == data


def test_recovers_matching_cloud_object(
) -> None:
    store = FakeCloudStore()

    key = "quality/run/report.json"

    data = b'{"status":"ok"}'

    store.object_data[
        key
    ] = data

    uri = create_or_verify_gcs_object(
        store=store,
        key=key,
        data=data,
        content_type="application/json",
    )

    assert uri == (
        f"gs://{store.bucket_name}/"
        f"{key}"
    )


def test_rejects_conflicting_cloud_object(
) -> None:
    store = FakeCloudStore()

    key = "quality/run/report.json"

    store.object_data[
        key
    ] = b'{"status":"different"}'

    with pytest.raises(
        CloudProcessingOutputError,
        match=(
            "does not match recomputed "
            "content"
        ),
    ):
        create_or_verify_gcs_object(
            store=store,
            key=key,
            data=b'{"status":"ok"}',
            content_type=(
                "application/json"
            ),
        )