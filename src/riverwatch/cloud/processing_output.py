from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from riverwatch.cloud.processing_input import (
    gcs_uri,
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
from riverwatch.storage.paths import (
    build_processed_run_prefix,
)


class CloudProcessingOutputError(
    RuntimeError,
):
    """Cloud processed-output state is unsafe."""


class GcsProcessedOutputState(
    StrEnum,
):
    ABSENT = "absent"
    INCOMPLETE = "incomplete"
    COMPLETE = "complete"


class ProcessingObjectCatalog(
    Protocol,
):
    @property
    def bucket_name(
        self,
    ) -> str:
        ...

    def object_exists(
        self,
        *,
        key: str,
    ) -> bool:
        ...

    def prefix_exists(
        self,
        *,
        prefix: str,
    ) -> bool:
        ...


class ProcessingCloudStore(
    ProcessingObjectCatalog,
    Protocol,
):
    def read_bytes(
        self,
        *,
        key: str,
    ) -> bytes:
        ...

    def create_bytes(
        self,
        *,
        key: str,
        data: bytes,
        content_type: str,
    ) -> StoredObject:
        ...


class _FrameWriter(
    Protocol,
):
    def mode(
        self,
        save_mode: str,
    ) -> _FrameWriter:
        ...

    def option(
        self,
        key: str,
        value: str,
    ) -> _FrameWriter:
        ...

    def parquet(
        self,
        path: str,
    ) -> None:
        ...


class _WritableFrame(
    Protocol,
):
    @property
    def write(
        self,
    ) -> _FrameWriter:
        ...


class _ReadableFrame(
    Protocol,
):
    def count(
        self,
    ) -> int:
        ...


class _FrameReader(
    Protocol,
):
    def parquet(
        self,
        path: str,
    ) -> _ReadableFrame:
        ...


class ProcessingSparkSession(
    Protocol,
):
    @property
    def read(
        self,
    ) -> _FrameReader:
        ...


@dataclass(
    frozen=True,
    slots=True,
)
class GcsProcessedOutputTarget:
    dataset: ProcessedDataset
    key: str
    uri: str
    success_marker_key: str


@dataclass(
    frozen=True,
    slots=True,
)
class CloudProcessedOutput:
    dataset: ProcessedDataset
    uri: str
    row_count: int


def build_gcs_processed_output_target(
    *,
    bucket_name: str,
    dataset: ProcessedDataset,
    processing_input: ProcessingInputRun,
) -> GcsProcessedOutputTarget:
    key = build_processed_run_prefix(
        dataset=dataset,
        endpoint=processing_input.endpoint,
        captured_at=(
            processing_input.captured_at
        ),
        run_id=processing_input.run_id,
    )

    normalized_key = key.rstrip("/")

    return GcsProcessedOutputTarget(
        dataset=dataset,
        key=normalized_key,
        uri=gcs_uri(
            bucket_name=bucket_name,
            key=normalized_key,
        ),
        success_marker_key=(
            f"{normalized_key}/_SUCCESS"
        ),
    )


def inspect_gcs_processed_output(
    *,
    store: ProcessingObjectCatalog,
    target: GcsProcessedOutputTarget,
) -> GcsProcessedOutputState:
    try:
        if store.object_exists(
            key=target.success_marker_key
        ):
            return (
                GcsProcessedOutputState
                .COMPLETE
            )

        if store.prefix_exists(
            prefix=target.key
        ):
            return (
                GcsProcessedOutputState
                .INCOMPLETE
            )

        return (
            GcsProcessedOutputState
            .ABSENT
        )

    except StorageError as exc:
        raise CloudProcessingOutputError(
            "Unable to inspect processed "
            "output state: "
            f"{target.uri}"
        ) from exc


def write_or_recover_gcs_processed_output(
    *,
    spark: ProcessingSparkSession,
    dataframe: _WritableFrame,
    store: ProcessingObjectCatalog,
    target: GcsProcessedOutputTarget,
) -> CloudProcessedOutput:
    state = inspect_gcs_processed_output(
        store=store,
        target=target,
    )

    if (
        state
        is GcsProcessedOutputState.INCOMPLETE
    ):
        raise CloudProcessingOutputError(
            "Processed output exists "
            "without a Spark success marker: "
            f"{target.uri}"
        )

    if (
        state
        is GcsProcessedOutputState.ABSENT
    ):
        (
            dataframe.write
            .mode("errorifexists")
            .option(
                "compression",
                "snappy",
            )
            .parquet(
                target.uri
            )
        )

        state = (
            inspect_gcs_processed_output(
                store=store,
                target=target,
            )
        )

        if (
            state
            is not
            GcsProcessedOutputState.COMPLETE
        ):
            raise CloudProcessingOutputError(
                "Spark write did not produce "
                "a complete processed output: "
                f"{target.uri}"
            )

    row_count = (
        spark.read
        .parquet(
            target.uri
        )
        .count()
    )

    return CloudProcessedOutput(
        dataset=target.dataset,
        uri=target.uri,
        row_count=row_count,
    )


def create_or_verify_gcs_object(
    *,
    store: ProcessingCloudStore,
    key: str,
    data: bytes,
    content_type: str,
) -> str:
    uri = gcs_uri(
        bucket_name=store.bucket_name,
        key=key,
    )

    try:
        exists = store.object_exists(
            key=key
        )

    except StorageError as exc:
        raise CloudProcessingOutputError(
            "Unable to inspect cloud object: "
            f"{uri}"
        ) from exc

    if exists:
        _verify_existing_object(
            store=store,
            key=key,
            expected_data=data,
            uri=uri,
        )

        return uri

    try:
        store.create_bytes(
            key=key,
            data=data,
            content_type=content_type,
        )

    except ObjectAlreadyExistsError:
        # Another at-least-once execution may have
        # created the same deterministic object
        # between the existence check and create.
        _verify_existing_object(
            store=store,
            key=key,
            expected_data=data,
            uri=uri,
        )

    except StorageError as exc:
        raise CloudProcessingOutputError(
            "Unable to create cloud object: "
            f"{uri}"
        ) from exc

    return uri


def _verify_existing_object(
    *,
    store: ProcessingCloudStore,
    key: str,
    expected_data: bytes,
    uri: str,
) -> None:
    try:
        existing_data = (
            store.read_bytes(
                key=key
            )
        )

    except StorageError as exc:
        raise CloudProcessingOutputError(
            "Unable to read existing "
            "cloud object: "
            f"{uri}"
        ) from exc

    if existing_data != expected_data:
        raise CloudProcessingOutputError(
            "Existing cloud object does "
            "not match recomputed content: "
            f"{uri}"
        )