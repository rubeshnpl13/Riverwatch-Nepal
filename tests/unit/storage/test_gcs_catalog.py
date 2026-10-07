from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from google.api_core.exceptions import (
    ServiceUnavailable,
)

from riverwatch.storage.errors import (
    StorageError,
)
from riverwatch.storage.gcs import (
    GcsObjectStore,
)


@dataclass
class FakeBlob:
    name: str
    present: bool = False
    exists_error: Exception | None = None
    metadata: dict[str, str] | None = None

    def exists(
        self,
    ) -> bool:
        if self.exists_error is not None:
            raise self.exists_error

        return self.present

    def upload_from_string(
        self,
        data: bytes,
        *,
        content_type: str,
        if_generation_match: int,
    ) -> None:
        _ = data
        _ = content_type
        _ = if_generation_match

    def download_as_bytes(
        self,
    ) -> bytes:
        return b""


@dataclass
class FakeBucket:
    blobs: dict[
        str,
        FakeBlob,
    ] = field(
        default_factory=dict
    )

    def blob(
        self,
        blob_name: str,
    ) -> FakeBlob:
        if blob_name not in self.blobs:
            self.blobs[
                blob_name
            ] = FakeBlob(
                name=blob_name
            )

        return self.blobs[
            blob_name
        ]


class FakeStorageClient:
    def __init__(
        self,
    ) -> None:
        self.bucket_instance = (
            FakeBucket()
        )

        self.list_error: (
            Exception | None
        ) = None

    def bucket(
        self,
        bucket_name: str,
    ) -> FakeBucket:
        _ = bucket_name

        return self.bucket_instance

    def list_blobs(
        self,
        bucket_or_name: str,
        *,
        prefix: str,
        max_results: int,
    ) -> list[FakeBlob]:
        _ = bucket_or_name

        if self.list_error is not None:
            raise self.list_error

        matches = [
            blob
            for name, blob
            in (
                self.bucket_instance
                .blobs.items()
            )
            if (
                blob.present
                and name.startswith(
                    prefix
                )
            )
        ]

        return matches[
            :max_results
        ]


def test_object_exists_returns_true(
) -> None:
    client = FakeStorageClient()

    blob = (
        client
        .bucket_instance
        .blob(
            "processed/run/_SUCCESS"
        )
    )

    blob.present = True

    store = GcsObjectStore(
        bucket_name="lake",
        client=client,
    )

    assert store.object_exists(
        key="processed/run/_SUCCESS"
    )


def test_object_exists_returns_false(
) -> None:
    store = GcsObjectStore(
        bucket_name="lake",
        client=FakeStorageClient(),
    )

    assert not store.object_exists(
        key="processed/run/_SUCCESS"
    )


def test_prefix_exists_returns_true(
) -> None:
    client = FakeStorageClient()

    blob = (
        client
        .bucket_instance
        .blob(
            "processed/run/"
            "part-00000.parquet"
        )
    )

    blob.present = True

    store = GcsObjectStore(
        bucket_name="lake",
        client=client,
    )

    assert store.prefix_exists(
        prefix="processed/run"
    )


def test_prefix_exists_returns_false(
) -> None:
    store = GcsObjectStore(
        bucket_name="lake",
        client=FakeStorageClient(),
    )

    assert not store.prefix_exists(
        prefix="processed/run"
    )


def test_object_exists_maps_failure(
) -> None:
    client = FakeStorageClient()

    blob = (
        client
        .bucket_instance
        .blob(
            "processed/run/_SUCCESS"
        )
    )

    blob.exists_error = (
        ServiceUnavailable(  # type: ignore[no-untyped-call]
            "unavailable"
        )
    )

    store = GcsObjectStore(
        bucket_name="lake",
        client=client,
    )

    with pytest.raises(
        StorageError,
        match="Unable to inspect object",
    ):
        store.object_exists(
            key=(
                "processed/run/"
                "_SUCCESS"
            )
        )


def test_prefix_exists_maps_failure(
) -> None:
    client = FakeStorageClient()

    client.list_error = (
        ServiceUnavailable(  # type: ignore[no-untyped-call]
            "unavailable"
        )
    )

    store = GcsObjectStore(
        bucket_name="lake",
        client=client,
    )

    with pytest.raises(
        StorageError,
        match=(
            "Unable to inspect "
            "object prefix"
        ),
    ):
        store.prefix_exists(
            prefix="processed/run"
        )