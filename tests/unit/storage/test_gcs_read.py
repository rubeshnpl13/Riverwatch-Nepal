from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from google.api_core.exceptions import (
    NotFound,
    ServiceUnavailable,
)

from riverwatch.storage.errors import (
    ObjectNotFoundError,
    StorageError,
)
from riverwatch.storage.gcs import (
    GcsObjectStore,
)


@dataclass
class FakeBlob:
    name: str
    metadata: dict[str, str] | None = None
    data: bytes = b""
    download_error: Exception | None = None

    def upload_from_string(
        self,
        data: bytes,
        *,
        content_type: str,
        if_generation_match: int,
    ) -> None:
        _ = (
            data,
            content_type,
            if_generation_match,
        )

    def download_as_bytes(
        self,
    ) -> bytes:
        if self.download_error is not None:
            raise self.download_error

        return self.data


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

    def bucket(
        self,
        bucket_name: str,
    ) -> FakeBucket:
        _ = bucket_name

        return self.bucket_instance


def test_read_bytes_downloads_object(
) -> None:
    client = FakeStorageClient()

    blob = (
        client
        .bucket_instance
        .blob(
            "manifests/run/manifest.json"
        )
    )

    blob.data = b'{"status":"completed"}'

    store = GcsObjectStore(
        bucket_name=(
            "riverwatch-dev-lake"
        ),
        client=client,
    )

    assert store.read_bytes(
        key=(
            "manifests/run/"
            "manifest.json"
        )
    ) == b'{"status":"completed"}'


def test_read_bytes_maps_not_found(
) -> None:
    client = FakeStorageClient()

    blob = (
        client
        .bucket_instance
        .blob(
            "manifests/missing.json"
        )
    )

    blob.download_error = NotFound(  # type: ignore[no-untyped-call]
        "missing"
    )

    store = GcsObjectStore(
        bucket_name=(
            "riverwatch-dev-lake"
        ),
        client=client,
    )

    with pytest.raises(
        ObjectNotFoundError,
        match="does not exist",
    ):
        store.read_bytes(
            key=(
                "manifests/"
                "missing.json"
            )
        )


def test_read_bytes_maps_gcs_failure(
) -> None:
    client = FakeStorageClient()

    blob = (
        client
        .bucket_instance
        .blob(
            "manifests/failure.json"
        )
    )

    blob.download_error = (
        ServiceUnavailable(  # type: ignore[no-untyped-call]
            "unavailable"
        )
    )

    store = GcsObjectStore(
        bucket_name=(
            "riverwatch-dev-lake"
        ),
        client=client,
    )

    with pytest.raises(
        StorageError,
        match="Unable to read object",
    ):
        store.read_bytes(
            key=(
                "manifests/"
                "failure.json"
            )
        )