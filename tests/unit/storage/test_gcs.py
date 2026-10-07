from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256

import pytest
from google.api_core.exceptions import (
    PreconditionFailed,
    ServiceUnavailable,
)

from riverwatch.storage.errors import (
    InvalidObjectKeyError,
    ObjectAlreadyExistsError,
    StorageError,
)
from riverwatch.storage.gcs import (
    GcsObjectStore,
)


@dataclass
class FakeUpload:
    data: bytes
    content_type: str
    if_generation_match: int


@dataclass
class FakeBlob:
    name: str
    metadata: dict[str, str] | None = None
    error: Exception | None = None
    uploads: list[FakeUpload] = field(
        default_factory=list
    )

    def upload_from_string(
        self,
        data: bytes,
        *,
        content_type: str,
        if_generation_match: int,
    ) -> None:
        if self.error is not None:
            raise self.error

        self.uploads.append(
            FakeUpload(
                data=data,
                content_type=content_type,
                if_generation_match=(
                    if_generation_match
                ),
            )
        )


class FakeBucket:
    def __init__(self) -> None:
        self.blobs: dict[
            str,
            FakeBlob,
        ] = {}

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
    def __init__(self) -> None:
        self.bucket_names: list[str] = []
        self.bucket_instance = FakeBucket()

    def bucket(
        self,
        bucket_name: str,
    ) -> FakeBucket:
        self.bucket_names.append(
            bucket_name
        )

        return self.bucket_instance


def test_create_bytes_uploads_create_only_object() -> None:
    client = FakeStorageClient()

    store = GcsObjectStore(
        bucket_name="riverwatch-dev-lake",
        client=client,
    )

    data = b'{"results":[]}'

    result = store.create_bytes(
        key=(
            "raw/provider=bipad/"
            "page=000000/payload.json"
        ),
        data=data,
        content_type="application/json",
        metadata={
            "provider": "bipad",
            "endpoint": "river-stations",
        },
    )

    assert client.bucket_names == [
        "riverwatch-dev-lake"
    ]

    blob = (
        client
        .bucket_instance
        .blobs[
            (
                "raw/provider=bipad/"
                "page=000000/payload.json"
            )
        ]
    )

    assert blob.metadata == {
        "provider": "bipad",
        "endpoint": "river-stations",
    }

    assert len(blob.uploads) == 1

    upload = blob.uploads[0]

    assert upload.data == data
    assert upload.content_type == (
        "application/json"
    )

    assert (
        upload.if_generation_match
        == 0
    )

    assert result.key == (
        "raw/provider=bipad/"
        "page=000000/payload.json"
    )

    assert result.size_bytes == len(
        data
    )

    assert result.content_type == (
        "application/json"
    )

    assert result.sha256 == (
        sha256(data).hexdigest()
    )


def test_create_bytes_rejects_existing_object() -> None:
    client = FakeStorageClient()

    blob = client.bucket_instance.blob(
        "raw/existing.json"
    )

    blob.error = PreconditionFailed(  # type: ignore[no-untyped-call]
        "generation condition failed"
    )

    store = GcsObjectStore(
        bucket_name="riverwatch-dev-lake",
        client=client,
    )

    with pytest.raises(
        ObjectAlreadyExistsError,
        match="Object already exists",
    ):
        store.create_bytes(
            key="raw/existing.json",
            data=b"{}",
            content_type="application/json",
        )


def test_create_bytes_maps_gcs_failure_to_storage_error() -> None:
    client = FakeStorageClient()

    blob = client.bucket_instance.blob(
        "raw/failure.json"
    )

    blob.error = ServiceUnavailable( # type: ignore[no-untyped-call]
        "storage unavailable"
    )

    store = GcsObjectStore(
        bucket_name="riverwatch-dev-lake",
        client=client,
    )

    with pytest.raises(
        StorageError,
        match="Unable to create object",
    ):
        store.create_bytes(
            key="raw/failure.json",
            data=b"{}",
            content_type="application/json",
        )


@pytest.mark.parametrize(
    "key",
    [
        "",
        " raw/object.json",
        "raw/object.json ",
        "/raw/object.json",
        "raw//object.json",
        "raw/./object.json",
        "raw/../object.json",
        r"raw\object.json",
    ],
)
def test_create_bytes_rejects_invalid_object_keys(
    key: str,
) -> None:
    store = GcsObjectStore(
        bucket_name="riverwatch-dev-lake",
        client=FakeStorageClient(),
    )

    with pytest.raises(
        InvalidObjectKeyError
    ):
        store.create_bytes(
            key=key,
            data=b"{}",
            content_type="application/json",
        )


def test_create_bytes_rejects_blank_content_type() -> None:
    store = GcsObjectStore(
        bucket_name="riverwatch-dev-lake",
        client=FakeStorageClient(),
    )

    with pytest.raises(
        ValueError,
        match="content_type cannot be empty",
    ):
        store.create_bytes(
            key="raw/object.json",
            data=b"{}",
            content_type="   ",
        )


def test_rejects_blank_bucket_name() -> None:
    with pytest.raises(
        ValueError,
        match="bucket_name cannot be empty",
    ):
        GcsObjectStore(
            bucket_name="   ",
            client=FakeStorageClient(),
        )

def test_gcs_store_satisfies_object_store_contract() -> None:
    from riverwatch.storage.object_store import (
        ObjectStore,
    )

    store: ObjectStore = GcsObjectStore(
        bucket_name="riverwatch-dev-lake",
        client=FakeStorageClient(),
    )

    result = store.create_bytes(
        key="raw/object.json",
        data=b"{}",
        content_type="application/json",
    )

    assert result.key == "raw/object.json"