from __future__ import annotations

from collections.abc import (
    Iterable,
    Mapping,
)
from typing import (
    Protocol,
    cast,
)

import google.cloud.storage as storage  # type: ignore[import-untyped]
from google.api_core.exceptions import (
    GoogleAPIError,
    NotFound,
    PreconditionFailed,
)

from riverwatch.storage.errors import (
    InvalidObjectKeyError,
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
    StorageError,
)
from riverwatch.storage.models import StoredObject
from riverwatch.storage.serialization import sha256_hex


class _Blob(Protocol):
    metadata: dict[str, str] | None

    def upload_from_string(
        self,
        data: bytes,
        *,
        content_type: str,
        if_generation_match: int,
    ) -> None:
        ...

    def download_as_bytes(
        self,
    ) -> bytes:
        ...


class _Bucket(Protocol):
    def blob(
        self,
        blob_name: str,
    ) -> _Blob:
        ...


class _StorageClient(Protocol):
    def bucket(
        self,
        bucket_name: str,
    ) -> _Bucket:
        ...

class _ExistingBlob(
    Protocol,
):
    def exists(
        self,
    ) -> bool:
        ...


class _ListingStorageClient(
    Protocol,
):
    def list_blobs(
        self,
        bucket_or_name: str,
        *,
        prefix: str,
        max_results: int,
    ) -> Iterable[object]:
        ...

def _validate_object_key(
    key: str,
) -> str:
    cleaned = key.strip()

    if not cleaned:
        raise InvalidObjectKeyError(
            "Object key cannot be empty"
        )

    if cleaned != key:
        raise InvalidObjectKeyError(
            "Object key cannot have leading "
            "or trailing whitespace"
        )

    if "\\" in cleaned:
        raise InvalidObjectKeyError(
            "Object key cannot contain '\\'"
        )

    if cleaned.startswith("/"):
        raise InvalidObjectKeyError(
            "Object key cannot be absolute"
        )

    parts = cleaned.split("/")

    if any(
        part in {
            "",
            ".",
            "..",
        }
        for part in parts
    ):
        raise InvalidObjectKeyError(
            f"Invalid object key: {key}"
        )

    return cleaned


class GcsObjectStore:
    """Create-only object store backed by Google Cloud Storage."""

    def __init__(
        self,
        *,
        bucket_name: str,
        client: _StorageClient | None = None,
    ) -> None:
        cleaned_bucket_name = bucket_name.strip()

        if not cleaned_bucket_name:
            raise ValueError(
                "bucket_name cannot be empty"
            )

        self._bucket_name = cleaned_bucket_name

        if client is None:
            self._client: _StorageClient = (
                storage.Client()
            )
        else:
            self._client = client

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def read_bytes(
        self,
        *,
        key: str,
    ) -> bytes:
        normalized_key = (
            _validate_object_key(
                key
            )
        )

        try:
            bucket = self._client.bucket(
                self._bucket_name
            )

            blob = bucket.blob(
                normalized_key
            )

            return (
                blob.download_as_bytes()
            )

        except NotFound as exc:
            raise ObjectNotFoundError(
                "Object does not exist: "
                f"{normalized_key}"
            ) from exc

        except GoogleAPIError as exc:
            raise StorageError(
                "Unable to read object: "
                f"{normalized_key}"
            ) from exc

    def object_exists(
        self,
        *,
        key: str,
    ) -> bool:
        normalized_key = (
            _validate_object_key(
                key
            )
        )

        try:
            bucket = self._client.bucket(
                self._bucket_name
            )

            blob = bucket.blob(
                normalized_key
            )

            existing_blob = cast(
                _ExistingBlob,
                blob,
            )

            return existing_blob.exists()

        except GoogleAPIError as exc:
            raise StorageError(
                "Unable to inspect object: "
                f"{normalized_key}"
            ) from exc

    def prefix_exists(
            self,
            *,
            prefix: str,
    ) -> bool:
        normalized_prefix = (
            _validate_object_key(
                prefix
            )
        )
        listing_client = cast(
            _ListingStorageClient,
            self._client,
        )

        try:
            blobs = listing_client.list_blobs(
                self._bucket_name,
                prefix=(
                    f"{normalized_prefix}/"
                ),
                max_results=1,
            )

            return (
                    next(
                        iter(blobs),
                        None,
                    )
                    is not None
            )

        except GoogleAPIError as exc:
            raise StorageError(
                "Unable to inspect object "
                "prefix: "
                f"{normalized_prefix}"
            ) from exc

    def create_bytes(
        self,
        *,
        key: str,
        data: bytes,
        content_type: str,
        metadata: Mapping[str, str] | None = None,
    ) -> StoredObject:
        normalized_key = _validate_object_key(
            key
        )

        if not content_type.strip():
            raise ValueError(
                "content_type cannot be empty"
            )

        try:
            bucket = self._client.bucket(
                self._bucket_name
            )

            blob = bucket.blob(
                normalized_key
            )

            if metadata is not None:
                blob.metadata = dict(
                    metadata
                )

            blob.upload_from_string(
                data,
                content_type=content_type,
                if_generation_match=0,
            )

        except PreconditionFailed as exc:
            raise ObjectAlreadyExistsError(
                f"Object already exists: "
                f"{normalized_key}"
            ) from exc

        except GoogleAPIError as exc:
            raise StorageError(
                f"Unable to create object: "
                f"{normalized_key}"
            ) from exc

        return StoredObject(
            key=normalized_key,
            size_bytes=len(data),
            content_type=content_type,
            sha256=sha256_hex(
                data
            ),
        )