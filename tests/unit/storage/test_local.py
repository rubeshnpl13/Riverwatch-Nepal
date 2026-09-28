from pathlib import Path

import pytest

from riverwatch.storage.errors import (
    InvalidObjectKeyError,
    ObjectAlreadyExistsError,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)
from riverwatch.storage.object_store import (
    ObjectStore,
)
from riverwatch.storage.serialization import (
    sha256_hex,
)


def test_creates_object_in_nested_directory(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    key = (
        "raw/"
        "provider=bipad/"
        "endpoint=river/"
        "payload.json"
    )

    data = b'{"hello":"riverwatch"}\n'

    result = store.create_bytes(
        key=key,
        data=data,
        content_type="application/json",
    )

    object_path = (
        tmp_path
        / "raw"
        / "provider=bipad"
        / "endpoint=river"
        / "payload.json"
    )

    assert object_path.exists()

    assert (
        object_path.read_bytes()
        == data
    )

    assert result.key == key

    assert (
        result.size_bytes
        == len(data)
    )

    assert (
        result.content_type
        == "application/json"
    )

    assert (
        result.sha256
        == sha256_hex(data)
    )

#Test create-only behavior
def test_does_not_overwrite_existing_object(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    key = (
        "raw/"
        "provider=bipad/"
        "payload.json"
    )

    store.create_bytes(
        key=key,
        data=b"first",
        content_type="application/json",
    )

    with pytest.raises(
        ObjectAlreadyExistsError,
        match="already exists",
    ):
        store.create_bytes(
            key=key,
            data=b"second",
            content_type="application/json",
        )

    stored = (
        tmp_path
        / "raw"
        / "provider=bipad"
        / "payload.json"
    )

    # Most important part:
    # the original data survived.
    assert (
        stored.read_bytes()
        == b"first"
    )
#Test traversal protection
@pytest.mark.parametrize(
    "key",
    [
        "../outside.json",
        "../../outside.json",
        "/absolute/path.json",
        "raw/../outside.json",
        "raw//payload.json",
        "./payload.json",
        "raw\\payload.json",
        "",
        "   ",
    ],
)
def test_rejects_unsafe_object_keys(
    tmp_path: Path,
    key: str,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    with pytest.raises(
        InvalidObjectKeyError
    ):
        store.create_bytes(
            key=key,
            data=b"test",
            content_type="application/json",
        )
#Test empty content type
def test_rejects_empty_content_type(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    with pytest.raises(
        ValueError,
        match="content_type",
    ):
        store.create_bytes(
            key="raw/payload.json",
            data=b"{}",
            content_type="",
        )

#Test empty file support
def test_can_create_empty_object(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    result = store.create_bytes(
        key="raw/empty.json",
        data=b"",
        content_type="application/json",
    )

    assert result.size_bytes == 0

    assert (
        tmp_path
        .joinpath(
            "raw",
            "empty.json",
        )
        .read_bytes()
        == b""
    )
def accepts_object_store(
    store: ObjectStore,
) -> ObjectStore:
    return store


def test_local_store_satisfies_object_store_protocol(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    assert (
        accepts_object_store(
            store
        )
        is store
    )