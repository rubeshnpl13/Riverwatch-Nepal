import json

from riverwatch.storage.serialization import (
    serialize_json,
    sha256_hex,
)


def test_json_serialization_is_deterministic() -> None:
    first = {
        "b": 2,
        "a": 1,
    }

    second = {
        "a": 1,
        "b": 2,
    }

    assert (
        serialize_json(first)
        == serialize_json(second)
    )


def test_serialized_json_can_be_read_back() -> None:
    payload = {
        "station": 44,
        "waterLevel": 1.5,
    }

    data = serialize_json(
        payload
    )

    decoded = json.loads(
        data.decode(
            "utf-8"
        )
    )

    assert decoded == payload


def test_sha256_is_stable() -> None:
    data = b"riverwatch"

    first = sha256_hex(
        data
    )

    second = sha256_hex(
        data
    )

    assert first == second

    assert len(first) == 64