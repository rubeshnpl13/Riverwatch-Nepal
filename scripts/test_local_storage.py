from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)
from riverwatch.storage.paths import (
    build_raw_metadata_key,
    build_raw_payload_key,
)
from riverwatch.storage.serialization import (
    serialize_json,
)


def main() -> None:
    store = LocalObjectStore(
        root=Path(
            "data/lake"
        )
    )

    run_id = str(
        uuid4()
    )

    captured_at = datetime.now(
        UTC
    )

    payload_key = build_raw_payload_key(
        provider=DataProvider.BIPAD,
        endpoint=BipadEndpoint.RIVER_STATIONS,
        captured_at=captured_at,
        run_id=run_id,
        page_index=0,
    )

    metadata_key = build_raw_metadata_key(
        provider=DataProvider.BIPAD,
        endpoint=BipadEndpoint.RIVER_STATIONS,
        captured_at=captured_at,
        run_id=run_id,
        page_index=0,
    )

    payload = {
        "count": 2,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": 282,
                "title": "Example Station",
            },
            {
                "id": 44,
                "title": "Example River",
            },
        ],
    }

    payload_bytes = serialize_json(
        payload
    )

    payload_object = store.create_bytes(
        key=payload_key,
        data=payload_bytes,
        content_type="application/json",
    )

    metadata = {
        "provider": (
            DataProvider.BIPAD.value
        ),
        "endpoint": (
            BipadEndpoint
            .RIVER_STATIONS
            .value
        ),
        "run_id": run_id,
        "page_index": 0,
        "captured_at": captured_at,
        "payload_key": (
            payload_object.key
        ),
        "payload_size_bytes": (
            payload_object.size_bytes
        ),
        "payload_sha256": (
            payload_object.sha256
        ),
    }

    metadata_object = store.create_bytes(
        key=metadata_key,
        data=serialize_json(
            metadata
        ),
        content_type="application/json",
    )

    print(
        "Created payload:"
    )
    print(
        payload_object.key
    )

    print()

    print(
        "Created metadata:"
    )
    print(
        metadata_object.key
    )

    print()

    print(
        "Payload SHA-256:",
        payload_object.sha256,
    )


if __name__ == "__main__":
    main()