from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.pagination import (
    BipadRawPage,
)
from riverwatch.ingestion.bipad.raw_writer import (
    BipadRawPageWriter,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)


def main() -> None:
    store = LocalObjectStore(
        root=Path(
            "data/lake"
        )
    )

    writer = BipadRawPageWriter(
        store=store
    )

    page = BipadRawPage(
        count=2,
        next=None,
        previous=None,
        results=[
            {
                "id": 282,
                "title": (
                    "Example River Station"
                ),
            },
            {
                "id": 44,
                "title": (
                    "Example Historical Station"
                ),
            },
        ],
    )

    result = writer.write_page(
        page=page,
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
        run_id=str(
            uuid4()
        ),
        captured_at=datetime.now(
            UTC
        ),
        page_index=0,
    )

    print(
        "Payload:"
    )
    print(
        result.payload.key
    )

    print()

    print(
        "Metadata:"
    )
    print(
        result.metadata.key
    )

    print()

    print(
        "Records:",
        result.page_metadata.record_count,
    )

    print(
        "SHA-256:",
        result.page_metadata.payload_sha256,
    )


if __name__ == "__main__":
    main()