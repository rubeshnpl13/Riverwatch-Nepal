import json
from datetime import UTC, datetime
from pathlib import Path

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.quarantine_writer import (
    BipadQuarantineWriter,
)
from riverwatch.ingestion.bipad.validation import (
    QuarantinedRecord,
    ValidationIssue,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)


def test_writes_quarantined_record(
    tmp_path: Path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    writer = BipadQuarantineWriter(
        store=store
    )

    record = QuarantinedRecord(
        endpoint=BipadEndpoint.RIVER,
        record_index=1042,
        raw_record={
            "id": 123,
            "waterLevel": "invalid",
        },
        issues=[
            ValidationIssue(
                error_type="float_parsing",
                location=[
                    "waterLevel"
                ],
                message=(
                    "Input should be a valid number"
                ),
            )
        ],
    )

    captured_at = datetime(
        2026,
        9,
        29,
        3,
        0,
        tzinfo=UTC,
    )

    result = writer.write_record(
        record=record,
        run_id="test-run",
        captured_at=captured_at,
        page_index=1,
    )

    path = (
        tmp_path
        / result.object.key
    )

    assert path.exists()

    document = json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )

    assert (
        document["provider"]
        == "bipad"
    )

    assert (
        document["endpoint"]
        == "river"
    )

    assert (
        document["page_index"]
        == 1
    )

    assert (
        document["record_index"]
        == 1042
    )

    assert (
        document["raw_record"]["id"]
        == 123
    )

    assert (
        document["issues"][0][
            "error_type"
        ]
        == "float_parsing"
    )