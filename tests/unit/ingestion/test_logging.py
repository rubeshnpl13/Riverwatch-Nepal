import json
import logging
from datetime import UTC, datetime
from io import StringIO

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.validation import (
    QuarantinedRecord,
    ValidationIssue,
)
from riverwatch.ingestion.logging import (
    log_ingestion_completed,
    log_quarantined_record,
)
from riverwatch.ingestion.metrics import (
    IngestionMetrics,
    IngestionRunResult,
)
from riverwatch.observability.logging import (
    configure_logging,
)


def test_logs_completed_ingestion_run() -> None:
    output = StringIO()

    configure_logging(
        stream=output
    )

    logger = logging.getLogger(
        "riverwatch.ingestion"
    )

    started_at = datetime(
        2026,
        9,
        28,
        12,
        0,
        tzinfo=UTC,
    )

    finished_at = datetime(
        2026,
        9,
        28,
        12,
        0,
        1,
        tzinfo=UTC,
    )

    quarantined_record = QuarantinedRecord(
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
        record_index=4,
        raw_record={
            "id": "invalid-record",
        },
        issues=[
            ValidationIssue(
                error_type="missing",
                location=[
                    "point",
                ],
                message="Field required",
            )
        ],
    )

    result = IngestionRunResult(
        run_id="run-123",
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=250.0,
        metrics=IngestionMetrics(
            records_received=10,
            records_valid=9,
            records_invalid=1,
            stations_emitted=9,
            observations_emitted=8,
            records_without_observation=1,
        ),
        quarantined_records=(
            quarantined_record,
        ),
    )

    log_ingestion_completed(
        logger,
        result,
    )

    payload = json.loads(
        output.getvalue()
    )

    assert (
        payload["event"]
        == "bipad_ingestion_completed"
    )

    assert (
        payload["run_id"]
        == "run-123"
    )

    assert (
        payload["endpoint"]
        == "river-stations"
    )

    assert (
        payload["records_received"]
        == 10
    )

    assert (
        payload["records_valid"]
        == 9
    )

    assert (
        payload["records_invalid"]
        == 1
    )

    assert (
        payload["stations_emitted"]
        == 9
    )

    assert (
        payload["observations_emitted"]
        == 8
    )

    assert (
        payload["records_without_observation"]
        == 1
    )

    assert (
        payload["duration_ms"]
        == 250.0
    )

    assert (
        payload["started_at"]
        == started_at.isoformat()
    )

    assert (
        payload["finished_at"]
        == finished_at.isoformat()
    )


def test_logs_quarantined_record_without_raw_payload() -> None:
    output = StringIO()

    configure_logging(
        stream=output
    )

    logger = logging.getLogger(
        "riverwatch.ingestion"
    )

    quarantine = QuarantinedRecord(
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        ),
        record_index=17,
        raw_record={
            "secret_big_raw_payload": (
                "do-not-log-this"
            )
        },
        issues=[
            ValidationIssue(
                error_type="missing",
                location=[
                    "point",
                ],
                message="Field required",
            )
        ],
    )

    log_quarantined_record(
        logger,
        run_id="run-123",
        record=quarantine,
    )

    payload = json.loads(
        output.getvalue()
    )

    assert (
        payload["event"]
        == "bipad_record_quarantined"
    )

    assert (
        payload["level"]
        == "WARNING"
    )

    assert (
        payload["run_id"]
        == "run-123"
    )

    assert (
        payload["endpoint"]
        == "river-stations"
    )

    assert (
        payload["record_index"]
        == 17
    )

    assert (
        payload["validation_issues"][0][
            "location"
        ]
        == ["point"]
    )

    assert (
        payload["validation_issues"][0][
            "error_type"
        ]
        == "missing"
    )

    assert (
        "raw_record"
        not in payload
    )

    assert (
        "secret_big_raw_payload"
        not in output.getvalue()
    )