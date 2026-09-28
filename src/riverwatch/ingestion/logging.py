import logging

from riverwatch.ingestion.bipad.validation import (
    QuarantinedRecord,
)
from riverwatch.ingestion.metrics import (
    IngestionRunResult,
)
from riverwatch.observability.logging import (
    log_event,
)


def log_ingestion_completed(
    logger: logging.Logger,
    result: IngestionRunResult,
) -> None:
    metrics = result.metrics

    log_event(
        logger,
        logging.INFO,
        "bipad_ingestion_completed",
        run_id=result.run_id,
        endpoint=result.endpoint.value,
        started_at=result.started_at,
        finished_at=result.finished_at,
        duration_ms=result.duration_ms,
        records_received=(
            metrics.records_received
        ),
        records_valid=(
            metrics.records_valid
        ),
        records_invalid=(
            metrics.records_invalid
        ),
        stations_emitted=(
            metrics.stations_emitted
        ),
        observations_emitted=(
            metrics.observations_emitted
        ),
        records_without_observation=(
            metrics.records_without_observation
        ),
    )


def log_quarantined_record(
    logger: logging.Logger,
    *,
    run_id: str,
    record: QuarantinedRecord,
) -> None:
    log_event(
        logger,
        logging.WARNING,
        "bipad_record_quarantined",
        run_id=run_id,
        endpoint=record.endpoint.value,
        record_index=record.record_index,
        validation_issues=[
            issue.model_dump(
                mode="json"
            )
            for issue in record.issues
        ],
    )