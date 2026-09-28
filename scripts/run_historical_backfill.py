import logging

from riverwatch.config import get_settings
from riverwatch.ingestion.bipad.client import (
    create_bipad_client,
)
from riverwatch.ingestion.bipad.historical_service import (
    HistoricalRiverBackfillService,
)
from riverwatch.ingestion.models import (
    HistoricalRiverIngestionBatch,
)
from riverwatch.observability.logging import (
    configure_logging,
)


def handle_batch(
    batch: HistoricalRiverIngestionBatch,
) -> None:
    print(
        "Batch:",
        batch.batch_index,
        "received:",
        batch.records_received,
        "observations:",
        len(batch.observations),
        "quarantined:",
        len(batch.quarantined_records),
    )


def main() -> None:
    settings = get_settings()

    configure_logging(
        log_level=settings.log_level
    )

    logger = logging.getLogger(
        "riverwatch.ingestion"
    )

    with create_bipad_client(
        settings
    ) as client:
        service = HistoricalRiverBackfillService(
            client=client,
            logger=logger,
            batch_handler=handle_batch,
        )

        result = service.run(
            page_limit=2,
            page_size=1000,
        )

    print()
    print(
        "Total received:",
        result.metrics.records_received,
    )

    print(
        "Total observations:",
        result.metrics.observations_emitted,
    )

    print(
        "Total quarantined:",
        result.metrics.records_invalid,
    )


if __name__ == "__main__":
    main()