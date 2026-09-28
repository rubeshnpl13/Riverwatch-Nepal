import logging
from pathlib import Path

from riverwatch.config import get_settings
from riverwatch.ingestion.bipad.client import (
    create_bipad_client,
)
from riverwatch.ingestion.bipad.historical_service import (
    HistoricalRiverBackfillService,
)
from riverwatch.ingestion.bipad.manifest_writer import BipadRunManifestWriter
from riverwatch.ingestion.bipad.quarantine_writer import BipadQuarantineWriter
from riverwatch.ingestion.bipad.raw_writer import (
    BipadRawPageWriter,
)
from riverwatch.ingestion.models import (
    HistoricalRiverIngestionBatch,
)
from riverwatch.observability.logging import (
    configure_logging,
)
from riverwatch.storage.local import (
    LocalObjectStore,
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
    store = LocalObjectStore(
        root=Path(
            "data/lake"
        )
    )
    manifest_writer = BipadRunManifestWriter(
        store=store
    )
    raw_writer = BipadRawPageWriter(
        store=store
    )
    quarantine_writer = BipadQuarantineWriter(
        store=store
    )
    with create_bipad_client(
        settings
    ) as client:
        service = HistoricalRiverBackfillService(
            client=client,
            logger=logger,
            batch_handler=handle_batch,
            raw_writer=raw_writer,
            quarantine_writer=quarantine_writer,
            manifest_writer=manifest_writer,
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