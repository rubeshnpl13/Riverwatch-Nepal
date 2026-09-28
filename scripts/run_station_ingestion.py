import logging
from pathlib import Path

from riverwatch.config import get_settings
from riverwatch.ingestion.bipad.client import (
    create_bipad_client,
)
from riverwatch.ingestion.bipad.manifest_writer import (
    BipadRunManifestWriter,
)
from riverwatch.ingestion.bipad.quarantine_writer import (
    BipadQuarantineWriter,
)
from riverwatch.ingestion.bipad.raw_writer import (
    BipadRawPageWriter,
)
from riverwatch.ingestion.bipad.service import (
    RiverStationIngestionService,
)
from riverwatch.observability.logging import (
    configure_logging,
)
from riverwatch.storage.local import (
    LocalObjectStore,
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
        service = RiverStationIngestionService(
            client=client,
            logger=logger,
            raw_writer=raw_writer,
            quarantine_writer=quarantine_writer,
            manifest_writer=manifest_writer,
        )

        batch = service.run()

    print()

    print(
        "Stations:",
        len(batch.stations),
    )

    print(
        "Observations:",
        len(batch.observations),
    )

    print(
        "Quarantined:",
        len(
            batch.quarantined_records
        ),
    )


if __name__ == "__main__":
    main()