import logging

from riverwatch.config import get_settings
from riverwatch.ingestion.bipad.client import (
    create_bipad_client,
)
from riverwatch.ingestion.bipad.service import (
    RiverStationIngestionService,
)
from riverwatch.observability.logging import (
    configure_logging,
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
        service = RiverStationIngestionService(
            client=client,
            logger=logger,
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