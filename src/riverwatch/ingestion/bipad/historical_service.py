import logging
from collections.abc import Callable
from datetime import UTC, datetime
from itertools import islice

from riverwatch.ingestion.bipad.client import BipadClient
from riverwatch.ingestion.bipad.endpoints import BipadEndpoint
from riverwatch.ingestion.bipad.mapper import (
    map_bipad_river_record,
)
from riverwatch.ingestion.bipad.schemas import BipadRiverRecord
from riverwatch.ingestion.bipad.validation import (
    QuarantinedRecord,
    validate_records,
)
from riverwatch.ingestion.logging import (
    log_ingestion_completed,
    log_quarantined_record,
)
from riverwatch.ingestion.metrics import (
    IngestionRunResult,
    IngestionRunTracker,
)
from riverwatch.ingestion.models import (
    HistoricalRiverIngestionBatch,
)
from riverwatch.models import RiverObservation


def utc_now() -> datetime:
    return datetime.now(UTC)


BatchHandler = Callable[
    [HistoricalRiverIngestionBatch],
    None,
]


class HistoricalRiverBackfillService:
    def __init__(
        self,
        *,
        client: BipadClient,
        logger: logging.Logger,
        batch_handler: BatchHandler,
        now: Callable[[], datetime] = utc_now,
    ) -> None:
        self._client = client
        self._logger = logger
        self._batch_handler = batch_handler
        self._now = now

    def run(
        self,
        *,
        page_limit: int,
        page_size: int = 1000,
        start_offset: int = 0,
        pagination_safety_limit: int = 1000,
    ) -> IngestionRunResult:
        if page_limit <= 0:
            raise ValueError(
                "page_limit must be greater than zero"
            )

        if page_size <= 0:
            raise ValueError(
                "page_size must be greater than zero"
            )

        if start_offset < 0:
            raise ValueError(
                "start_offset must be greater than "
                "or equal to zero"
            )

        if pagination_safety_limit <= 0:
            raise ValueError(
                "pagination_safety_limit must be "
                "greater than zero"
            )

        if page_limit > pagination_safety_limit:
            raise ValueError(
                "page_limit cannot exceed "
                "pagination_safety_limit"
            )

        tracker = IngestionRunTracker(
            endpoint=BipadEndpoint.RIVER
        )

        ingested_at = self._now()

        global_record_index = start_offset

        pages = self._client.iter_pages(
            BipadEndpoint.RIVER,
            params={
                "limit": page_size,
                "offset": start_offset,
            },
            max_pages=pagination_safety_limit,
        )

        limited_pages = islice(
            pages,
            page_limit,
        )

        for batch_index, page in enumerate(
            limited_pages
        ):
            if not page.results:
                continue

            observations: list[
                RiverObservation
            ] = []

            quarantined_records: list[
                QuarantinedRecord
            ] = []

            validation_results = validate_records(
                page.results,
                BipadRiverRecord,
                endpoint=BipadEndpoint.RIVER,
                start_index=global_record_index,
            )

            for validation_result in validation_results:
                tracker.record_validation_result(
                    validation_result
                )

                if isinstance(
                    validation_result,
                    QuarantinedRecord,
                ):
                    quarantined_records.append(
                        validation_result
                    )
                    continue

                observation = map_bipad_river_record(
                    validation_result.value,
                    ingested_at=ingested_at,
                )

                observations.append(
                    observation
                )

                tracker.record_observation_emitted()

            batch = HistoricalRiverIngestionBatch(
                batch_index=batch_index,
                records_received=len(
                    page.results
                ),
                observations=tuple(
                    observations
                ),
                quarantined_records=tuple(
                    quarantined_records
                ),
            )

            self._batch_handler(
                batch
            )

            global_record_index += len(
                page.results
            )

        run_result = tracker.finish()

        for quarantined_record in (
            run_result.quarantined_records
        ):
            log_quarantined_record(
                self._logger,
                run_id=run_result.run_id,
                record=quarantined_record,
            )

        log_ingestion_completed(
            self._logger,
            run_result,
        )

        return run_result