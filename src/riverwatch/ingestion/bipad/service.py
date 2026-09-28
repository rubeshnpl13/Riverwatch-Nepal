import logging
from collections.abc import Callable
from datetime import UTC, datetime

from riverwatch.ingestion.bipad.client import (
    BipadClient,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.mapper import (
    map_bipad_station,
    map_bipad_station_observation,
)
from riverwatch.ingestion.bipad.schemas import (
    BipadRiverStation,
)
from riverwatch.ingestion.bipad.validation import (
    QuarantinedRecord,
    validate_records,
)
from riverwatch.ingestion.logging import (
    log_ingestion_completed,
    log_quarantined_record,
)
from riverwatch.ingestion.metrics import (
    IngestionRunTracker,
)
from riverwatch.ingestion.models import (
    RiverStationIngestionBatch,
)
from riverwatch.models import (
    RiverObservation,
    RiverStation,
)


def utc_now() -> datetime:
    return datetime.now(UTC)


class RiverStationIngestionService:
    def __init__(
        self,
        *,
        client: BipadClient,
        logger: logging.Logger,
        now: Callable[[], datetime] = utc_now,
    ) -> None:
        self._client = client
        self._logger = logger
        self._now = now

    def run(
        self,
        *,
        max_pages: int = 100,
    ) -> RiverStationIngestionBatch:
        tracker = IngestionRunTracker(
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            )
        )

        # One ingestion timestamp for the entire batch.
        #
        # This is intentionally calculated once rather
        # than once per observation.
        ingested_at = self._now()

        stations: list[
            RiverStation
        ] = []

        observations: list[
            RiverObservation
        ] = []

        raw_records = self._client.iter_records(
            BipadEndpoint.RIVER_STATIONS,
            max_pages=max_pages,
        )

        validation_results = validate_records(
            raw_records,
            BipadRiverStation,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
        )

        for validation_result in validation_results:
            tracker.record_validation_result(
                validation_result
            )

            if isinstance(
                validation_result,
                QuarantinedRecord,
            ):
                continue

            source_record = (
                validation_result.value
            )

            station = map_bipad_station(
                source_record
            )

            stations.append(
                station
            )

            tracker.record_station_emitted()

            observation = (
                map_bipad_station_observation(
                    source_record,
                    ingested_at=ingested_at,
                )
            )

            if observation is None:
                tracker.record_without_observation()
                continue

            observations.append(
                observation
            )

            tracker.record_observation_emitted()

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

        return RiverStationIngestionBatch(
            stations=tuple(
                stations
            ),
            observations=tuple(
                observations
            ),
            quarantined_records=(
                run_result.quarantined_records
            ),
            run_result=run_result,
        )