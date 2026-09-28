import logging
from collections.abc import Callable
from datetime import UTC, datetime

from riverwatch.ingestion.bipad.client import (
    BipadClient,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.manifest_writer import (
    BipadRunManifestWriter,
)
from riverwatch.ingestion.bipad.mapper import (
    map_bipad_station,
    map_bipad_station_observation,
)
from riverwatch.ingestion.bipad.quarantine_writer import (
    BipadQuarantineWriter,
)
from riverwatch.ingestion.bipad.raw_writer import (
    BipadRawPageWriter,
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
from riverwatch.storage.quarantine import (
    QuarantineWriteResult,
)
from riverwatch.storage.raw import (
    RawPageWriteResult,
)


def utc_now() -> datetime:
    return datetime.now(UTC)


class RiverStationIngestionService:
    def __init__(
        self,
        *,
        client: BipadClient,
        logger: logging.Logger,
        raw_writer: BipadRawPageWriter | None = None,
        quarantine_writer: BipadQuarantineWriter | None = None,
        manifest_writer: BipadRunManifestWriter | None = None,
        now: Callable[[], datetime] = utc_now,
    ) -> None:
        self._client = client
        self._logger = logger
        self._raw_writer = raw_writer
        self._quarantine_writer = (
            quarantine_writer
        )
        self._manifest_writer = (
            manifest_writer
        )
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

        raw_page_results: list[
            RawPageWriteResult
        ] = []

        quarantine_results: list[
            QuarantineWriteResult
        ] = []

        record_index = 0

        pages = self._client.iter_pages(
            BipadEndpoint.RIVER_STATIONS,
            max_pages=max_pages,
        )

        for page_index, page in enumerate(
                pages
        ):
            if self._raw_writer is not None:
                raw_result = (
                    self._raw_writer.write_page(
                        page=page,
                        endpoint=(
                            BipadEndpoint.RIVER_STATIONS
                        ),
                        run_id=tracker.run_id,
                        captured_at=ingested_at,
                        page_index=page_index,
                    )
                )

                raw_page_results.append(
                    raw_result
                )

            validation_results = validate_records(
                page.results,
                BipadRiverStation,
                endpoint=(
                    BipadEndpoint.RIVER_STATIONS
                ),
                start_index=record_index,
            )

            for validation_result in (
                    validation_results
            ):
                tracker.record_validation_result(
                    validation_result
                )
                if isinstance(
                        validation_result,
                        QuarantinedRecord,
                ):
                    if (
                            self._quarantine_writer
                            is not None
                    ):
                        quarantine_result = (
                            self._quarantine_writer.write_record(
                                record=validation_result,
                                run_id=tracker.run_id,
                                captured_at=ingested_at,
                                page_index=page_index,
                            )
                        )

                        quarantine_results.append(
                            quarantine_result
                        )
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

            record_index += len(
                page.results
            )

        run_result = tracker.finish()

        if self._manifest_writer is not None:
            self._manifest_writer.write_manifest(
                run_result=run_result,
                captured_at=ingested_at,
                raw_pages=raw_page_results,
                quarantine_objects=(
                    quarantine_results
                ),
            )

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