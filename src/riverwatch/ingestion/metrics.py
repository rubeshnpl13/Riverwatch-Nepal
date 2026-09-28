from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.validation import (
    QuarantinedRecord,
    ValidatedRecord,
)


class IngestionMetrics(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    records_received: int = Field(
        ge=0,
    )

    records_valid: int = Field(
        ge=0,
    )

    records_invalid: int = Field(
        ge=0,
    )

    stations_emitted: int = Field(
        default=0,
        ge=0,
    )

    observations_emitted: int = Field(
        default=0,
        ge=0,
    )

    records_without_observation: int = Field(
        default=0,
        ge=0,
    )

    @model_validator(mode="after")
    def validate_counts(
        self,
    ) -> IngestionMetrics:
        if (
            self.records_valid
            + self.records_invalid
            != self.records_received
        ):
            raise ValueError(
                "records_valid + records_invalid "
                "must equal records_received"
            )

        if (
            self.stations_emitted
            > self.records_valid
        ):
            raise ValueError(
                "stations_emitted cannot exceed "
                "records_valid"
            )

        if (
            self.observations_emitted
            > self.records_valid
        ):
            raise ValueError(
                "observations_emitted cannot exceed "
                "records_valid"
            )

        if (
            self.records_without_observation
            > self.records_valid
        ):
            raise ValueError(
                "records_without_observation cannot "
                "exceed records_valid"
            )

        return self


class IngestionRunResult(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    run_id: str = Field(
        min_length=1,
    )

    endpoint: BipadEndpoint

    started_at: datetime

    finished_at: datetime

    duration_ms: float = Field(
        ge=0,
    )

    metrics: IngestionMetrics

    quarantined_records: tuple[
        QuarantinedRecord,
        ...,
    ] = ()

    @model_validator(mode="after")
    def validate_run(
        self,
    ) -> IngestionRunResult:
        if self.finished_at < self.started_at:
            raise ValueError(
                "finished_at cannot be earlier "
                "than started_at"
            )

        if (
            len(self.quarantined_records)
            != self.metrics.records_invalid
        ):
            raise ValueError(
                "quarantined record count must "
                "match records_invalid"
            )

        return self

#clock helper
def utc_now() -> datetime:
    return datetime.now(UTC)


def monotonic_now() -> float:
    return time.monotonic()

class IngestionRunTracker:
    def __init__(
        self,
        *,
        endpoint: BipadEndpoint,
        run_id: str | None = None,
        now: Callable[[], datetime] = utc_now,
        monotonic_clock: Callable[
            [],
            float,
        ] = monotonic_now,
    ) -> None:
        self._endpoint = endpoint

        self._run_id = (
            run_id
            if run_id is not None
            else str(uuid4())
        )

        self._now = now
        self._monotonic_clock = (
            monotonic_clock
        )

        self._started_at = self._now()

        self._started_monotonic = (
            self._monotonic_clock()
        )

        self._records_received = 0
        self._records_valid = 0
        self._records_invalid = 0

        self._stations_emitted = 0

        self._observations_emitted = 0

        self._records_without_observation = 0

        self._quarantined_records: list[
            QuarantinedRecord
        ] = []

        self._finished = False

    def _ensure_active(self) -> None:
        if self._finished:
            raise RuntimeError(
                "Ingestion run has already finished"
            )

    def record_validation_result(
            self,
            result: (
                    ValidatedRecord[Any]
                    | QuarantinedRecord
            ),
    ) -> None:
        self._ensure_active()

        self._records_received += 1

        if isinstance(
                result,
                QuarantinedRecord,
        ):
            self._records_invalid += 1

            self._quarantined_records.append(
                result
            )

            return

        self._records_valid += 1

    def record_station_emitted(
            self,
    ) -> None:
        self._ensure_active()

        self._stations_emitted += 1

    def record_observation_emitted(
            self,
    ) -> None:
        self._ensure_active()

        self._observations_emitted += 1

    def record_without_observation(
            self,
    ) -> None:
        self._ensure_active()

        self._records_without_observation += 1

    def finish(
            self,
    ) -> IngestionRunResult:
        self._ensure_active()

        finished_at = self._now()

        finished_monotonic = (
            self._monotonic_clock()
        )

        duration_ms = (
                              finished_monotonic
                              - self._started_monotonic
                      ) * 1000

        self._finished = True

        metrics = IngestionMetrics(
            records_received=(
                self._records_received
            ),
            records_valid=(
                self._records_valid
            ),
            records_invalid=(
                self._records_invalid
            ),
            stations_emitted=(
                self._stations_emitted
            ),
            observations_emitted=(
                self._observations_emitted
            ),
            records_without_observation=(
                self._records_without_observation
            ),
        )

        return IngestionRunResult(
            run_id=self._run_id,
            endpoint=self._endpoint,
            started_at=self._started_at,
            finished_at=finished_at,
            duration_ms=max(
                duration_ms,
                0.0,
            ),
            metrics=metrics,
            quarantined_records=tuple(
                self._quarantined_records
            ),
        )

    @property
    def run_id(self) -> str:
        return self._run_id