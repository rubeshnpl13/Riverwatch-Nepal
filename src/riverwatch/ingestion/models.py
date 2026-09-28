from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)

from riverwatch.ingestion.bipad.validation import (
    QuarantinedRecord,
)
from riverwatch.ingestion.metrics import (
    IngestionRunResult,
)
from riverwatch.models import (
    RiverObservation,
    RiverStation,
)


class RiverStationIngestionBatch(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    stations: tuple[
        RiverStation,
        ...,
    ]

    observations: tuple[
        RiverObservation,
        ...,
    ]

    quarantined_records: tuple[
        QuarantinedRecord,
        ...,
    ]

    run_result: IngestionRunResult

class HistoricalRiverIngestionBatch(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    batch_index: int = Field(
        ge=0,
    )

    records_received: int = Field(
        ge=0,
    )

    observations: tuple[
        RiverObservation,
        ...,
    ]

    quarantined_records: tuple[
        QuarantinedRecord,
        ...,
    ]

    @model_validator(mode="after")
    def validate_counts(
        self,
    ) -> "HistoricalRiverIngestionBatch":
        processed = (
            len(self.observations)
            + len(self.quarantined_records)
        )

        if processed != self.records_received:
            raise ValueError(
                "observations + quarantined records "
                "must equal records_received"
            )

        return self