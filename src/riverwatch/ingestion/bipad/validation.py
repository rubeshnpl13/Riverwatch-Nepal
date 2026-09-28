from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
)

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)


class ValidationIssue(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    error_type: str

    location: list[str | int]

    message: str


class QuarantinedRecord(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    endpoint: BipadEndpoint

    record_index: int = Field(
        ge=0,
    )

    raw_record: Any

    issues: list[ValidationIssue]


@dataclass(
    frozen=True,
    slots=True,
)
class ValidatedRecord[ModelT: BaseModel]:
    record_index: int
    value: ModelT


def validation_issues_from_error(
    error: ValidationError,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    for item in error.errors(
        include_url=False,
        include_context=False,
        include_input=False,
    ):
        location = [
            part
            if isinstance(part, int)
            else str(part)
            for part in item["loc"]
        ]

        issues.append(
            ValidationIssue(
                error_type=str(
                    item["type"]
                ),
                location=location,
                message=str(
                    item["msg"]
                ),
            )
        )

    return issues


def validate_records[ModelT: BaseModel](
    raw_records: Iterable[Any],
    model_type: type[ModelT],
    *,
    endpoint: BipadEndpoint,
    start_index: int = 0,
) -> Iterator[
    ValidatedRecord[ModelT]
    | QuarantinedRecord
]:
    if start_index < 0:
        raise ValueError(
            "start_index must be greater than "
            "or equal to zero"
        )

    for record_index, raw_record in enumerate(
        raw_records,
        start=start_index,
    ):
        try:
            validated = (
                model_type.model_validate(
                    raw_record
                )
            )

        except ValidationError as error:
            yield QuarantinedRecord(
                endpoint=endpoint,
                record_index=record_index,
                raw_record=raw_record,
                issues=validation_issues_from_error(
                    error
                ),
            )

            continue

        yield ValidatedRecord(
            record_index=record_index,
            value=validated,
        )