import json
from pathlib import Path

import pytest

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.ingestion.bipad.schemas import (
    BipadRiverStation,
)
from riverwatch.ingestion.bipad.validation import (
    QuarantinedRecord,
    ValidatedRecord,
    validate_records,
)

FIXTURE_PATH = Path(
    "tests/fixtures/bipad/river_station.json"
)


def load_station_fixture() -> dict[str, object]:
    return json.loads(
        FIXTURE_PATH.read_text(
            encoding="utf-8",
        )
    )

#test 1 -valid record
def test_valid_record_is_returned() -> None:
    raw_record = load_station_fixture()

    results = list(
        validate_records(
            [raw_record],
            BipadRiverStation,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
        )
    )

    assert len(results) == 1

    result = results[0]

    assert isinstance(
        result,
        ValidatedRecord,
    )

    assert result.record_index == 0

    assert result.value.id == 282

#test2 - Invalid record gets quarantined
def test_invalid_record_is_quarantined() -> None:
    raw_record = load_station_fixture()

    raw_record.pop("point")

    results = list(
        validate_records(
            [raw_record],
            BipadRiverStation,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
        )
    )

    assert len(results) == 1

    result = results[0]

    assert isinstance(
        result,
        QuarantinedRecord,
    )

    assert result.record_index == 0

    assert (
        result.endpoint
        == BipadEndpoint.RIVER_STATIONS
    )

    assert result.raw_record == raw_record

    assert len(result.issues) >= 1

#test 3 - Verify we capture the actual failing field
def test_quarantine_contains_validation_issue() -> None:
    raw_record = load_station_fixture()

    raw_record.pop("point")

    result = list(
        validate_records(
            [raw_record],
            BipadRiverStation,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
        )
    )[0]

    assert isinstance(
        result,
        QuarantinedRecord,
    )

    locations = [
        issue.location
        for issue in result.issues
    ]

    assert ["point"] in locations

def test_invalid_record_does_not_stop_other_records() -> None:
    first = load_station_fixture()

    invalid = load_station_fixture()
    invalid.pop("point")

    third = load_station_fixture()
    third["id"] = 999
    third["title"] = "Another Valid Station"

    results = list(
        validate_records(
            [
                first,
                invalid,
                third,
            ],
            BipadRiverStation,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
        )
    )

    assert len(results) == 3

    assert isinstance(
        results[0],
        ValidatedRecord,
    )

    assert isinstance(
        results[1],
        QuarantinedRecord,
    )

    assert isinstance(
        results[2],
        ValidatedRecord,
    )

    assert results[0].record_index == 0
    assert results[1].record_index == 1
    assert results[2].record_index == 2

# test multiple validation errors
def test_quarantine_can_capture_multiple_issues() -> None:
    raw_record = load_station_fixture()

    raw_record.pop("point")
    raw_record.pop("createdOn")
    raw_record.pop("modifiedOn")

    result = list(
        validate_records(
            [raw_record],
            BipadRiverStation,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
        )
    )[0]

    assert isinstance(
        result,
        QuarantinedRecord,
    )

    locations = {
        tuple(issue.location)
        for issue in result.issues
    }

    assert ("point",) in locations
    assert ("createdOn",) in locations
    assert ("modifiedOn",) in locations

#test ordering
def test_record_indices_are_preserved() -> None:
    records = []

    for station_id in [
        100,
        200,
        300,
    ]:
        record = load_station_fixture()
        record["id"] = station_id

        records.append(
            record
        )

    results = list(
        validate_records(
            records,
            BipadRiverStation,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
        )
    )

    assert [
        result.record_index
        for result in results
    ] == [
        0,
        1,
        2,
    ]

#test indexing behaviour
def test_validation_supports_start_index() -> None:
    first = load_station_fixture()

    second = load_station_fixture()
    second["id"] = 999

    results = list(
        validate_records(
            [
                first,
                second,
            ],
            BipadRiverStation,
            endpoint=(
                BipadEndpoint.RIVER_STATIONS
            ),
            start_index=1000,
        )
    )

    assert [
        result.record_index
        for result in results
    ] == [
        1000,
        1001,
    ]

def test_validation_rejects_negative_start_index() -> None:
    with pytest.raises(
        ValueError,
        match="start_index",
    ):
        list(
            validate_records(
                [],
                BipadRiverStation,
                endpoint=(
                    BipadEndpoint.RIVER_STATIONS
                ),
                start_index=-1,
            )
        )