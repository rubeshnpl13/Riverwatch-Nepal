import json
import logging
from io import StringIO

import pytest

from riverwatch.observability.logging import (
    configure_logging,
    log_event,
)


def test_log_event_outputs_json() -> None:
    output = StringIO()

    configure_logging(
        log_level="INFO",
        stream=output,
    )

    logger = logging.getLogger(
        "riverwatch.test"
    )

    log_event(
        logger,
        logging.INFO,
        "test_event",
        run_id="abc123",
        records=10,
    )

    payload = json.loads(
        output.getvalue()
    )

    assert (
        payload["event"]
        == "test_event"
    )

    assert payload["level"] == "INFO"

    assert (
        payload["logger"]
        == "riverwatch.test"
    )

    assert (
        payload["run_id"]
        == "abc123"
    )

    assert payload["records"] == 10


def test_log_level_filters_messages() -> None:
    output = StringIO()

    configure_logging(
        log_level="WARNING",
        stream=output,
    )

    logger = logging.getLogger(
        "riverwatch.test"
    )

    log_event(
        logger,
        logging.INFO,
        "should_not_appear",
    )

    assert output.getvalue() == ""


def test_invalid_log_level_is_rejected() -> None:
    with pytest.raises(
        ValueError,
        match="Invalid log level",
    ):
        configure_logging(
            log_level="BANANA"
        )


def test_reserved_log_fields_are_rejected() -> None:
    output = StringIO()

    configure_logging(
        stream=output
    )

    logger = logging.getLogger(
        "riverwatch.test"
    )

    with pytest.raises(
        ValueError,
        match="reserved",
    ):
        log_event(
            logger,
            logging.INFO,
            "test_event",
            level="something",
        )


def test_reconfiguring_logging_does_not_duplicate_logs() -> None:
    output = StringIO()

    configure_logging(
        stream=output
    )

    configure_logging(
        stream=output
    )

    logger = logging.getLogger(
        "riverwatch.test"
    )

    log_event(
        logger,
        logging.INFO,
        "single_event",
    )

    lines = [
        line
        for line in (
            output.getvalue().splitlines()
        )
        if line
    ]

    assert len(lines) == 1