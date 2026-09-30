from datetime import timedelta

import pytest

from riverwatch.quality.model import (
    ObservationQualityPolicy,
)


def test_rejects_non_positive_current_max_age() -> None:
    with pytest.raises(
        ValueError,
        match="current_max_age",
    ):
        ObservationQualityPolicy(
            current_max_age=timedelta(0)
        )


def test_rejects_non_positive_suspicious_level() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "suspicious_absolute_water_level_m"
        ),
    ):
        ObservationQualityPolicy(
            suspicious_absolute_water_level_m=0
        )