from datetime import (
    UTC,
    datetime,
)

import pytest

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.quality.model import (
    DatasetQualitySummary,
    QualityDataset,
    QualityRunReport,
    QualityStatus,
)
from riverwatch.quality.writer import (
    QualityReportWriter,
)
from riverwatch.storage.errors import (
    ObjectAlreadyExistsError,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)

CAPTURED_AT = datetime(
    2026,
    9,
    29,
    7,
    0,
    tzinfo=UTC,
)


def create_report() -> QualityRunReport:
    return QualityRunReport(
        report_version=1,
        run_id="quality-test",
        endpoint=BipadEndpoint.RIVER,
        captured_at=CAPTURED_AT,
        station_summary=None,
        observation_summary=(
            DatasetQualitySummary(
                dataset=(
                    QualityDataset.OBSERVATIONS
                ),
                total_rows=2,
                pass_rows=2,
                warn_rows=0,
                fail_rows=0,
                affected_rows=0,
                pass_ratio=1.0,
                status=(
                    QualityStatus.PASS
                ),
                error_rule_counts=(),
                warning_rule_counts=(),
            )
        ),
        current_data_health=None,
    )


def test_writes_quality_report(
    tmp_path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    writer = QualityReportWriter(
        store=store
    )

    result = writer.write_report(
        report=create_report()
    )

    report_path = (
        tmp_path
        / result.key
    )

    assert report_path.exists()

    content = (
        report_path.read_text(
            encoding="utf-8"
        )
    )

    assert (
        '"run_id":"quality-test"'
        in content
    )

    assert (
        '"status":"pass"'
        in content
    )


def test_quality_report_is_immutable(
    tmp_path,
) -> None:
    store = LocalObjectStore(
        root=tmp_path
    )

    writer = QualityReportWriter(
        store=store
    )

    report = create_report()

    writer.write_report(
        report=report
    )

    with pytest.raises(
        ObjectAlreadyExistsError
    ):
        writer.write_report(
            report=report
        )