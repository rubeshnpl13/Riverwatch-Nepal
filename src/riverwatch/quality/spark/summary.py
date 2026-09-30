from pyspark.sql import (
    DataFrame,
)
from pyspark.sql import (
    functions as F,
)

from riverwatch.quality.model import (
    DatasetQualitySummary,
    QualityDataset,
    QualityRuleCount,
    QualityStatus,
)


def _summarize_rule_counts(
    *,
    dataframe: DataFrame,
    column_name: str,
) -> tuple[
    QualityRuleCount,
    ...,
]:
    rows = (
        dataframe
        .select(
            F.explode(
                F.col(
                    column_name
                )
            ).alias(
                "rule"
            )
        )
        .where(
            F.col(
                "rule"
            ).isNotNull()
        )
        .groupBy(
            "rule"
        )
        .count()
        .orderBy(
            "rule"
        )
        .collect()
    )

    return tuple(
        QualityRuleCount(
            rule=str(
                row["rule"]
            ),
            count=int(
                row["count"]
            ),
        )
        for row in rows
    )


def summarize_dataset_quality(
    *,
    dataframe: DataFrame,
    dataset: QualityDataset,
) -> DatasetQualitySummary:
    counts = (
        dataframe
        .agg(
            F.count(
                F.lit(1)
            ).alias(
                "total_rows"
            ),
            F.coalesce(
                F.sum(
                    F.when(
                        F.col(
                            "quality_status"
                        )
                        == QualityStatus.PASS.value,
                        F.lit(1),
                    ).otherwise(
                        F.lit(0)
                    )
                ),
                F.lit(0),
            ).alias(
                "pass_rows"
            ),
            F.coalesce(
                F.sum(
                    F.when(
                        F.col(
                            "quality_status"
                        )
                        == QualityStatus.WARN.value,
                        F.lit(1),
                    ).otherwise(
                        F.lit(0)
                    )
                ),
                F.lit(0),
            ).alias(
                "warn_rows"
            ),
            F.coalesce(
                F.sum(
                    F.when(
                        F.col(
                            "quality_status"
                        )
                        == QualityStatus.FAIL.value,
                        F.lit(1),
                    ).otherwise(
                        F.lit(0)
                    )
                ),
                F.lit(0),
            ).alias(
                "fail_rows"
            ),
        )
        .collect()[0]
    )

    total_rows = int(
        counts[
            "total_rows"
        ]
    )

    pass_rows = int(
        counts[
            "pass_rows"
        ]
    )

    warn_rows = int(
        counts[
            "warn_rows"
        ]
    )

    fail_rows = int(
        counts[
            "fail_rows"
        ]
    )

    affected_rows = (
        warn_rows
        + fail_rows
    )

    if total_rows > 0:
        pass_ratio = (
            pass_rows
            / total_rows
        )
    else:
        pass_ratio = 0.0

    if (
        total_rows == 0
        or fail_rows > 0
    ):
        status = QualityStatus.FAIL

    elif warn_rows > 0:
        status = QualityStatus.WARN

    else:
        status = QualityStatus.PASS

    error_rule_counts = (
        _summarize_rule_counts(
            dataframe=dataframe,
            column_name="quality_errors",
        )
    )

    warning_rule_counts = (
        _summarize_rule_counts(
            dataframe=dataframe,
            column_name="quality_warnings",
        )
    )

    return DatasetQualitySummary(
        dataset=dataset,
        total_rows=total_rows,
        pass_rows=pass_rows,
        warn_rows=warn_rows,
        fail_rows=fail_rows,
        affected_rows=affected_rows,
        pass_ratio=pass_ratio,
        status=status,
        error_rule_counts=(
            error_rule_counts
        ),
        warning_rule_counts=(
            warning_rule_counts
        ),
    )