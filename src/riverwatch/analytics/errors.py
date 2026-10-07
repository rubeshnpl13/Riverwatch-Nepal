from pathlib import Path


class AnalyticsDatasetNotFoundError(
    RuntimeError
):
    def __init__(
        self,
        *,
        dataset: str,
        path: Path,
    ) -> None:
        super().__init__(
            "Processed analytics dataset "
            f"{dataset!r} was not found at "
            f"{path}"
        )

class InvalidAnalyticsQueryError(
    ValueError
):
    pass

class BigQueryAnalyticsError(
    RuntimeError
):
    """A BigQuery analytics query failed."""