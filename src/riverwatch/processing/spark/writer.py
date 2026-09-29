from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame

from riverwatch.processing.errors import (
    ProcessedOutputAlreadyExistsError,
)
from riverwatch.processing.models import (
    ProcessedDataset,
    ProcessingInputRun,
)
from riverwatch.storage.paths import (
    build_processed_run_prefix,
)


def write_processed_parquet(
    *,
    dataframe: DataFrame,
    lake_root: Path,
    dataset: ProcessedDataset,
    processing_input: ProcessingInputRun,
) -> Path:
    output_path = (
        build_processed_output_path(
            lake_root=lake_root,
            dataset=dataset,
            processing_input=(
                processing_input
            ),
        )
    )

    if output_path.exists():
        raise (
            ProcessedOutputAlreadyExistsError(
                "Processed output already exists: "
                f"{output_path}"
            )
        )

    (
        dataframe.write
        .mode("errorifexists")
        .option(
            "compression",
            "snappy",
        )
        .parquet(
            str(
                output_path.resolve()
            )
        )
    )

    return output_path

def build_processed_output_path(
    *,
    lake_root: Path,
    dataset: ProcessedDataset,
    processing_input: ProcessingInputRun,
) -> Path:
    relative_prefix = (
        build_processed_run_prefix(
            dataset=dataset,
            endpoint=(
                processing_input.endpoint
            ),
            captured_at=(
                processing_input.captured_at
            ),
            run_id=(
                processing_input.run_id
            ),
        )
    )

    return (
        lake_root
        / relative_prefix
    )

def ensure_processed_outputs_absent(
    *,
    lake_root: Path,
    datasets: Sequence[
        ProcessedDataset
    ],
    processing_input: ProcessingInputRun,
) -> None:
    for dataset in datasets:
        output_path = (
            build_processed_output_path(
                lake_root=lake_root,
                dataset=dataset,
                processing_input=(
                    processing_input
                ),
            )
        )

        if output_path.exists():
            raise (
                ProcessedOutputAlreadyExistsError(
                    "Processed output already exists: "
                    f"{output_path}"
                )
            )