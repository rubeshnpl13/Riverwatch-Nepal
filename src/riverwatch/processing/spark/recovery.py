from __future__ import annotations

import shutil
from hashlib import sha256
from pathlib import Path

from pyspark.sql import SparkSession

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.manifest_loader import (
    load_processing_input,
)
from riverwatch.processing.models import (
    ProcessedDataset,
    ProcessedOutput,
    ProcessingInputRun,
    ProcessingRunResult,
)
from riverwatch.processing.spark.pipeline import (
    process_manifest,
)
from riverwatch.processing.spark.writer import (
    build_processed_output_path,
)
from riverwatch.storage.paths import (
    build_quality_report_key,
)


class ProcessingRecoveryError(
    RuntimeError
):
    """Processing state cannot be safely recovered."""


def process_manifest_retry_safe(
    *,
    spark: SparkSession,
    lake_root: Path,
    manifest_path: Path,
) -> ProcessingRunResult:
    processing_input = load_processing_input(
        lake_root=lake_root,
        manifest_path=manifest_path,
    )

    datasets = _datasets_for_endpoint(
        processing_input.endpoint
    )

    final_output_paths = {
        dataset: build_processed_output_path(
            lake_root=lake_root,
            dataset=dataset,
            processing_input=processing_input,
        )
        for dataset in datasets
    }

    final_quality_path = (
        _build_final_quality_path(
            lake_root=lake_root,
            processing_input=processing_input,
        )
    )

    _validate_existing_final_state(
        output_paths=final_output_paths,
        quality_path=final_quality_path,
    )

    staging_root = _build_staging_root(
        lake_root=lake_root,
        processing_input=processing_input,
    )

    if _is_fully_committed(
        output_paths=final_output_paths,
        quality_path=final_quality_path,
    ):
        _remove_staging_root(
            staging_root
        )

        return _recover_result(
            spark=spark,
            processing_input=processing_input,
            output_paths=final_output_paths,
            quality_path=final_quality_path,
        )

    _remove_staging_root(
        staging_root
    )

    staging_root.mkdir(
        parents=True,
        exist_ok=False,
    )

    staged_result = process_manifest(
        spark=spark,
        lake_root=lake_root,
        manifest_path=manifest_path,
        output_root=staging_root,
    )

    try:
        final_outputs = (
            _commit_processed_outputs(
                staged_result=staged_result,
                final_output_paths=(
                    final_output_paths
                ),
            )
        )

        _commit_quality_report(
            staged_quality_path=(
                staged_result
                .quality_report_path
            ),
            final_quality_path=(
                final_quality_path
            ),
        )

    except Exception:
        # Keep staging artifacts available for
        # inspection after a failed commit.
        raise

    _remove_staging_root(
        staging_root
    )

    return ProcessingRunResult(
        run_id=staged_result.run_id,
        endpoint=staged_result.endpoint,
        outputs=tuple(
            final_outputs
        ),
        quality_report_path=(
            final_quality_path
        ),
    )


def _datasets_for_endpoint(
    endpoint: BipadEndpoint,
) -> tuple[
    ProcessedDataset,
    ...,
]:
    if endpoint is BipadEndpoint.RIVER:
        return (
            ProcessedDataset.OBSERVATIONS,
        )

    if (
        endpoint
        is BipadEndpoint.RIVER_STATIONS
    ):
        return (
            ProcessedDataset.STATIONS,
            ProcessedDataset.OBSERVATIONS,
        )

    raise ProcessingRecoveryError(
        "Unsupported processing endpoint: "
        f"{endpoint.value}"
    )


def _build_final_quality_path(
    *,
    lake_root: Path,
    processing_input: ProcessingInputRun,
) -> Path:
    key = build_quality_report_key(
        endpoint=processing_input.endpoint,
        captured_at=(
            processing_input.captured_at
        ),
        run_id=processing_input.run_id,
    )

    return lake_root / key


def _build_staging_root(
    *,
    lake_root: Path,
    processing_input: ProcessingInputRun,
) -> Path:
    identity = (
        f"{processing_input.endpoint.value}\n"
        f"{processing_input.run_id}\n"
        f"{processing_input.captured_at.isoformat()}"
    )

    token = sha256(
        identity.encode(
            "utf-8"
        )
    ).hexdigest()

    return (
        lake_root
        / ".staging"
        / "processing"
        / token
    )


def _remove_staging_root(
    staging_root: Path,
) -> None:
    if not staging_root.exists():
        return

    shutil.rmtree(
        staging_root
    )


def _is_processed_output_complete(
    path: Path,
) -> bool:
    return (
        path.is_dir()
        and (
            path
            / "_SUCCESS"
        ).is_file()
    )


def _validate_existing_final_state(
    *,
    output_paths: dict[
        ProcessedDataset,
        Path,
    ],
    quality_path: Path,
) -> None:
    for path in output_paths.values():
        if (
            path.exists()
            and not _is_processed_output_complete(
                path
            )
        ):
            raise ProcessingRecoveryError(
                "Processed output exists "
                "without a Spark success "
                f"marker: {path}"
            )

    if (
        quality_path.exists()
        and not quality_path.is_file()
    ):
        raise ProcessingRecoveryError(
            "Quality report path exists "
            "but is not a file: "
            f"{quality_path}"
        )


def _is_fully_committed(
    *,
    output_paths: dict[
        ProcessedDataset,
        Path,
    ],
    quality_path: Path,
) -> bool:
    return (
        all(
            _is_processed_output_complete(
                path
            )
            for path
            in output_paths.values()
        )
        and quality_path.is_file()
    )


def _recover_result(
    *,
    spark: SparkSession,
    processing_input: ProcessingInputRun,
    output_paths: dict[
        ProcessedDataset,
        Path,
    ],
    quality_path: Path,
) -> ProcessingRunResult:
    outputs: list[
        ProcessedOutput
    ] = []

    for dataset, path in (
        output_paths.items()
    ):
        row_count = (
            spark.read
            .parquet(
                str(path)
            )
            .count()
        )

        outputs.append(
            ProcessedOutput(
                dataset=dataset,
                path=path,
                row_count=row_count,
            )
        )

    return ProcessingRunResult(
        run_id=processing_input.run_id,
        endpoint=processing_input.endpoint,
        outputs=tuple(
            outputs
        ),
        quality_report_path=(
            quality_path
        ),
    )


def _commit_processed_outputs(
    *,
    staged_result: ProcessingRunResult,
    final_output_paths: dict[
        ProcessedDataset,
        Path,
    ],
) -> list[
    ProcessedOutput
]:
    final_outputs: list[
        ProcessedOutput
    ] = []

    for staged_output in (
        staged_result.outputs
    ):
        final_path = (
            final_output_paths[
                staged_output.dataset
            ]
        )

        if final_path.exists():
            if not _is_processed_output_complete(
                final_path
            ):
                raise ProcessingRecoveryError(
                    "Existing processed output "
                    "is incomplete: "
                    f"{final_path}"
                )

        else:
            staged_path = (
                staged_output.path
            )

            if not (
                _is_processed_output_complete(
                    staged_path
                )
            ):
                raise ProcessingRecoveryError(
                    "Staged processed output "
                    "is incomplete: "
                    f"{staged_path}"
                )

            final_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            staged_path.rename(
                final_path
            )

        final_outputs.append(
            ProcessedOutput(
                dataset=(
                    staged_output.dataset
                ),
                path=final_path,
                row_count=(
                    staged_output.row_count
                ),
            )
        )

    return final_outputs


def _commit_quality_report(
    *,
    staged_quality_path: Path,
    final_quality_path: Path,
) -> None:
    if final_quality_path.exists():
        if not final_quality_path.is_file():
            raise ProcessingRecoveryError(
                "Existing quality report "
                "is not a file: "
                f"{final_quality_path}"
            )

        if (
            final_quality_path.read_bytes()
            != staged_quality_path.read_bytes()
        ):
            raise ProcessingRecoveryError(
                "Existing quality report "
                "does not match the "
                "recomputed report"
            )

        return

    if not staged_quality_path.is_file():
        raise ProcessingRecoveryError(
            "Staged quality report does "
            "not exist: "
            f"{staged_quality_path}"
        )

    final_quality_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    staged_quality_path.rename(
        final_quality_path
    )