from __future__ import annotations

from pathlib import Path
from uuid import UUID

from pyspark.sql import SparkSession

from riverwatch.events.processing import (
    ProcessingRunReference,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.models import (
    ProcessedDataset,
)
from riverwatch.processing.spark.recovery import (
    process_manifest_retry_safe,
)

SUPPORTED_PROVIDER = "bipad"


class ProcessingEventRunnerError(
    RuntimeError
):
    """Processing event runner state is invalid."""


class UnsupportedProcessingRequestError(
    ValueError
):
    """Processing request is unsupported."""


class SparkProcessingEventRunner:
    def __init__(
        self,
        *,
        spark: SparkSession,
        lake_root: Path,
    ) -> None:
        self._spark = spark

        self._lake_root = (
            lake_root
            .expanduser()
            .resolve()
        )

    def run(
        self,
        *,
        provider: str,
        endpoint: str,
        run_id: str,
        manifest_path: str,
        idempotency_key: UUID,
    ) -> ProcessingRunReference:
        _ = idempotency_key

        if provider != SUPPORTED_PROVIDER:
            raise (
                UnsupportedProcessingRequestError(
                    "Unsupported processing "
                    f"provider: {provider}"
                )
            )

        try:
            expected_endpoint = (
                BipadEndpoint(
                    endpoint
                )
            )

        except ValueError as exc:
            raise (
                UnsupportedProcessingRequestError(
                    "Unsupported processing "
                    f"endpoint: {endpoint}"
                )
            ) from exc

        if expected_endpoint not in {
            BipadEndpoint.RIVER,
            BipadEndpoint.RIVER_STATIONS,
        }:
            raise (
                UnsupportedProcessingRequestError(
                    "Unsupported processing "
                    f"endpoint: {endpoint}"
                )
            )

        resolved_manifest = (
            self._resolve_manifest_path(
                manifest_path
            )
        )

        result = (
            process_manifest_retry_safe(
                spark=self._spark,
                lake_root=self._lake_root,
                manifest_path=(
                    resolved_manifest
                ),
            )
        )

        if result.run_id != run_id:
            raise ProcessingEventRunnerError(
                "Processing result run_id "
                "does not match the "
                "ingestion event"
            )

        if (
            result.endpoint
            is not expected_endpoint
        ):
            raise ProcessingEventRunnerError(
                "Processing result endpoint "
                "does not match the "
                "ingestion event"
            )

        station_output_path: (
            str | None
        ) = None

        observation_output_path: (
            str | None
        ) = None

        for output in result.outputs:
            relative_path = (
                self._relative_lake_path(
                    output.path
                )
            )

            if (
                output.dataset
                is ProcessedDataset.STATIONS
            ):
                station_output_path = (
                    relative_path
                )

            elif (
                output.dataset
                is ProcessedDataset.OBSERVATIONS
            ):
                observation_output_path = (
                    relative_path
                )

        if (
            observation_output_path
            is None
        ):
            raise ProcessingEventRunnerError(
                "Processing result did not "
                "contain observations"
            )

        return ProcessingRunReference(
            run_id=result.run_id,
            quality_report_path=(
                self._relative_lake_path(
                    result
                    .quality_report_path
                )
            ),
            station_output_path=(
                station_output_path
            ),
            observation_output_path=(
                observation_output_path
            ),
        )

    def _resolve_manifest_path(
        self,
        manifest_path: str,
    ) -> Path:
        cleaned = (
            manifest_path.strip()
        )

        if not cleaned:
            raise ValueError(
                "manifest_path must "
                "not be blank"
            )

        relative = Path(
            cleaned
        )

        if relative.is_absolute():
            raise ValueError(
                "manifest_path must "
                "be relative"
            )

        candidate = (
            self._lake_root
            / relative
        ).resolve()

        try:
            candidate.relative_to(
                self._lake_root
            )

        except ValueError as exc:
            raise ValueError(
                "manifest_path escapes "
                "the lake root"
            ) from exc

        if not candidate.is_file():
            raise FileNotFoundError(
                "Ingestion manifest does "
                f"not exist: {candidate}"
            )

        return candidate

    def _relative_lake_path(
        self,
        path: Path,
    ) -> str:
        resolved = (
            path.resolve()
        )

        try:
            relative = (
                resolved.relative_to(
                    self._lake_root
                )
            )

        except ValueError as exc:
            raise ProcessingEventRunnerError(
                "Processing output is "
                "outside the lake root"
            ) from exc

        return (
            relative.as_posix()
        )