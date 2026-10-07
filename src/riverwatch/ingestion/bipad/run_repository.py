from __future__ import annotations

from pathlib import Path
from typing import Protocol

from pydantic import ValidationError

from riverwatch.events.worker import (
    IngestionRunReference,
)
from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.models.source import (
    DataProvider,
)
from riverwatch.storage.manifest import (
    BipadRunManifest,
)


class BipadEventRunnerError(
    RuntimeError
):
    """BIPAD event runner state is invalid."""


class IngestionRunRepository(Protocol):
    def find_completed_run(
        self,
        *,
        run_prefix: str,
    ) -> IngestionRunReference | None:
        ...

    def reference_for_run_id(
        self,
        *,
        run_id: str,
    ) -> IngestionRunReference | None:
        ...

    def next_attempt_number(
        self,
        *,
        run_prefix: str,
    ) -> int:
        ...


def parse_attempt_number(
    *,
    run_id: str,
    run_prefix: str,
) -> int | None:
    if not run_id.startswith(
        run_prefix
    ):
        return None

    raw_attempt = run_id[
        len(run_prefix):
    ]

    if (
        len(raw_attempt) != 4
        or not raw_attempt.isdigit()
    ):
        return None

    attempt = int(
        raw_attempt
    )

    if attempt < 1:
        return None

    return attempt


class LocalIngestionRunRepository:
    """Recover BIPAD ingestion runs from the local lake."""

    def __init__(
        self,
        *,
        lake_root: Path,
    ) -> None:
        self._lake_root = (
            lake_root
            .expanduser()
            .resolve()
        )

    def find_completed_run(
        self,
        *,
        run_prefix: str,
    ) -> IngestionRunReference | None:
        manifest_root = (
            self._manifest_root()
        )

        if not manifest_root.is_dir():
            return None

        references: list[
            IngestionRunReference
        ] = []

        for path in manifest_root.rglob(
            "manifest.json"
        ):
            parent_name = (
                path.parent.name
            )

            if not parent_name.startswith(
                "run_id="
            ):
                continue

            run_id = (
                parent_name
                .removeprefix(
                    "run_id="
                )
            )

            if not run_id.startswith(
                run_prefix
            ):
                continue

            references.append(
                self._load_completed_manifest(
                    path=path,
                    expected_run_id=run_id,
                )
            )

        if not references:
            return None

        if len(references) > 1:
            raise BipadEventRunnerError(
                "Multiple completed ingestion "
                "runs exist for run prefix "
                f"{run_prefix}"
            )

        return references[0]

    def reference_for_run_id(
        self,
        *,
        run_id: str,
    ) -> IngestionRunReference | None:
        manifest_root = (
            self._manifest_root()
        )

        if not manifest_root.is_dir():
            return None

        matches = [
            path
            for path in (
                manifest_root.rglob(
                    "manifest.json"
                )
            )
            if (
                path.parent.name
                == f"run_id={run_id}"
            )
        ]

        if not matches:
            return None

        if len(matches) > 1:
            raise BipadEventRunnerError(
                "Multiple manifests exist for "
                f"run_id={run_id}"
            )

        return self._load_completed_manifest(
            path=matches[0],
            expected_run_id=run_id,
        )

    def next_attempt_number(
        self,
        *,
        run_prefix: str,
    ) -> int:
        highest_attempt = 0

        for top_level in (
            "raw",
            "quarantine",
            "manifests",
        ):
            root = (
                self._lake_root
                / top_level
            )

            if not root.is_dir():
                continue

            for path in root.rglob(
                f"run_id={run_prefix}*"
            ):
                if not path.is_dir():
                    continue

                run_id = (
                    path.name
                    .removeprefix(
                        "run_id="
                    )
                )

                attempt = (
                    parse_attempt_number(
                        run_id=run_id,
                        run_prefix=run_prefix,
                    )
                )

                if attempt is None:
                    continue

                highest_attempt = max(
                    highest_attempt,
                    attempt,
                )

        return highest_attempt + 1

    def _manifest_root(
        self,
    ) -> Path:
        return (
            self._lake_root
            / "manifests"
            / "provider=bipad"
            / (
                "endpoint="
                f"{BipadEndpoint.RIVER_STATIONS.value}"
            )
        )

    def _load_completed_manifest(
        self,
        *,
        path: Path,
        expected_run_id: str,
    ) -> IngestionRunReference:
        try:
            manifest = (
                BipadRunManifest
                .model_validate_json(
                    path.read_text(
                        encoding="utf-8"
                    )
                )
            )

        except (
            OSError,
            ValidationError,
        ) as exc:
            raise BipadEventRunnerError(
                "Unable to load ingestion "
                f"manifest: {path}"
            ) from exc

        if (
            manifest.provider
            != DataProvider.BIPAD
        ):
            raise BipadEventRunnerError(
                "Recovered manifest has "
                "unexpected provider"
            )

        if (
            manifest.endpoint
            != BipadEndpoint.RIVER_STATIONS
        ):
            raise BipadEventRunnerError(
                "Recovered manifest has "
                "unexpected endpoint"
            )

        if (
            manifest.run_id
            != expected_run_id
        ):
            raise BipadEventRunnerError(
                "Recovered manifest run_id "
                "does not match its path"
            )

        if manifest.status != "completed":
            raise BipadEventRunnerError(
                "Recovered ingestion manifest "
                "is not completed"
            )

        try:
            relative_path = (
                path.relative_to(
                    self._lake_root
                )
            )

        except ValueError as exc:
            raise BipadEventRunnerError(
                "Manifest is outside "
                "the lake root"
            ) from exc

        return IngestionRunReference(
            run_id=manifest.run_id,
            manifest_path=(
                relative_path.as_posix()
            ),
            completed_at=(
                manifest.finished_at
            ),
        )