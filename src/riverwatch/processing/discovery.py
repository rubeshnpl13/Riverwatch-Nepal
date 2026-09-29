from pathlib import Path

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.manifest_loader import (
    ProcessingManifestError,
)


def find_latest_manifest(
    *,
    lake_root: Path,
    endpoint: BipadEndpoint,
) -> Path:
    manifest_root = (
        lake_root
        / "manifests"
        / "provider=bipad"
        / f"endpoint={endpoint.value}"
    )

    candidates = list(
        manifest_root.rglob(
            "manifest.json"
        )
    )

    if not candidates:
        raise ProcessingManifestError(
            "No completed manifest found "
            f"for endpoint: {endpoint.value}"
        )

    return max(
        candidates,
        key=lambda path: (
            path.stat().st_mtime_ns
        ),
    )