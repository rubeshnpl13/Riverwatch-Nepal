from collections.abc import Mapping
from pathlib import Path

from riverwatch.storage.errors import (
    InvalidObjectKeyError,
    ObjectAlreadyExistsError,
    StorageError,
)
from riverwatch.storage.models import StoredObject
from riverwatch.storage.serialization import (
    sha256_hex,
)


class LocalObjectStore:
    """Create-only object store backed by the local filesystem."""

    def __init__(
        self,
        *,
        root: str | Path,
    ) -> None:
        root_path = Path(
            root
        ).expanduser()

        try:
            root_path.mkdir(
                parents=True,
                exist_ok=True,
            )
        except OSError as exc:
            raise StorageError(
                f"Unable to create storage root: "
                f"{root_path}"
            ) from exc

        if not root_path.is_dir():
            raise StorageError(
                f"Storage root is not a directory: "
                f"{root_path}"
            )

        self._root = root_path.resolve()

    @property
    def root(self) -> Path:
        return self._root

    def _resolve_key(
        self,
        key: str,
    ) -> Path:
        cleaned = key.strip()

        if not cleaned:
            raise InvalidObjectKeyError(
                "Object key cannot be empty"
            )

        if cleaned != key:
            raise InvalidObjectKeyError(
                "Object key cannot have leading "
                "or trailing whitespace"
            )

        if "\\" in cleaned:
            raise InvalidObjectKeyError(
                "Object key cannot contain '\\'"
            )

        key_path = Path(
            cleaned
        )

        if key_path.is_absolute():
            raise InvalidObjectKeyError(
                "Object key cannot be absolute"
            )

        parts = cleaned.split(
            "/"
        )

        if any(
            part in {
                "",
                ".",
                "..",
            }
            for part in parts
        ):
            raise InvalidObjectKeyError(
                f"Invalid object key: {key}"
            )

        candidate = (
            self._root.joinpath(
                *parts
            )
        ).resolve()

        try:
            candidate.relative_to(
                self._root
            )
        except ValueError as exc:
            raise InvalidObjectKeyError(
                "Object key escapes storage root"
            ) from exc

        return candidate

    def create_bytes(
        self,
        *,
        key: str,
        data: bytes,
        content_type: str,
        metadata: Mapping[str, str] | None = None,
    ) -> StoredObject:
        if not content_type.strip():
            raise ValueError(
                "content_type cannot be empty"
            )

        target = self._resolve_key(
            key
        )

        try:
            target.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            # "xb" means:
            #
            # x -> create exclusively; fail if it exists
            # b -> binary mode
            #
            # This gives our raw layer create-only
            # semantics rather than silently overwriting.
            with target.open(
                "xb"
            ) as file:
                file.write(
                    data
                )

        except FileExistsError as exc:
            raise ObjectAlreadyExistsError(
                f"Object already exists: {key}"
            ) from exc

        except OSError as exc:
            raise StorageError(
                f"Unable to create object: {key}"
            ) from exc

        # Local filesystems do not have a portable equivalent
        # of GCS custom object metadata. The parameter is
        # accepted so this backend conforms to ObjectStore.
        #
        # RiverWatch's logical ingestion metadata will be
        # persisted separately as metadata.json objects.
        _ = metadata

        return StoredObject(
            key=key,
            size_bytes=len(
                data
            ),
            content_type=content_type,
            sha256=sha256_hex(
                data
            ),
        )