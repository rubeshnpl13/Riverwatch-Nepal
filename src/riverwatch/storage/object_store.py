from collections.abc import Mapping
from typing import Protocol

from riverwatch.storage.models import (
    StoredObject,
)


class ObjectStore(Protocol):
    def create_bytes(
        self,
        *,
        key: str,
        data: bytes,
        content_type: str,
        metadata: Mapping[str, str] | None = None,
    ) -> StoredObject:
        """
        Create a new immutable object.

        Implementations must not silently overwrite an
        existing object with the same key.
        """
        ...