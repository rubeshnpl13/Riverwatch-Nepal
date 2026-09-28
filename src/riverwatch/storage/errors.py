class StorageError(Exception):
    """Base exception for RiverWatch storage failures."""


class InvalidObjectKeyError(StorageError):
    """Raised when an object key cannot be constructed safely."""


class ObjectAlreadyExistsError(StorageError):
    """Raised when a create-only object already exists."""