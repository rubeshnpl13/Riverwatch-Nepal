class ProcessingError(Exception):
    """Base error for RiverWatch processing."""


class ProcessedOutputAlreadyExistsError(
    ProcessingError
):
    """Raised when immutable processed output already exists."""