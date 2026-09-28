from enum import StrEnum


class DataProvider(StrEnum):
    BIPAD = "bipad"


class OriginalDataSource(StrEnum):
    DHM = "dhm"
    ICIMOD = "icimod"
    UNKNOWN = "unknown"