import re
from dataclasses import dataclass
from enum import Enum


class ModTimeStrategy(Enum):
    USE_SERVER_MODTIME = "use-server-modtime"
    NO_MODTIME = "no-modtime"


class ListingOption(Enum):
    DIRS_ONLY = "dirs-only"
    FILES_ONLY = "files-only"
    ALL = "all"


class Order(Enum):
    NORMAL = "normal"
    REVERSE = "reverse"
    RANDOM = "random"


@dataclass
class S3PathInfo:
    remote: str
    bucket: str
    key: str


@dataclass
class SizeResult:
    """Size result dataclass."""

    prefix: str
    total_size: int
    file_sizes: dict[str, int]


def _to_size_suffix(size: int) -> str:
    if size < 1024:
        return f"{size}B"
    elif size < 1024**2:
        val = size / 1024
        unit = "K"
    elif size < 1024**3:
        val = size / (1024**2)
        unit = "M"
    elif size < 1024**4:
        val = size / (1024**3)
        unit = "G"
    elif size < 1024**5:
        val = size / (1024**4)
        unit = "T"
    elif size < 1024**6:
        val = size / (1024**5)
        unit = "P"
    else:
        raise ValueError(f"Invalid size: {size}")

    # If the float is an integer, drop the decimal, otherwise format with one decimal.
    return f"{int(val) if val.is_integer() else f'{val:.1f}'}{unit}"


# Update regex to allow decimals (e.g., 16.5MB)
_PATTERN_SIZE_SUFFIX = re.compile(r"^(\d+(?:\.\d+)?)([A-Za-z]+)$")


def _from_size_suffix(size: str) -> int:
    if size == "0":
        return 0
    match = _PATTERN_SIZE_SUFFIX.match(size)
    if match is None:
        raise ValueError(f"Invalid size suffix: {size}")
    num_str, suffix = match.group(1), match.group(2)
    n = float(num_str)
    # Determine the unit from the first letter (e.g., "M" from "MB")
    unit = suffix[0].upper()
    if unit == "B":
        return int(n)
    if unit == "K":
        return int(n * 1024)
    if unit == "M":
        return int(n * 1024 * 1024)
    if unit == "G":
        return int(n * 1024 * 1024 * 1024)
    if unit == "T":
        return int(n * 1024**4)
    if unit == "P":
        return int(n * 1024**5)
    raise ValueError(f"Invalid size suffix: {suffix}")


class SizeSuffix:
    def __init__(self, size: "int | str | SizeSuffix"):
        self._size: int
        if isinstance(size, SizeSuffix):
            self._size = size._size
        elif isinstance(size, int):
            self._size = size
        elif isinstance(size, str):
            self._size = _from_size_suffix(size)
        elif isinstance(size, float):
            self._size = int(size)
        else:
            raise ValueError(f"Invalid type for size: {type(size)}")

    def as_int(self) -> int:
        return self._size

    def as_str(self) -> str:
        return _to_size_suffix(self._size)

    def __repr__(self) -> str:
        return self.as_str()

    def __str__(self) -> str:
        return self.as_str()

    @staticmethod
    def _to_size(size: "int | SizeSuffix") -> int:
        if isinstance(size, int):
            return size
        elif isinstance(size, SizeSuffix):
            return size._size
        else:
            raise ValueError(f"Invalid type for size: {type(size)}")

    def __mul__(self, other: "int | SizeSuffix") -> "SizeSuffix":
        other_int = SizeSuffix(other)
        return SizeSuffix(self._size * other_int._size)

    def __add__(self, other: "int | SizeSuffix") -> "SizeSuffix":
        other_int = SizeSuffix(other)
        return SizeSuffix(self._size + other_int._size)

    def __sub__(self, other: "int | SizeSuffix") -> "SizeSuffix":
        other_int = SizeSuffix(other)
        return SizeSuffix(self._size - other_int._size)

    def __truediv__(self, other: "int | SizeSuffix") -> "SizeSuffix":
        other_int = SizeSuffix(other)
        if other_int._size == 0:
            raise ZeroDivisionError("Division by zero is undefined")
        # Use floor division to maintain integer arithmetic.
        return SizeSuffix(self._size // other_int._size)
