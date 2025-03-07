import atexit
import os
import re
import threading
import time
import warnings
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Any


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
    def _convert(size: int) -> tuple[float, str]:
        val: float
        unit: str
        if size < 1024:
            val = size
            unit = "B"
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

        return val, unit

    def _fmt(_val: float | int, _unit: str) -> str:
        # If the float is an integer, drop the decimal, otherwise format with one decimal.
        val_str: str = str(_val)
        if not val_str.endswith(".0"):
            first_str: str = f"{_val:.1f}"
        else:
            first_str = str(int(_val))
        return first_str + _unit

    val, unit = _convert(size)
    out = _fmt(val, unit)
    # Now round trip the value to fix floating point issues via rounding.
    int_val = _from_size_suffix(out)
    val, unit = _convert(int_val)
    out = _fmt(val, unit)
    return out


# Update regex to allow decimals (e.g., 16.5MB)
_PATTERN_SIZE_SUFFIX = re.compile(r"^(\d+(?:\.\d+)?)([A-Za-z]+)$")


def _parse_elements(value: str) -> tuple[str, str] | None:
    match = _PATTERN_SIZE_SUFFIX.match(value)
    if match is None:
        return None
    return match.group(1), match.group(2)


def _from_size_suffix(size: str) -> int:
    if size == "0":
        return 0
    pair = _parse_elements(size)
    if pair is None:
        raise ValueError(f"Invalid size suffix: {size}")
    num_str, suffix = pair
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

    # support / division
    def __floordiv__(self, other: "int | SizeSuffix") -> "SizeSuffix":
        other_int = SizeSuffix(other)
        if other_int._size == 0:
            raise ZeroDivisionError("Division by zero is undefined")
        # Use floor division to maintain integer arithmetic.
        return SizeSuffix(self._size // other_int._size)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SizeSuffix):
            return False
        return self._size == other._size

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, SizeSuffix):
            return True
        return self._size != other._size

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SizeSuffix):
            return False
        return self._size < other._size

    def __le__(self, other: object) -> bool:
        if not isinstance(other, SizeSuffix):
            return False
        return self._size <= other._size

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, SizeSuffix):
            return False
        return self._size > other._size

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, SizeSuffix):
            return False
        return self._size >= other._size

    def __hash__(self) -> int:
        return hash(self._size)

    def __int__(self) -> int:
        return self._size


_TMP_DIR_ACCESS_LOCK = Lock()


def _clean_old_files(out: Path) -> None:
    # clean up files older than 1 day
    from rclone_api.util import locked_print

    now = time.time()
    # Erase all stale files and then purge empty directories.
    for root, dirs, files in os.walk(out):
        for name in files:
            f = Path(root) / name
            filemod = f.stat().st_mtime
            diff_secs = now - filemod
            diff_days = diff_secs / (60 * 60 * 24)
            if diff_days > 1:
                locked_print(f"Removing old file: {f}")
                f.unlink()

    for root, dirs, _ in os.walk(out):
        for dir in dirs:
            d = Path(root) / dir
            if not list(d.iterdir()):
                locked_print(f"Removing empty directory: {d}")
                d.rmdir()


def get_chunk_tmpdir() -> Path:
    with _TMP_DIR_ACCESS_LOCK:
        dat = get_chunk_tmpdir.__dict__
        if "out" in dat:
            return dat["out"]  # Folder already validated.
        out = Path("chunk_store")
        if out.exists():
            # first access, clean up directory
            _clean_old_files(out)
        out.mkdir(exist_ok=True, parents=True)
        dat["out"] = out
        return out


class EndOfStream:
    pass


_CLEANUP_LIST: list[Path] = []


def _add_for_cleanup(path: Path) -> None:
    _CLEANUP_LIST.append(path)


def _on_exit_cleanup() -> None:
    paths = list(_CLEANUP_LIST)
    for path in paths:
        try:
            if path.exists():
                path.unlink()
        except Exception as e:
            warnings.warn(f"Cannot cleanup {path}: {e}")


atexit.register(_on_exit_cleanup)


_FILEPARTS: list["FilePart"] = []

_FILEPARTS_LOCK = Lock()


def _add_filepart(part: "FilePart") -> None:
    with _FILEPARTS_LOCK:
        if part not in _FILEPARTS:
            _FILEPARTS.append(part)


def _remove_filepart(part: "FilePart") -> None:
    with _FILEPARTS_LOCK:
        if part in _FILEPARTS:
            _FILEPARTS.remove(part)


def run_debug_parts():
    while True:
        print("\nAlive file parts:")
        for part in list(_FILEPARTS):
            print(part)
            # print(part.stacktrace)
        print("\n\n")
        time.sleep(60)


dbg_thread = threading.Thread(target=run_debug_parts)
dbg_thread.start()


class FilePart:
    def __init__(self, payload: Path | bytes | Exception, extra: Any) -> None:
        import traceback

        from rclone_api.util import random_str

        stacktrace = traceback.format_stack()
        stacktrace_str = "".join(stacktrace)
        self.stacktrace = stacktrace_str
        # _FILEPARTS.append(self)
        _add_filepart(self)

        self.extra = extra
        self._lock = Lock()
        self.payload: Path | Exception
        if isinstance(payload, Exception):
            self.payload = payload
            return
        if isinstance(payload, bytes):
            print(f"Creating file part with payload: {len(payload)}")
            self.payload = get_chunk_tmpdir() / f"{random_str(12)}.chunk"
            with _TMP_DIR_ACCESS_LOCK:
                if not self.payload.parent.exists():
                    self.payload.parent.mkdir(parents=True, exist_ok=True)
                self.payload.write_bytes(payload)
            _add_for_cleanup(self.payload)
        if isinstance(payload, Path):
            print("Adopting payload: ", payload)
            self.payload = payload
            _add_for_cleanup(self.payload)

    def get_file(self) -> Path | Exception:
        return self.payload

    @property
    def size(self) -> int:
        with self._lock:
            if isinstance(self.payload, Path):
                return self.payload.stat().st_size
            return -1

    def n_bytes(self) -> int:
        with self._lock:
            if isinstance(self.payload, Path):
                return self.payload.stat().st_size
            return -1

    def load(self) -> bytes:
        with self._lock:
            if isinstance(self.payload, Path):
                with open(self.payload, "rb") as f:
                    return f.read()
            raise ValueError("Cannot load from error")

    def __post_init__(self):
        if isinstance(self.payload, Path):
            assert self.payload.exists(), f"File part {self.payload} does not exist"
            assert self.payload.is_file(), f"File part {self.payload} is not a file"
            assert self.payload.stat().st_size > 0, f"File part {self.payload} is empty"
        elif isinstance(self.payload, Exception):
            warnings.warn(f"File part error: {self.payload}")
        print(f"File part created with payload: {self.payload}")

    def is_error(self) -> bool:
        return isinstance(self.payload, Exception)

    def dispose(self) -> None:
        # _FILEPARTS.remove(self)
        _remove_filepart(self)
        print("Disposing file part")
        with self._lock:
            if isinstance(self.payload, Exception):
                warnings.warn(
                    f"Cannot close file part because the payload represents an error: {self.payload}"
                )
                print("Cannot close file part because the payload represents an error")
                return
            if self.payload.exists():
                print(f"File part {self.payload} exists")
                try:
                    print(f"Unlinking file part {self.payload}")
                    self.payload.unlink()
                    print(f"File part {self.payload} deleted")
                except Exception as e:
                    warnings.warn(f"Cannot close file part because of error: {e}")
            else:
                warnings.warn(
                    f"Cannot close file part because it does not exist: {self.payload}"
                )

    def __del__(self):
        self.dispose()

    def __repr__(self):
        payload_str = "err" if self.is_error() else f"{SizeSuffix(self.n_bytes())}"
        return f"FilePart(payload={payload_str}, extra={self.extra})"
