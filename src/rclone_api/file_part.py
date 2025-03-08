import atexit
import os
import time
import warnings
from pathlib import Path
from threading import Lock
from typing import Any

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


# dbg_thread = threading.Thread(target=run_debug_parts)
# dbg_thread.start()


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
        from rclone_api.types import SizeSuffix

        payload_str = "err" if self.is_error() else f"{SizeSuffix(self.n_bytes())}"
        return f"FilePart(payload={payload_str}, extra={self.extra})"
