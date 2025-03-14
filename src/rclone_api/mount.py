from dataclasses import dataclass
from pathlib import Path

from rclone_api.process import Process


@dataclass
class Mount:
    """Mount information."""

    src: str
    mount_path: Path
    process: Process
    read_only: bool
    cache_dir: Path | None = None
    cache_dir_delete_on_exit: bool | None = None
    _closed: bool = False

    def __post_init__(self):
        from rclone_api.mount_util import add_mount_for_gc, wait_for_mount

        assert isinstance(self.mount_path, Path)
        assert self.process is not None
        wait_for_mount(self)
        add_mount_for_gc(self)

    def close(self, wait=True) -> None:
        """Clean up the mount."""
        from rclone_api.mount_util import (
            cache_dir_delete_on_exit,
            clean_mount,
            remove_mount_for_gc,
        )

        if self._closed:
            return
        self._closed = True
        self.process.terminate()
        clean_mount(self, verbose=False, wait=wait)
        if self.cache_dir and self.cache_dir_delete_on_exit:
            cache_dir_delete_on_exit(self.cache_dir)
        remove_mount_for_gc(self)

    def __enter__(self) -> "Mount":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close(wait=True)

    def __del__(self):
        self.close(wait=False)

    # make this a hashable object
    def __hash__(self):
        return hash(self.mount_path)
