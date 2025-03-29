from dataclasses import dataclass
from typing import Any


@dataclass
class FSWalker:
    """Threaded"""

    fspath: Any
    max_backlog: int

    def __enter__(self):
        from rclone_api.fs.filesystem import FSPath
        from rclone_api.fs.walk_threaded import FSWalkThread

        assert isinstance(
            self.fspath, FSPath
        ), f"Expected FSPath, got {type(self.fspath)}"
        self.walker = FSWalkThread(self.fspath, self.max_backlog)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.walker.stop_event.set()
        self.walker.join()

    def __iter__(self):
        return self.walk()

    def walk(self):
        return self.walker.get_results()
