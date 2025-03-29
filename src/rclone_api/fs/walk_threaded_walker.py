from dataclasses import dataclass
from typing import Any


@dataclass
class Walker:
    """Threaded"""

    fspath: Any
    max_backlog: int = 8

    def __enter__(self):
        from rclone_api.fs.walk_threaded import _OSWalkThread

        self.walker = _OSWalkThread(self.fspath, self.max_backlog)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.walker.stop_event.set()
        self.walker.join()

    def walk(self):
        return self.walker.get_results()
