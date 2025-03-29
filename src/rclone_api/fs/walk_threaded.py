from queue import Queue
from threading import Event, Thread
from typing import Generator, List, Optional, Tuple

from rclone_api.fs.filesystem import FSPath
from rclone_api.fs.walk_threaded_walker import FSWalker

### API


def os_walk_threaded_begin(self: FSPath, max_backlog: int = 8) -> FSWalker:
    return FSWalker(self, max_backlog)


### Implementation


class FSWalkThread:
    def __init__(self, fspath: FSPath, max_backlog: int = 8):
        self.fspath = fspath
        self.result_queue: Queue[Optional[Tuple[FSPath, List[str], List[str]]]] = Queue(
            maxsize=max_backlog
        )
        self.thread = Thread(target=self.worker, daemon=True)
        self.stop_event = Event()
        self.start()

    def worker(self):
        from rclone_api.fs.walk import fs_walk

        for root, dirnames, filenames in fs_walk(self.fspath):
            if self.stop_event.is_set():
                break
            self.result_queue.put((root, dirnames, filenames))
        self.result_queue.put(None)  # Sentinel value to indicate completion

    def start(self):
        self.thread.start()

    def join(self):
        self.thread.join()

    def get_results(self) -> Generator[Tuple[FSPath, List[str], List[str]], None, None]:
        while True:
            result = self.result_queue.get()
            if result is None:  # Check for sentinel value
                break
            yield result

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop_event.set()
        self.join()
