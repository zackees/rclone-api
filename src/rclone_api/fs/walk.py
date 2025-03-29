from pathlib import Path
from queue import Queue
from threading import Event, Thread
from typing import Generator, List, Optional, Tuple

from rclone_api.fs.filesystem import FSPath, logger


def os_walk(
    self: FSPath,
) -> Generator[tuple[FSPath, list[str], list[str]], None, None]:
    root_path = self
    stack: list[str] = [self.path]

    while stack:
        curr_path = stack.pop()
        curr_suffix_path = Path(curr_path).relative_to(Path(root_path.path)).as_posix()
        current_dir = root_path / curr_suffix_path
        try:
            filenames, dirnames = current_dir.ls()
        except Exception as e:
            logger.warning(f"Unable to list directory {current_dir}: {e}")
            continue

        yield current_dir, dirnames, filenames

        # Add subdirectories to stack for further traversal
        for dirname in reversed(dirnames):
            stack.append((current_dir / dirname).path)


class OSWalkThread:
    def __init__(self, fspath: FSPath, max_backlog: int = 8):
        self.fspath = fspath
        self.result_queue: Queue[Optional[Tuple[FSPath, List[str], List[str]]]] = Queue(
            maxsize=max_backlog
        )
        self.thread = Thread(target=self.worker)
        self.stop_event = Event()

    def worker(self):
        for root, dirnames, filenames in os_walk(self.fspath):
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


def os_walk_threaded(
    self: FSPath, max_backlog: int = 8
) -> Generator[tuple[FSPath, list[str], list[str]], None, None]:
    with OSWalkThread(self, max_backlog) as walker:
        yield from walker.get_results()
