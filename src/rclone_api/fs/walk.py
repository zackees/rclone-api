from pathlib import Path
from queue import Queue
from threading import Thread
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


def os_walk_threaded(
    self: FSPath, max_backlog: int = 8
) -> Generator[tuple[FSPath, list[str], list[str]], None, None]:
    result_queue: Queue[Optional[Tuple[FSPath, List[str], List[str]]]] = Queue(
        maxsize=max_backlog
    )

    def worker():
        for root, dirnames, filenames in os_walk(self):
            result_queue.put((root, dirnames, filenames))
        result_queue.put(None)  # Sentinel value to indicate completion

    # Start the worker thread
    thread = Thread(target=worker)
    thread.start()

    # Yield results from the queue
    while True:
        result = result_queue.get()
        if result is None:  # Check for sentinel value
            break
        yield result

    # Ensure the thread has finished
    thread.join()
