from queue import Queue
from threading import Thread
from typing import Generator

from rclone_api import Dir
from rclone_api.dir_listing import DirListing
from rclone_api.remote import Remote

_MAX_OUT_QUEUE_SIZE = 50


def _walk_runner_breadth_first(
    dir: Dir, max_depth: int, out_queue: Queue[DirListing | None]
) -> None:
    queue: Queue[Dir] = Queue()
    queue.put(dir)
    try:
        while not queue.empty():
            current_dir = queue.get()
            dirlisting = current_dir.ls()
            out_queue.put(dirlisting)
            dirs = dirlisting.dirs

            if max_depth != 0 and len(dirs) > 0:
                for child in dirs:
                    queue.put(child)
            if max_depth < 0:
                continue
            if max_depth > 0:
                max_depth -= 1
        out_queue.put(None)
    except KeyboardInterrupt:
        import _thread

        out_queue.put(None)

        _thread.interrupt_main()


def _walk_runner_depth_first(
    dir: Dir, max_depth: int, out_queue: Queue[DirListing | None]
) -> None:
    try:
        stack = [(dir, max_depth)]
        while stack:
            current_dir, depth = stack.pop()
            dirlisting = current_dir.ls()
            if depth != 0:
                for subdir in reversed(
                    dirlisting.dirs
                ):  # Process deeper directories first
                    # stack.append((child, depth - 1 if depth > 0 else depth))
                    next_depth = depth - 1 if depth > 0 else depth
                    _walk_runner_depth_first(subdir, next_depth, out_queue)
            out_queue.put(dirlisting)
        out_queue.put(None)
    except KeyboardInterrupt:
        import _thread

        out_queue.put(None)
        _thread.interrupt_main()


def walk(
    dir: Dir | Remote,
    breadth_first: bool,
    max_depth: int = -1,
) -> Generator[DirListing, None, None]:
    """Walk through the given directory recursively.

    Args:
        dir: Directory or Remote to walk through
        max_depth: Maximum depth to traverse (-1 for unlimited)

    Yields:
        DirListing: Directory listing for each directory encountered
    """
    try:
        # Convert Remote to Dir if needed
        if isinstance(dir, Remote):
            dir = Dir(dir)
        out_queue: Queue[DirListing] = Queue(maxsize=_MAX_OUT_QUEUE_SIZE)

        strategy = (
            _walk_runner_breadth_first if breadth_first else _walk_runner_depth_first
        )

        # Start worker thread
        worker = Thread(
            target=strategy,
            args=(dir, max_depth, out_queue),
            daemon=True,
        )
        worker.start()

        while dirlisting := out_queue.get():
            if dirlisting is None:
                break
            yield dirlisting

        worker.join()
    except KeyboardInterrupt:
        pass
