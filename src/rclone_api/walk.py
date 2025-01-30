from queue import Queue
from threading import Thread
from typing import Generator

from rclone_api import Dir
from rclone_api.dir_listing import DirListing
from rclone_api.remote import Remote

_MAX_OUT_QUEUE_SIZE = 50


def _walk_runner(
    queue: Queue[Dir], max_depth: int, out_queue: Queue[DirListing | None]
) -> None:
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


def walk(dir: Dir | Remote, max_depth: int = -1) -> Generator[DirListing, None, None]:
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

        in_queue: Queue[Dir] = Queue()
        out_queue: Queue[DirListing] = Queue(maxsize=_MAX_OUT_QUEUE_SIZE)
        in_queue.put(dir)

        # Start worker thread
        worker = Thread(
            target=_walk_runner, args=(in_queue, max_depth, out_queue), daemon=True
        )
        worker.start()

        while dirlisting := out_queue.get():
            if dirlisting is None:
                break
            yield dirlisting

        worker.join()
    except KeyboardInterrupt:
        pass
