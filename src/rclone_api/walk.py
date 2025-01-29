from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
from typing import Generator

from rclone_api import Dir
from rclone_api.dir_listing import DirListing
from rclone_api.remote import Remote


def walk(
    dir: Dir | Remote, max_depth: int = -1, max_workers: int = 4
) -> Generator[DirListing, None, None]:
    """Walk through the given directory recursively.

    Args:
        dir: Directory or Remote to walk through
        max_depth: Maximum depth to traverse (-1 for unlimited)
        max_workers: Maximum number of concurrent workers

    Yields:
        DirListing: Directory listing for each directory encountered
    """
    # Convert Remote to Dir if needed
    if isinstance(dir, Remote):
        dir = Dir(dir)
    pending: Queue[tuple[Dir | None, int]] = Queue()
    results: Queue[DirListing | Exception] = Queue()

    def worker():
        while True:
            try:
                # Add timeout to allow checking for sentinel value
                try:
                    current_dir, depth = pending.get(timeout=0.1)
                except Empty:
                    continue

                # Check for sentinel value
                if current_dir is None:
                    pending.task_done()
                    break

                listing = current_dir.ls()
                results.put(listing)

                if max_depth == -1 or depth < max_depth:
                    for d in listing.dirs:
                        pending.put((d, depth + 1))

                pending.task_done()
            except Exception as e:
                results.put(e)
                pending.task_done()
                break
        return None

    # Start workers
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        workers = [executor.submit(worker) for _ in range(max_workers)]

        # Start walking
        pending.put((dir, 0))

        # Process results while workers are running
        completed = 0
        while completed < max_workers:
            try:
                result = results.get(timeout=0.1)
                if isinstance(result, Exception):
                    # Propagate exception
                    raise result
                yield result
            except Empty:
                # Check if any workers have completed
                completed = sum(1 for w in workers if w.done())
                continue

        # Signal workers to stop
        for _ in range(max_workers):
            pending.put((None, 0))

        # Drain any remaining results
        while not results.empty():
            result = results.get()
            if isinstance(result, Exception):
                raise result
            yield result
