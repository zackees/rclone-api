from queue import Queue
from typing import Generator

from rclone_api import Dir
from rclone_api.dir_listing import DirListing
from rclone_api.remote import Remote


def walk(
    dir: Dir | Remote, max_depth: int = -1, check=True
) -> Generator[DirListing, None, None]:
    """Walk through the given directory recursively.

    Args:
        dir: Directory or Remote to walk through
        max_depth: Maximum depth to traverse (-1 for unlimited)

    Yields:
        DirListing: Directory listing for each directory encountered
    """
    # Convert Remote to Dir if needed
    if isinstance(dir, Remote):
        dir = Dir(dir)

    queue: Queue[Dir] = Queue()
    queue.put(dir)

    while not queue.empty():
        current_dir = queue.get()
        dirlisting = current_dir.ls()
        yield dirlisting
        dirs = dirlisting.dirs

        if max_depth != 0 and len(dirs) > 0:
            for child in dirs:
                queue.put(child)
        if max_depth < 0:
            continue
        if max_depth > 0:
            max_depth -= 1
