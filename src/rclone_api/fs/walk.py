from collections import OrderedDict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Generator

from rclone_api.fs.filesystem import FSPath, logger

# module‐wide executor
_executor = ThreadPoolExecutor(max_workers=16)


def fs_walk_parallel(
    self: FSPath,
) -> Generator[tuple[FSPath, list[str], list[str]], None, None]:
    """
    Parallel version of fs_walk: walks `self` and lists
    up to 16 directories at once using the global executor,
    but yields results in the same order tasks were submitted.
    """
    root = self

    def _list_dir(path: FSPath):
        try:
            filenames, dirnames = path.ls()
        except Exception as e:
            logger.warning(f"Unable to list directory {path}: {e}")
            return None
        return path, dirnames, filenames

    # use an OrderedDict to remember submission order
    futures: OrderedDict = OrderedDict()
    # submit the root first
    futures[_executor.submit(_list_dir, root)] = root

    while futures:
        # wait until at least one of them finishes
        done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)

        # iterate through our futures *in submission order*
        for fut in list(futures.keys()):
            if fut not in done:
                continue

            _ = futures.pop(fut)  # remove it in order
            result = fut.result()
            if result is None:
                continue  # error already logged

            current_dir, dirnames, filenames = result
            yield current_dir, dirnames, filenames

            # now schedule its children (they go at the end of our OrderedDict)
            for dirname in dirnames:
                sub = current_dir / dirname
                futures[_executor.submit(_list_dir, sub)] = sub


def fs_walk(self: FSPath) -> Generator[tuple[FSPath, list[str], list[str]], None, None]:
    """Sequential API, now backed by the global-thread-pool parallel implementation."""
    yield from fs_walk_parallel(self)
