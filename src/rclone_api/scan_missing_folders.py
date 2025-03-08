import random
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
from threading import Thread
from typing import Generator

from rclone_api import Dir
from rclone_api.detail.walk import walk_runner_depth_first
from rclone_api.dir_listing import DirListing
from rclone_api.types import ListingOption, Order

_MAX_OUT_QUEUE_SIZE = 50


def _reorder_inplace(data: list, order: Order) -> None:
    if order == Order.NORMAL:
        return
    elif order == Order.REVERSE:
        data.reverse()
        return
    elif order == Order.RANDOM:
        random.shuffle(data)
        return
    else:
        raise ValueError(f"Invalid order: {order}")


# ONLY Works from src -> dst diffing.
def _async_diff_dir_walk_task(
    src: Dir, dst: Dir, max_depth: int, out_queue: Queue[Dir | None], order: Order
) -> None:
    can_scan_two_deep = max_depth > 1 or max_depth == -1
    ls_depth = 2 if can_scan_two_deep else 1
    with ThreadPoolExecutor(max_workers=2) as executor:
        t1 = executor.submit(
            src.ls,
            listing_option=ListingOption.DIRS_ONLY,
            order=order,
            max_depth=ls_depth,
        )
        t2 = executor.submit(
            dst.ls,
            listing_option=ListingOption.DIRS_ONLY,
            order=order,
            max_depth=ls_depth,
        )
        src_dir_listing: DirListing = t1.result()
        dst_dir_listing: DirListing = t2.result()
    next_depth = max_depth - ls_depth if max_depth > 0 else max_depth
    dst_dirs: list[str] = [d.relative_to(src) for d in dst_dir_listing.dirs]
    src_dirs: list[str] = [d.relative_to(dst) for d in src_dir_listing.dirs]
    dst_files_set: set[str] = set(dst_dirs)
    matching_dirs: list[str] = []
    _reorder_inplace(src_dirs, order)
    _reorder_inplace(dst_dirs, order)
    for i, src_dir in enumerate(src_dirs):
        src_dir_dir = src / src_dir
        if src_dir not in dst_files_set:
            queue_dir_listing: Queue[DirListing | None] = Queue()
            if next_depth > 0 or next_depth == -1:
                walk_runner_depth_first(
                    dir=src_dir_dir,
                    out_queue=queue_dir_listing,
                    order=order,
                    max_depth=next_depth,
                )
                out_queue.put(src)
            while dirlisting := queue_dir_listing.get():
                if dirlisting is None:
                    break
                # print(f"dirlisting: {dirlisting}")
                for d in dirlisting.dirs:
                    out_queue.put(d)
        else:
            matching_dirs.append(src_dir)

    for matching_dir in matching_dirs:
        # print(f"matching dir: {matching_dir}")
        if next_depth > 0 or next_depth == -1:
            src_next = src / matching_dir
            dst_next = dst / matching_dir
            _async_diff_dir_walk_task(
                src=src_next,
                dst=dst_next,
                max_depth=next_depth,
                out_queue=out_queue,
                order=order,
            )


def async_diff_dir_walk_task(
    src: Dir, dst: Dir, max_depth: int, out_queue: Queue[Dir | None], order: Order
) -> None:
    try:
        _async_diff_dir_walk_task(
            src=src, dst=dst, max_depth=max_depth, out_queue=out_queue, order=order
        )
    except Exception:
        import _thread

        _thread.interrupt_main()
        raise
    finally:
        out_queue.put(None)


def scan_missing_folders(
    src: Dir,
    dst: Dir,
    max_depth: int = -1,
    order: Order = Order.NORMAL,
) -> Generator[Dir, None, None]:
    """Walk through the given directory recursively.

    Args:
        dir: Directory or Remote to walk through
        max_depth: Maximum depth to traverse (-1 for unlimited)

    Yields:
        DirListing: Directory listing for each directory encountered
    """

    try:
        out_queue: Queue[Dir | None] = Queue(maxsize=_MAX_OUT_QUEUE_SIZE)

        def task() -> None:
            async_diff_dir_walk_task(
                src=src,
                dst=dst,
                max_depth=max_depth,
                out_queue=out_queue,
                order=order,
            )

        worker = Thread(
            target=task,
            daemon=True,
        )
        worker.start()

        while True:
            try:
                dir = out_queue.get_nowait()
                if dir is None:
                    break
                yield dir
            except Empty:
                time.sleep(0.1)

        worker.join()
    except KeyboardInterrupt:
        pass
