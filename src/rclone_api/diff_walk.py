from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Thread
from typing import Generator

from rclone_api import Dir
from rclone_api.dir_listing import DirListing
from rclone_api.types import ListingOption
from rclone_api.walk import walk_runner_depth_first

_MAX_OUT_QUEUE_SIZE = 50


# ONLY Works from src -> dst diffing.
def _async_diff_dir_walk_task(
    src: Dir, dst: Dir, max_depth: int, out_queue: Queue[Dir | None], reverse=False
) -> None:

    try:
        stack = [(src, dst)]
        while stack:
            curr_src, curr_dst = stack.pop()
            curr_src = curr_src
            curr_dst = curr_dst

            with ThreadPoolExecutor(max_workers=2) as executor:
                # src_dir_listing = src.ls(listing_option=ListingOption.DIRS_ONLY)
                # dst_dir_listing = dst.ls(listing_option=ListingOption.DIRS_ONLY)
                t1 = executor.submit(
                    src.ls, listing_option=ListingOption.DIRS_ONLY, reverse=reverse
                )
                t2 = executor.submit(
                    dst.ls, listing_option=ListingOption.DIRS_ONLY, reverse=reverse
                )
                src_dir_listing: DirListing = t1.result()
                dst_dir_listing: DirListing = t2.result()

            # dirlisting = current_dir.ls()
            # if reverse:
            #     dirlisting.dirs.reverse()
            # if depth != 0:
            #     for subdir in dirlisting.dirs:  # Process deeper directories first
            #         # stack.append((child, depth - 1 if depth > 0 else depth))
            #         next_depth = depth - 1 if depth > 0 else depth
            #         _walk_runner_depth_first(
            #             subdir, next_depth, out_queue, reverse=reverse
            #         )
            # out_queue.put(dirlisting)

            # for subdir in dst_dir_listing.dirs:
            #     subdir.to_string(include_remote=False)
            #     walk_runner_depth_first()

            # find elements missing on dst
            # missing_on_dst: set[Dir] = set(src_dir_listing.dirs) - set(
            #     dst_dir_listing.dirs
            # )
            # exists_on_dst: set[Dir] = set(src_dir_listing.dirs) - missing_on_dst

            dst_files: list[str] = [d.name for d in dst_dir_listing.dirs]
            src_files: list[str] = [d.name for d in src_dir_listing.dirs]

            dst_files_set: set[str] = set(dst_files)
            # src_files_set: set[str] = set(src_files)

            # print(f"src_files: {src_files}")
            # print(f"dst_files: {dst_files}")

            matching_dirs: list[str] = []

            for file in src_files:
                if file not in dst_files_set:
                    # print(f"missing dir on src: {file}")
                    queue_dir_listing: Queue[DirListing | None] = Queue()
                    walk_runner_depth_first(
                        dir=curr_src,
                        max_depth=max_depth,
                        out_queue=queue_dir_listing,
                        reverse=reverse,
                    )
                    while dirlisting := queue_dir_listing.get():
                        if dirlisting is None:
                            break
                        # print(f"dirlisting: {dirlisting}")
                        for d in dirlisting.dirs:
                            out_queue.put(d)
                else:
                    matching_dirs.append(file)

            for matching_dir in matching_dirs:
                # print(f"matching dir: {matching_dir}")
                _async_diff_dir_walk_task(
                    src=curr_src / matching_dir,
                    dst=curr_dst / matching_dir,
                    max_depth=max_depth,
                    out_queue=out_queue,
                    reverse=reverse,
                )

        out_queue.put(None)
    except KeyboardInterrupt:
        import _thread

        out_queue.put(None)
        _thread.interrupt_main()


def diff_walk(
    src: Dir,
    dst: Dir,
    max_depth: int = -1,
    reverse: bool = False,
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
            _async_diff_dir_walk_task(src, dst, max_depth, out_queue, reverse=reverse)

        worker = Thread(
            target=task,
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
