# from queue import Queue
# from threading import Thread
# from typing import Generator

# from rclone_api import Dir
# from rclone_api.dir_listing import DirListing
# from rclone_api.remote import Remote

# _MAX_OUT_QUEUE_SIZE = 50


# def diff_walk(
#     src: Dir | Remote,
#     dir: Dir | Remote,
#     reverse: bool = False,
# ) -> Generator[DirListing, None, None]:
#     """Walk through the given directory recursively.

#     Args:
#         dir: Directory or Remote to walk through
#         max_depth: Maximum depth to traverse (-1 for unlimited)

#     Yields:
#         DirListing: Directory listing for each directory encountered
#     """
#     try:
#         # Convert Remote to Dir if needed
#         if isinstance(dir, Remote):
#             dir = Dir(dir)
#         out_queue: Queue[DirListing | None] = Queue(maxsize=_MAX_OUT_QUEUE_SIZE)

#         src_listing = src.ls()


#         # Start worker thread
#         worker = Thread(
#             target=_task,
#             daemon=True,
#         )
#         worker.start()

#         while dirlisting := out_queue.get():
#             if dirlisting is None:
#                 break
#             yield dirlisting

#         worker.join()
#     except KeyboardInterrupt:
#         pass
