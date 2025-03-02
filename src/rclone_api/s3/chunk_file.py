import time
import warnings
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from threading import Event
from typing import Any, Callable

from rclone_api.mount import FilePart
from rclone_api.s3.chunk_types import UploadState
from rclone_api.types import EndOfStream
from rclone_api.util import locked_print


def _get_file_size(file_path: Path, timeout: int = 60) -> int:
    sleep_time = timeout / 60 if timeout > 0 else 1
    start = time.time()
    while True:
        expired = time.time() - start > timeout
        try:
            time.sleep(sleep_time)
            if file_path.exists():
                return file_path.stat().st_size
        except FileNotFoundError as e:
            if expired:
                print(f"File not found: {file_path}, exception is {e}")
                raise
        if expired:
            raise TimeoutError(f"File {file_path} not found after {timeout} seconds")


@dataclass
class S3FileInfo:
    upload_id: str
    part_number: int


def file_chunker(
    upload_state: UploadState,
    fetcher: Callable[[int, int, Any], Future[FilePart]],
    max_chunks: int | None,
    cancel_signal: Event,
    queue_upload: Queue[FilePart | EndOfStream],
) -> None:
    count = 0

    def should_stop() -> bool:
        nonlocal count

        if max_chunks is None:
            return False
        if count >= max_chunks:
            print(
                f"Stopping file chunker after {count} chunks because it exceeded max_chunks {max_chunks}"
            )
            return True
        count += 1
        return False

    upload_info = upload_state.upload_info
    file_path = upload_info.src_file_path
    chunk_size = upload_info.chunk_size
    # src = Path(file_path)

    try:
        part_number = 1
        done_part_numbers: set[int] = {
            p.part_number for p in upload_state.parts if not isinstance(p, EndOfStream)
        }
        num_parts = upload_info.total_chunks()

        def next_part_number() -> int | None:
            nonlocal part_number
            while part_number in done_part_numbers:
                part_number += 1
            if part_number > num_parts:
                return None
            return part_number

        if cancel_signal.is_set():
            print(
                f"Cancel signal is set for file chunker while processing {file_path}, returning"
            )
            return

        while not should_stop():
            curr_part_number = next_part_number()
            if curr_part_number is None:
                locked_print(f"File {file_path} has completed chunking all parts")
                break
            assert curr_part_number is not None
            offset = (curr_part_number - 1) * chunk_size
            file_size = upload_info.file_size

            assert offset < file_size, f"Offset {offset} is greater than file size"

            # Open the file, seek, read the chunk, and close immediately.
            # with open(file_path, "rb") as f:
            #     f.seek(offset)
            #     data = f.read(chunk_size)

            # data = chunk_fetcher(offset, chunk_size).result()

            assert curr_part_number is not None
            cpn: int = curr_part_number

            def on_complete(fut: Future[FilePart]) -> None:
                fp: FilePart = fut.result()
                if fp.is_error():
                    warnings.warn(
                        f"Error reading file: {fp}, skipping part {part_number}"
                    )
                    return

                if fp.n_bytes() == 0:
                    warnings.warn(f"Empty data for part {part_number} of {file_path}")
                    raise ValueError(
                        f"Empty data for part {part_number} of {file_path}"
                    )

                if isinstance(fp.payload, Exception):
                    warnings.warn(f"Error reading file because of error: {fp.payload}")
                    return

                done_part_numbers.add(part_number)
                queue_upload.put(fp)

            offset = (curr_part_number - 1) * chunk_size
            fut = fetcher(offset, file_size, S3FileInfo(upload_info.upload_id, cpn))
            fut.add_done_callback(on_complete)
            # wait until the queue_upload queue can accept the next chunk
            while queue_upload.full():
                time.sleep(0.1)
    except Exception as e:

        warnings.warn(f"Error reading file: {e}")
    finally:
        print("#############################################################")
        print(f"Finishing FILE CHUNKER for {file_path} and adding EndOfStream")
        print("#############################################################")
        queue_upload.put(EndOfStream())
