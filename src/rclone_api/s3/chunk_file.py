import time
import warnings
from concurrent.futures import Future
from pathlib import Path
from queue import Queue
from threading import Event
from typing import Callable

from rclone_api.s3.chunk_types import FileChunk, UploadState
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


def file_chunker(
    upload_state: UploadState,
    chunk_fetcher: Callable[[int, int], Future[bytes | Exception]],
    max_chunks: int | None,
    cancel_signal: Event,
    output: Queue[FileChunk | None],
) -> None:
    count = 0

    if False:
        print(chunk_fetcher)

    def should_stop() -> bool:
        nonlocal count

        if max_chunks is None:
            return False
        if count >= max_chunks:
            return True
        count += 1
        return False

    upload_info = upload_state.upload_info
    file_path = upload_info.src_file_path
    chunk_size = upload_info.chunk_size
    src = Path(file_path)
    # Mounted files may take a while to appear, so keep retrying.

    try:
        part_number = 1
        done_part_numbers: set[int] = {
            p.part_number for p in upload_state.parts if p is not None
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
            curr_parth_num = next_part_number()
            if curr_parth_num is None:
                locked_print(f"File {file_path} has completed chunking all parts")
                break
            assert curr_parth_num is not None
            offset = (curr_parth_num - 1) * chunk_size
            file_size = upload_info.file_size

            assert offset < file_size, f"Offset {offset} is greater than file size"

            # Open the file, seek, read the chunk, and close immediately.
            # with open(file_path, "rb") as f:
            #     f.seek(offset)
            #     data = f.read(chunk_size)

            # data = chunk_fetcher(offset, chunk_size).result()

            assert curr_parth_num is not None
            cpn: int = curr_parth_num

            def on_complete(
                fut: Future[bytes | Exception],
                part_number: int = cpn,
                file_path: Path = file_path,
            ) -> None:
                data: bytes | Exception = fut.result()
                if isinstance(data, Exception):
                    warnings.warn(
                        f"Error reading file: {data}, skipping part {part_number}"
                    )
                    return

                if isinstance(data, Exception):
                    err: Exception = data
                    warnings.warn(
                        f"Error reading file: {err}, skipping part {part_number}"
                    )
                    return

                if not data:
                    warnings.warn(f"Empty data for part {part_number} of {file_path}")

                file_chunk = FileChunk(
                    src,
                    upload_id=upload_info.upload_id,
                    part_number=part_number,
                    data=data,  # After this, data should not be reused.
                )
                done_part_numbers.add(part_number)
                output.put(file_chunk)

            fut = chunk_fetcher(curr_parth_num, chunk_size)
            fut.add_done_callback(on_complete)
            # wait until the output queue can accept the next chunk
            while output.full():
                time.sleep(0.1)
    except Exception as e:

        warnings.warn(f"Error reading file: {e}")
    finally:
        output.put(None)
