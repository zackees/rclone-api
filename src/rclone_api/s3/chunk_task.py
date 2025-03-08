import logging
import time
from concurrent.futures import Future
from pathlib import Path
from queue import Queue
from threading import Event, Lock
from typing import Any, Callable

from rclone_api.file_part import FilePart
from rclone_api.s3.multipart.file_info import S3FileInfo
from rclone_api.s3.multipart.upload_state import UploadState
from rclone_api.types import EndOfStream

logger = logging.getLogger(__name__)  # noqa


class _ShouldStopChecker:
    def __init__(self, max_chunks: int | None) -> None:
        self.count = 0
        self.max_chunks = max_chunks

    def should_stop(self) -> bool:
        if self.max_chunks is None:
            return False
        if self.count >= self.max_chunks:
            logger.info(
                f"Stopping file chunker after {self.count} chunks because it exceeded max_chunks {self.max_chunks}"
            )
            return True
        # self.count += 1
        return False

    def increment(self):
        self.count += 1


class _PartNumberTracker:
    def __init__(
        self, start_part_value: int, last_part_value: int, done_parts: set[int]
    ) -> None:
        # self._num_parts = (last_part_value - start_part_value) + 1
        self._start_part_value = start_part_value
        self._last_part_value = last_part_value
        self._done_part_numbers: set[int] = done_parts
        self._curr_part_number = start_part_value
        self._finished = False
        self._lock = Lock()

    def next_part_number(self) -> int | None:
        with self._lock:
            while self._curr_part_number in self._done_part_numbers:
                self._curr_part_number += 1
            if self._curr_part_number > self._last_part_value:
                self._finished = True
                return None
            curr_part_number = self._curr_part_number
            self._curr_part_number += (
                1  # prevent a second thread from getting the same part number
            )
            return curr_part_number

    def is_finished(self) -> bool:
        with self._lock:
            return self._finished

    def add_finished_part_number(self, part_number: int) -> None:
        with self._lock:
            self._done_part_numbers.add(part_number)


class _OnCompleteHandler:
    def __init__(
        self,
        part_number_tracker: _PartNumberTracker,
        file_path: Path,
        queue_upload: Queue[FilePart | EndOfStream],
    ) -> None:
        self.part_number_tracker = part_number_tracker
        self.file_path = file_path
        self.queue_upload = queue_upload

    def on_complete(self, fut: Future[FilePart]) -> None:
        logger.debug("Chunk read complete")
        fp: FilePart = fut.result()
        extra: S3FileInfo = fp.extra
        assert isinstance(extra, S3FileInfo)
        part_number = extra.part_number
        if fp.is_error():
            logger.warning(f"Error reading file: {fp}, skipping part {part_number}")
            return

        if fp.n_bytes() == 0:
            logger.warning(f"Empty data for part {part_number} of {self.file_path}")
            raise ValueError(f"Empty data for part {part_number} of {self.file_path}")

        if isinstance(fp.payload, Exception):
            logger.warning(f"Error reading file because of error: {fp.payload}")
            return

        # done_part_numbers.add(part_number)
        # queue_upload.put(fp)
        self.part_number_tracker.add_finished_part_number(
            part_number
        )  # in memory database, not persistant to resume.json
        self.queue_upload.put(fp)


def file_chunker(
    upload_state: UploadState,
    fetcher: Callable[[int, int, Any], Future[FilePart]],
    max_chunks: int | None,
    cancel_signal: Event,
    queue_upload: Queue[FilePart | EndOfStream],
) -> None:
    final_part_number = upload_state.upload_info.total_chunks() + 1
    should_stop_checker = _ShouldStopChecker(max_chunks)

    upload_info = upload_state.upload_info
    file_path = upload_info.src_file_path
    chunk_size = upload_info.chunk_size

    done_part_numbers: set[int] = {
        p.part_number for p in upload_state.parts if not isinstance(p, EndOfStream)
    }

    part_tracker = _PartNumberTracker(
        start_part_value=1,
        last_part_value=final_part_number,
        done_parts=done_part_numbers,
    )

    callback = _OnCompleteHandler(part_tracker, file_path, queue_upload)

    try:
        num_parts = upload_info.total_chunks()

        if cancel_signal.is_set():
            logger.info(
                f"Cancel signal is set for file chunker while processing {file_path}, returning"
            )
            return

        while not should_stop_checker.should_stop():
            should_stop_checker.increment()
            logger.debug("Processing next chunk")
            curr_part_number = part_tracker.next_part_number()
            if curr_part_number is None:
                logger.info(f"File {file_path} has completed chunking all parts")
                break

            assert curr_part_number is not None
            offset = (curr_part_number - 1) * chunk_size
            file_size = upload_info.file_size

            assert offset < file_size, f"Offset {offset} is greater than file size"
            fetch_size = max(0, min(chunk_size, file_size - offset))
            if fetch_size == 0:
                logger.error(
                    f"Empty data for part {curr_part_number} of {file_path}, is this the last chunk?"
                )
                # assert final_part_number == curr_part_number, f"Final part number is {final_part_number} but current part number is {curr_part_number}"
                if final_part_number != curr_part_number:
                    raise ValueError(
                        f"This should have been the last part, but it is not: {final_part_number} != {curr_part_number}"
                    )

            assert curr_part_number is not None
            logger.info(
                f"Reading chunk {curr_part_number} of {num_parts} for {file_path}"
            )
            logger.debug(
                f"Fetching part {curr_part_number} with offset {offset} and size {fetch_size}"
            )
            fut = fetcher(
                offset, fetch_size, S3FileInfo(upload_info.upload_id, curr_part_number)
            )
            fut.add_done_callback(callback.on_complete)
            # wait until the queue_upload queue can accept the next chunk
            qsize = queue_upload.qsize()
            print(f"queue_upload_size: {qsize}")
            while queue_upload.full():
                time.sleep(0.1)
    except Exception as e:
        logger.error(f"Error reading file: {e}", exc_info=True)
    finally:
        logger.info(f"Finishing FILE CHUNKER for {file_path} and adding EndOfStream")
        queue_upload.put(EndOfStream())
