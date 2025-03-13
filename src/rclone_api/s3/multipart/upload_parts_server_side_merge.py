"""
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_part_copy.html
  *  client.upload_part_copy

This module provides functionality for S3 multipart uploads, including copying parts
from existing S3 objects using upload_part_copy.
"""

import json
import os
import time
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from queue import Queue
from threading import Semaphore, Thread
from typing import Any, Callable

from rclone_api.rclone_impl import RcloneImpl
from rclone_api.s3.create import (
    BaseClient,
    S3Config,
    create_s3_client,
)
from rclone_api.s3.multipart.finished_piece import FinishedPiece
from rclone_api.s3.multipart.info_json import InfoJson
from rclone_api.s3.multipart.merge_state import MergeState, Part
from rclone_api.types import EndOfStream
from rclone_api.util import locked_print

DEFAULT_MAX_WORKERS = 5  # Backblaze can do 10 with exponential backoff, so let's try 5

_TIMEOUT_READ = 900
_TIMEOUT_CONNECTION = 900


def _upload_part_copy_task(
    s3_client: BaseClient,
    state: MergeState,
    source_bucket: str,
    source_key: str,
    part_number: int,
) -> FinishedPiece | Exception:
    """
    Upload a part by copying from an existing S3 object.

    Args:
        info: Upload information
        source_bucket: Source bucket name
        source_key: Source object key
        part_number: Part number (1-10000)
        byte_range: Optional byte range in format 'bytes=start-end'
        retries: Number of retry attempts

    Returns:
        FinishedPiece with ETag and part number
    """
    copy_source = {"Bucket": source_bucket, "Key": source_key}

    # from botocore.exceptions import NoSuchKey
    default_retries = 9
    retries = default_retries + 1  # Add one for the initial attempt
    for retry in range(retries):
        params: dict = {}
        try:
            if retry > 0:
                locked_print(f"Retrying part copy {part_number} for {state.dst_key}")

            locked_print(
                f"Copying part {part_number} for {state.dst_key} from {source_bucket}/{source_key}"
            )

            # Prepare the upload_part_copy parameters
            params = {
                "Bucket": state.bucket,
                "CopySource": copy_source,
                "Key": state.dst_key,
                "PartNumber": part_number,
                "UploadId": state.upload_id,
            }

            # Execute the copy operation
            part = s3_client.upload_part_copy(**params)

            # Extract ETag from the response
            etag = part["CopyPartResult"]["ETag"]
            out = FinishedPiece(etag=etag, part_number=part_number)
            locked_print(f"Finished part {part_number} for {state.dst_key}")
            return out

        except Exception as e:
            msg = (
                f"Error copying {copy_source} -> {state.dst_key}: {e}, params={params}"
            )
            if "An error occurred (InternalError)" in str(e):
                locked_print(msg)
            elif "NoSuchKey" in str(e):
                locked_print(msg)
            if retry == retries - 1:
                locked_print(msg)
                return e
            else:
                locked_print(f"{msg}, retrying")
                # sleep
                sleep_time = 2**retry
                locked_print(f"Sleeping for {sleep_time} seconds")
                continue

    return Exception("Should not reach here")


def _complete_multipart_upload_from_parts(
    s3_client: BaseClient, state: MergeState, finished_parts: list[FinishedPiece]
) -> Exception | None:
    """
    Complete a multipart upload using the provided parts.

    Args:
        info: Upload information
        parts: List of finished pieces with ETags

    Returns:
        The URL of the completed object
    """
    # Sort parts by part number to ensure correct order
    finished_parts.sort(key=lambda x: x.part_number)
    multipart_parts = FinishedPiece.to_json_array(finished_parts)
    multipart_upload: dict = {
        "Parts": multipart_parts,
    }
    response: Any = None
    try:
        # Complete the multipart upload
        response = s3_client.complete_multipart_upload(
            Bucket=state.bucket,
            Key=state.dst_key,
            UploadId=state.upload_id,
            MultipartUpload=multipart_upload,
        )
    except Exception as e:
        import traceback

        stacktrace = traceback.format_exc()
        warnings.warn(
            f"Error completing multipart upload: {e}\n\n{response}\n\n{stacktrace}"
        )
        return e

    return None


def _do_upload_task(
    s3_client: BaseClient,
    max_workers: int,
    merge_state: MergeState,
    on_finished: Callable[[FinishedPiece | EndOfStream], None],
) -> Exception | None:
    futures: list[Future[FinishedPiece | Exception]] = []
    parts = merge_state.remaining_parts()
    source_bucket = merge_state.bucket
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        semaphore = Semaphore(max_workers)
        for part in parts:
            part_number, s3_key = part.part_number, part.s3_key

            def task(
                s3_client=s3_client,
                state=merge_state,
                source_bucket=source_bucket,
                s3_key=s3_key,
                part_number=part_number,
            ):
                out = _upload_part_copy_task(
                    s3_client=s3_client,
                    state=state,
                    source_bucket=source_bucket,
                    source_key=s3_key,
                    part_number=part_number,
                )
                if isinstance(out, Exception):
                    return out
                # merge_state.on_finished(out)
                on_finished(out)
                return out

            fut = executor.submit(task)
            fut.add_done_callback(lambda x: semaphore.release())
            futures.append(fut)

            while not semaphore.acquire(blocking=False):
                time.sleep(0.1)

        final_fut = executor.submit(lambda: on_finished(EndOfStream()))

        for fut in futures:
            finished_part = fut.result()
            if isinstance(finished_part, Exception):
                executor.shutdown(wait=True, cancel_futures=True)
                return finished_part
        final_fut.result()

        finished_parts = merge_state.finished
        try:
            assert len(finished_parts) == len(merge_state.all_parts)
        except Exception:
            return ValueError(
                f"Finished parts mismatch: {len(finished_parts)} != {len(parts)}"
            )

        try:
            # Complete the multipart upload
            _complete_multipart_upload_from_parts(
                s3_client=s3_client, state=merge_state, finished_parts=finished_parts
            )
        except Exception as e:
            warnings.warn(f"Error completing multipart upload: {e}")
            return e
        return None


def _begin_upload(
    s3_client: BaseClient,
    parts: list[Part],
    bucket: str,
    dst_key: str,
    verbose: bool,
) -> str:
    """
    Finish a multipart upload by copying parts from existing S3 objects.

    Args:
        s3_client: Boto3 S3 client
        source_bucket: Source bucket name
        source_keys: List of source object keys to copy from
        bucket: Destination bucket name
        dst_key: Destination object key
        retries: Number of retry attempts
        byte_ranges: Optional list of byte ranges corresponding to source_keys

    Returns:
        The upload id of the multipart upload
    """

    # Initiate multipart upload
    if verbose:
        locked_print(
            f"Creating multipart upload for {bucket}/{dst_key} from {len(parts)} source objects"
        )
    create_params: dict[str, str] = {
        "Bucket": bucket,
        "Key": dst_key,
    }
    if verbose:
        locked_print(f"Creating multipart upload with {create_params}")
    mpu = s3_client.create_multipart_upload(**create_params)
    if verbose:
        locked_print(f"Created multipart upload: {mpu}")
    upload_id = mpu["UploadId"]
    return upload_id


class WriteMergeStateThread(Thread):
    def __init__(self, rclone_impl: RcloneImpl, merge_state: MergeState, verbose: bool):
        super().__init__(daemon=True)
        assert isinstance(merge_state, MergeState)
        self.verbose = verbose
        self.merge_state = merge_state
        self.merge_path = merge_state.merge_path
        self.rclone_impl = rclone_impl
        self.queue: Queue[FinishedPiece | EndOfStream] = Queue()
        self.start()

    def _get_next(self) -> FinishedPiece | EndOfStream:
        item = self.queue.get()
        if isinstance(item, EndOfStream):
            return item
        # see if there are more items in the queue, only write the last one
        while not self.queue.empty():
            item = self.queue.get()
            if isinstance(item, EndOfStream):
                # put it back in for next time
                self.queue.put(item)
                return item
        return item

    def verbose_print(self, msg: str) -> None:
        if self.verbose:
            locked_print(msg)

    def run(self):
        while True:
            item = self._get_next()
            if isinstance(item, EndOfStream):
                self.verbose_print("WriteMergeStateThread: End of stream")
                break

            assert isinstance(item, FinishedPiece)
            # piece: FinishedPiece = item
            # at this point just write out the whole json str
            json_str = self.merge_state.to_json_str()
            err = self.rclone_impl.write_text(self.merge_path, json_str)
            if isinstance(err, Exception):
                warnings.warn(f"Error writing merge state: {err}")
                break

    def add_finished(self, finished: FinishedPiece) -> None:
        self.queue.put(finished)

    def add_eos(self) -> None:
        self.queue.put(EndOfStream())


def _cleanup_merge(rclone: RcloneImpl, info: InfoJson) -> Exception | None:
    size = info.size
    dst = info.dst
    parts_dir = info.parts_dir
    if not rclone.exists(dst):
        return FileNotFoundError(f"Destination file not found: {dst}")

    write_size = rclone.size_file(dst)
    if write_size != size:
        return ValueError(f"Size mismatch: {write_size} != {size}")

    print(f"Upload complete: {dst}")
    cp = rclone.purge(parts_dir)
    if cp.failed():
        return Exception(f"Failed to purge parts dir: {cp}")
    return None


def _get_merge_path(info_path: str) -> str:
    par_dir = os.path.dirname(info_path)
    merge_path = f"{par_dir}/merge.json"
    return merge_path


def _begin_or_resume_merge(
    rclone: RcloneImpl,
    info: InfoJson,
    verbose: bool = False,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> "S3MultiPartMerger | Exception":
    try:
        merger: S3MultiPartMerger = S3MultiPartMerger(
            rclone_impl=rclone,
            info=info,
            verbose=verbose,
            max_workers=max_workers,
        )

        s3_bucket = merger.bucket
        is_done = info.fetch_is_done()
        assert is_done, f"Upload is not done: {info}"

        merge_path = _get_merge_path(info_path=info.src_info)
        merge_json_text = rclone.read_text(merge_path)
        if isinstance(merge_json_text, str):
            # Attempt to do a resume
            merge_data = json.loads(merge_json_text)
            merge_state = MergeState.from_json(rclone_impl=rclone, json=merge_data)
            if isinstance(merge_state, MergeState):
                merger._begin_resume_merge(merge_state=merge_state)
                return merger
            warnings.warn(f"Failed to resume merge: {merge_state}, starting new merge")

        parts_dir = info.parts_dir
        source_keys = info.fetch_all_finished()

        parts_path = parts_dir.split(s3_bucket)[1]
        if parts_path.startswith("/"):
            parts_path = parts_path[1:]

        first_part: int | None = info.first_part
        last_part: int | None = info.last_part

        assert first_part is not None
        assert last_part is not None

        def _to_s3_key(name: str | None) -> str:
            if name:
                out = f"{parts_path}/{name}"
                return out
            out = f"{parts_path}"
            return out

        parts: list[Part] = []
        part_num = first_part
        for part_key in source_keys:
            assert part_num <= last_part and part_num >= first_part
            s3_key = _to_s3_key(name=part_key)
            part = Part(part_number=part_num, s3_key=s3_key)
            parts.append(part)
            part_num += 1

        dst_name = info.dst_name
        dst_dir = os.path.dirname(parts_path)
        dst_key = f"{dst_dir}/{dst_name}"

        err = merger._begin_new_merge(
            merge_path=merge_path,
            parts=parts,
            bucket=merger.bucket,
            dst_key=dst_key,
        )
        if isinstance(err, Exception):
            return err
        return merger
    except Exception as e:
        return e


class S3MultiPartMerger:
    def __init__(
        self,
        rclone_impl: RcloneImpl,
        info: InfoJson,
        s3_config: S3Config | None = None,
        verbose: bool = False,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> None:
        self.rclone_impl: RcloneImpl = rclone_impl
        self.info = info
        self.s3_creds = rclone_impl.get_s3_credentials(remote=info.dst)
        self.verbose = verbose
        s3_config = s3_config or S3Config(
            verbose=verbose,
            timeout_read=_TIMEOUT_READ,
            timeout_connection=_TIMEOUT_CONNECTION,
            max_pool_connections=max_workers,
        )
        self.max_workers = s3_config.max_pool_connections or DEFAULT_MAX_WORKERS
        self.client = create_s3_client(s3_creds=self.s3_creds, s3_config=s3_config)
        self.state: MergeState | None = None
        self.write_thread: WriteMergeStateThread | None = None

    @staticmethod
    def create(
        rclone: RcloneImpl, info: InfoJson, max_workers: int, verbose: bool
    ) -> "S3MultiPartMerger | Exception":
        return _begin_or_resume_merge(
            rclone=rclone, info=info, max_workers=max_workers, verbose=verbose
        )

    @property
    def bucket(self) -> str:
        return self.s3_creds.bucket_name

    def start_write_thread(self) -> None:
        assert self.state is not None
        assert self.write_thread is None
        self.write_thread = WriteMergeStateThread(
            rclone_impl=self.rclone_impl,
            merge_state=self.state,
            verbose=self.verbose,
        )

    def _begin_new_merge(
        self,
        parts: list[Part],
        merge_path: str,
        bucket: str,
        dst_key: str,
    ) -> Exception | None:
        try:
            upload_id: str = _begin_upload(
                s3_client=self.client,
                parts=parts,
                bucket=bucket,
                dst_key=dst_key,
                verbose=self.verbose,
            )
            merge_state = MergeState(
                rclone_impl=self.rclone_impl,
                merge_path=merge_path,
                upload_id=upload_id,
                bucket=bucket,
                dst_key=dst_key,
                finished=[],
                all_parts=parts,
            )
            self.state = merge_state
            return None
        except Exception as e:
            return e

    def _begin_resume_merge(
        self,
        merge_state: MergeState,
    ) -> None:
        self.state = merge_state

    def _on_piece_finished(self, finished_piece: FinishedPiece | EndOfStream) -> None:
        assert self.write_thread is not None
        assert self.state is not None
        if isinstance(finished_piece, EndOfStream):
            self.write_thread.add_eos()
        else:
            self.state.on_finished(finished_piece)
            self.write_thread.add_finished(finished_piece)

    def merge(
        self,
    ) -> Exception | None:
        state = self.state
        if state is None:
            return Exception("No merge state loaded")
        self.start_write_thread()
        err = _do_upload_task(
            s3_client=self.client,
            merge_state=state,
            max_workers=self.max_workers,
            on_finished=self._on_piece_finished,
        )
        if isinstance(err, Exception):
            return err
        return None

    def cleanup(self) -> Exception | None:
        return _cleanup_merge(rclone=self.rclone_impl, info=self.info)


def s3_server_side_multi_part_merge(
    rclone: RcloneImpl,
    info_path: str,
    max_workers: int = DEFAULT_MAX_WORKERS,
    verbose: bool = False,
) -> Exception | None:
    info = InfoJson(rclone, src=None, src_info=info_path)
    loaded = info.load()
    if not loaded:
        return FileNotFoundError(
            f"Info file not found, has the upload finished? {info_path}"
        )
    merger: S3MultiPartMerger | Exception = S3MultiPartMerger.create(
        rclone=rclone, info=info, max_workers=max_workers, verbose=verbose
    )
    if isinstance(merger, Exception):
        return merger

    err = merger.merge()
    if isinstance(err, Exception):
        return err

    err = merger.cleanup()
    if isinstance(err, Exception):
        return err
    return None
