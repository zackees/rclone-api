import _thread
import os
import traceback
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from threading import Event, Thread
from typing import Any, Callable

from botocore.client import BaseClient

from rclone_api.mount import FilePart
from rclone_api.s3.chunk_file import S3FileInfo, file_chunker
from rclone_api.s3.chunk_types import (
    FinishedPiece,
    UploadInfo,
    UploadState,
)
from rclone_api.s3.types import MultiUploadResult
from rclone_api.types import EndOfStream
from rclone_api.util import locked_print

_MIN_UPLOAD_CHUNK_SIZE = 5 * 1024 * 1024  # 5MB


def upload_task(
    info: UploadInfo, chunk: bytes, part_number: int, retries: int
) -> FinishedPiece:
    assert len(chunk) > 0
    retries = retries + 1  # Add one for the initial attempt
    for retry in range(retries):
        try:
            if retry > 0:
                locked_print(f"Retrying part {part_number} for {info.src_file_path}")
            locked_print(
                f"Uploading part {part_number} for {info.src_file_path} of size {len(chunk)}"
            )
            part = info.s3_client.upload_part(
                Bucket=info.bucket_name,
                Key=info.object_name,
                PartNumber=part_number,
                UploadId=info.upload_id,
                Body=chunk,
            )
            out: FinishedPiece = FinishedPiece(
                etag=part["ETag"], part_number=part_number
            )
            return out
        except Exception as e:
            if retry == retries - 1:
                locked_print(f"Error uploading part {part_number}: {e}")
                raise e
            else:
                locked_print(f"Error uploading part {part_number}: {e}, retrying")
                continue
    raise Exception("Should not reach here")


def handle_upload(
    upload_info: UploadInfo, fp: FilePart | EndOfStream
) -> FinishedPiece | Exception | EndOfStream:
    if isinstance(fp, EndOfStream):
        return fp
    assert isinstance(fp.extra, S3FileInfo)
    extra: S3FileInfo = fp.extra
    part_number = extra.part_number
    print(f"Handling upload for {part_number}, size {fp.size}")
    try:
        part: FinishedPiece = upload_task(
            info=upload_info,
            chunk=fp.load(),
            part_number=part_number,
            retries=upload_info.retries,
        )
        fp.close()
        return part
    except Exception as e:
        stacktrace = traceback.format_exc()
        msg = f"Error uploading part {part_number}: {e}\n{stacktrace}"
        warnings.warn(msg)
        return e


def prepare_upload_file_multipart(
    s3_client: BaseClient,
    bucket_name: str,
    file_path: Path,
    file_size: int | None,
    object_name: str,
    chunk_size: int,
    retries: int,
) -> UploadInfo:
    """Upload a file to the bucket using multipart upload with customizable chunk size."""

    # Initiate multipart upload
    locked_print(
        f"Creating multipart upload for {file_path} to {bucket_name}/{object_name}"
    )
    mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_name)
    upload_id = mpu["UploadId"]

    file_size = file_size if file_size is not None else os.path.getsize(file_path)

    upload_info: UploadInfo = UploadInfo(
        s3_client=s3_client,
        bucket_name=bucket_name,
        object_name=object_name,
        src_file_path=file_path,
        upload_id=upload_id,
        retries=retries,
        chunk_size=chunk_size,
        file_size=file_size,
    )
    return upload_info


def _abort_previous_upload(upload_state: UploadState) -> None:
    if upload_state.upload_info.upload_id:
        try:
            upload_state.upload_info.s3_client.abort_multipart_upload(
                Bucket=upload_state.upload_info.bucket_name,
                Key=upload_state.upload_info.object_name,
                UploadId=upload_state.upload_info.upload_id,
            )
        except Exception as e:
            locked_print(f"Error aborting previous upload: {e}")


def upload_file_multipart(
    s3_client: BaseClient,
    chunk_fetcher: Callable[[int, int, Any], Future[FilePart]],
    bucket_name: str,
    file_path: Path,
    file_size: int | None,
    object_name: str,
    resumable_info_path: Path | None,
    chunk_size: int = 16 * 1024 * 1024,  # Default chunk size is 16MB; can be overridden
    upload_threads: int = 16,
    retries: int = 20,
    max_chunks_before_suspension: int | None = None,
    abort_transfer_on_failure: bool = False,
) -> MultiUploadResult:
    """Upload a file to the bucket using multipart upload with customizable chunk size."""
    file_size = file_size if file_size is not None else os.path.getsize(str(file_path))
    # if chunk_size > file_size:
    #     warnings.warn(
    #         f"Chunk size {chunk_size} is greater than file size {file_size}, using file size"
    #     )
    #     chunk_size = file_size

    if chunk_size < _MIN_UPLOAD_CHUNK_SIZE:
        raise ValueError(
            f"Chunk size {chunk_size} is less than minimum upload chunk size {_MIN_UPLOAD_CHUNK_SIZE}"
        )

    def get_upload_state() -> UploadState | None:
        if resumable_info_path is None:
            locked_print(f"No resumable info path provided for {file_path}")
            return None
        if not resumable_info_path.exists():
            locked_print(
                f"Resumable info path {resumable_info_path} does not exist for {file_path}"
            )
            return None
        upload_state = UploadState.load(s3_client=s3_client, path=resumable_info_path)
        return upload_state

    def make_new_state() -> UploadState:
        locked_print(f"Creating new upload state for {file_path}")
        upload_info = prepare_upload_file_multipart(
            s3_client=s3_client,
            bucket_name=bucket_name,
            file_path=file_path,
            file_size=file_size,
            object_name=object_name,
            chunk_size=chunk_size,
            retries=retries,
        )
        upload_state = UploadState(
            upload_info=upload_info,
            parts=[],
            peristant=resumable_info_path,
        )
        return upload_state

    work_que_max = upload_threads // 2 + 2

    new_state = make_new_state()
    loaded_state = get_upload_state()

    if loaded_state is None:
        upload_state = new_state
    else:
        # if the file size has changed, we cannot resume
        if (
            loaded_state.upload_info.fingerprint()
            != new_state.upload_info.fingerprint()
        ):
            locked_print(
                f"Cannot resume upload: file size changed, starting over for {file_path}"
            )
            _abort_previous_upload(loaded_state)
            upload_state = new_state
        else:
            upload_state = loaded_state

    try:
        upload_state.update_source_file(file_path, file_size)
    except ValueError as e:
        locked_print(f"Cannot resume upload: {e}, size changed, starting over")
        _abort_previous_upload(upload_state)
        upload_state = make_new_state()
        upload_state.save()
    if upload_state.is_done():
        return MultiUploadResult.ALREADY_DONE
    finished = upload_state.finished()
    if finished > 0:
        locked_print(
            f"Resuming upload for {file_path}, {finished} parts already uploaded"
        )
    started_new_upload = finished == 0
    upload_info = upload_state.upload_info

    queue_upload: Queue[FilePart | EndOfStream] = Queue(work_que_max)
    chunker_errors: Queue[Exception] = Queue()
    cancel_chunker_event = Event()

    def chunker_task(
        upload_state=upload_state,
        chunk_fetcher=chunk_fetcher,
        queue_upload=queue_upload,
        max_chunks=max_chunks_before_suspension,
        cancel_signal=cancel_chunker_event,
        queue_errors=chunker_errors,
    ) -> None:
        try:
            file_chunker(
                upload_state=upload_state,
                fetcher=chunk_fetcher,
                queue_upload=queue_upload,
                max_chunks=max_chunks,
                cancel_signal=cancel_signal,
            )
        except Exception as e:
            queue_errors.put(e)
            _thread.interrupt_main()
            raise

    try:
        thread_chunker = Thread(target=chunker_task, daemon=True)
        thread_chunker.start()

        with ThreadPoolExecutor(max_workers=upload_threads) as executor:
            try:
                while True:
                    file_chunk: FilePart | EndOfStream = queue_upload.get()
                    if file_chunk is EndOfStream:
                        break

                    if isinstance(file_chunk, EndOfStream):
                        break

                    def task(upload_info=upload_info, file_chunk=file_chunk):
                        return handle_upload(upload_info, file_chunk)

                    fut = executor.submit(task)

                    def done_cb(fut=fut):
                        result = fut.result()
                        if isinstance(result, Exception):
                            warnings.warn(f"Error uploading part: {result}, skipping")
                            return
                        # upload_state.finished_parts.put(result)
                        upload_state.add_finished(result)

                    fut.add_done_callback(done_cb)
            except Exception:
                cancel_chunker_event.set()
                executor.shutdown(wait=False, cancel_futures=True)
                raise
        # upload_state.finished_parts.put(None)  # Signal the end of the queue
        upload_state.add_finished(EndOfStream())
        thread_chunker.join()

        if not chunker_errors.empty():
            raise chunker_errors.get()
        if not upload_state.is_done():
            upload_state.save()
            return MultiUploadResult.SUSPENDED
        parts: list[FinishedPiece] = [
            p for p in upload_state.parts if not isinstance(p, EndOfStream)
        ]
        locked_print(f"Upload complete, sorting {len(parts)} parts to complete upload")
        parts.sort(key=lambda x: x.part_number)  # Some backends need this.
        parts_s3: list[dict] = [
            {"ETag": p.etag, "PartNumber": p.part_number} for p in parts
        ]
        locked_print(f"Sending multi part completion message for {file_path}")
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_name,
            UploadId=upload_info.upload_id,
            MultipartUpload={"Parts": parts_s3},
        )
        locked_print(
            f"Multipart upload completed: {file_path} to {bucket_name}/{object_name}"
        )
    except Exception:
        if upload_info.upload_id and abort_transfer_on_failure:
            try:
                s3_client.abort_multipart_upload(
                    Bucket=bucket_name, Key=object_name, UploadId=upload_info.upload_id
                )
            except Exception:
                pass
        raise
    if started_new_upload:
        return MultiUploadResult.UPLOADED_FRESH
    return MultiUploadResult.UPLOADED_RESUME
