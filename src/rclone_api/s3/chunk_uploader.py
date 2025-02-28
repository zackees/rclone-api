import _thread
import os
import warnings
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from threading import Thread

from botocore.client import BaseClient

from rclone_api.s3.chunk_file import file_chunker
from rclone_api.s3.chunk_types import FileChunk, FinishedPiece, UploadInfo, UploadState
from rclone_api.s3.types import MultiUploadResult
from rclone_api.util import locked_print

_MIN_UPLOAD_CHUNK_SIZE = 5 * 1024 * 1024  # 5MB


def upload_task(
    info: UploadInfo, chunk: bytes, part_number: int, retries: int
) -> FinishedPiece:
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
    upload_info: UploadInfo, file_chunk: FileChunk | None
) -> FinishedPiece | None:
    if file_chunk is None:
        return None
    chunk, part_number = file_chunk.data, file_chunk.part_number
    part: FinishedPiece = upload_task(
        info=upload_info,
        chunk=chunk,
        part_number=part_number,
        retries=upload_info.retries,
    )
    file_chunk.close()
    return part


def prepare_upload_file_multipart(
    s3_client: BaseClient,
    bucket_name: str,
    file_path: Path,
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

    file_size = os.path.getsize(file_path)

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
    bucket_name: str,
    file_path: Path,
    object_name: str,
    resumable_info_path: Path | None,
    chunk_size: int = 16 * 1024 * 1024,  # Default chunk size is 16MB; can be overridden
    retries: int = 20,
    max_chunks_before_suspension: int | None = None,
    abort_transfer_on_failure: bool = False,
) -> MultiUploadResult:
    """Upload a file to the bucket using multipart upload with customizable chunk size."""
    file_size = os.path.getsize(str(file_path))
    if chunk_size > file_size:
        warnings.warn(
            f"Chunk size {chunk_size} is greater than file size {file_size}, using file size"
        )
        chunk_size = file_size

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

    filechunks: Queue[FileChunk | None] = Queue(10)
    upload_state = get_upload_state() or make_new_state()
    try:
        upload_state.update_source_file(file_path)
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
    max_workers = 8

    chunker_errors: Queue[Exception] = Queue()

    def chunker_task(
        upload_state=upload_state,
        output=filechunks,
        max_chunks=max_chunks_before_suspension,
        queue_errors=chunker_errors,
    ) -> None:
        try:
            file_chunker(
                upload_state=upload_state, output=output, max_chunks=max_chunks
            )
        except Exception as e:
            queue_errors.put(e)
            _thread.interrupt_main()
            raise

    try:
        thread_chunker = Thread(target=chunker_task, daemon=True)
        thread_chunker.start()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                file_chunk: FileChunk | None = filechunks.get()
                if file_chunk is None:
                    break

                def task(upload_info=upload_info, file_chunk=file_chunk):
                    try:
                        return handle_upload(upload_info, file_chunk)
                    except Exception:
                        _thread.interrupt_main()
                        raise

                fut = executor.submit(task)

                def done_cb(fut=fut):
                    result = fut.result()
                    # upload_state.finished_parts.put(result)
                    upload_state.add_finished(result)

                fut.add_done_callback(done_cb)
        # upload_state.finished_parts.put(None)  # Signal the end of the queue
        upload_state.add_finished(None)
        thread_chunker.join()

        if not chunker_errors.empty():
            raise chunker_errors.get()
        if not upload_state.is_done():
            upload_state.save()
            return MultiUploadResult.SUSPENDED
        parts: list[FinishedPiece] = [p for p in upload_state.parts if p is not None]
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
