"""
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_part_copy.html
  *  client.upload_part_copy

This module provides functionality for S3 multipart uploads, including copying parts
from existing S3 objects using upload_part_copy.
"""

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Semaphore
from typing import Optional

from botocore.client import BaseClient

from rclone_api.s3.merge_state import MergeState, Part
from rclone_api.s3.multipart.finished_piece import FinishedPiece
from rclone_api.util import locked_print


@dataclass
class MultipartUploadInfo:
    """Simplified upload information for multipart uploads."""

    s3_client: BaseClient
    bucket_name: str
    object_name: str
    upload_id: str
    chunk_size: int
    src_file_path: Optional[Path] = None


def upload_part_copy_task(
    info: MultipartUploadInfo,
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
                locked_print(f"Retrying part copy {part_number} for {info.object_name}")

            locked_print(
                f"Copying part {part_number} for {info.object_name} from {source_bucket}/{source_key}"
            )

            # Prepare the upload_part_copy parameters
            params = {
                "Bucket": info.bucket_name,
                "CopySource": copy_source,
                "Key": info.object_name,
                "PartNumber": part_number,
                "UploadId": info.upload_id,
            }

            # Execute the copy operation
            part = info.s3_client.upload_part_copy(**params)

            # Extract ETag from the response
            etag = part["CopyPartResult"]["ETag"]
            out = FinishedPiece(etag=etag, part_number=part_number)
            locked_print(f"Finished part {part_number} for {info.object_name}")
            return out

        except Exception as e:
            msg = f"Error copying {copy_source} -> {info.object_name}: {e}, params={params}"
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


def complete_multipart_upload_from_parts(
    info: MultipartUploadInfo, parts: list[FinishedPiece]
) -> str:
    """
    Complete a multipart upload using the provided parts.

    Args:
        info: Upload information
        parts: List of finished pieces with ETags

    Returns:
        The URL of the completed object
    """
    # Sort parts by part number to ensure correct order
    parts.sort(key=lambda x: x.part_number)

    # Prepare the parts list for the complete_multipart_upload call
    multipart_parts = [
        {"ETag": part.etag, "PartNumber": part.part_number} for part in parts
    ]

    # Complete the multipart upload
    response = info.s3_client.complete_multipart_upload(
        Bucket=info.bucket_name,
        Key=info.object_name,
        UploadId=info.upload_id,
        MultipartUpload={"Parts": multipart_parts},
    )

    # Return the URL of the completed object
    return response.get("Location", f"s3://{info.bucket_name}/{info.object_name}")


def do_body_work(
    info: MultipartUploadInfo,
    source_bucket: str,
    parts: list[Part],
    max_workers: int,
    merge_state: MergeState,
) -> str | Exception:

    futures: list[Future[FinishedPiece | Exception]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        semaphore = Semaphore(max_workers)
        for part in parts:
            part_number, s3_key = part.part_number, part.s3_key

            def task(
                info=info,
                source_bucket=source_bucket,
                s3_key=s3_key,
                part_number=part_number,
            ):
                out = upload_part_copy_task(
                    info=info,
                    source_bucket=source_bucket,
                    source_key=s3_key,
                    part_number=part_number,
                )
                if isinstance(out, Exception):
                    return out
                merge_state.on_finished(out)
                return out

            fut = executor.submit(task)
            fut.add_done_callback(lambda x: semaphore.release())
            futures.append(fut)
            semaphore.acquire()

        # Upload parts by copying from source objects
        finished_parts: list[FinishedPiece] = []

        for fut in futures:
            finished_part = fut.result()
            if isinstance(finished_part, Exception):
                executor.shutdown(wait=True, cancel_futures=True)
                return finished_part
            finished_parts.append(finished_part)

        # Complete the multipart upload
        return complete_multipart_upload_from_parts(info, finished_parts)


def begin_upload(
    s3_client: BaseClient,
    parts: list[Part],
    destination_bucket: str,
    destination_key: str,
    chunk_size: int,
) -> MultipartUploadInfo:
    """
    Finish a multipart upload by copying parts from existing S3 objects.

    Args:
        s3_client: Boto3 S3 client
        source_bucket: Source bucket name
        source_keys: List of source object keys to copy from
        destination_bucket: Destination bucket name
        destination_key: Destination object key
        chunk_size: Size of each part in bytes
        retries: Number of retry attempts
        byte_ranges: Optional list of byte ranges corresponding to source_keys

    Returns:
        The URL of the completed object
    """

    # Initiate multipart upload
    locked_print(
        f"Creating multipart upload for {destination_bucket}/{destination_key} from {len(parts)} source objects"
    )
    create_params: dict[str, str] = {
        "Bucket": destination_bucket,
        "Key": destination_key,
    }
    print(f"Creating multipart upload with {create_params}")
    mpu = s3_client.create_multipart_upload(**create_params)
    print(f"Created multipart upload: {mpu}")
    upload_id = mpu["UploadId"]

    # Create upload info
    info = MultipartUploadInfo(
        s3_client=s3_client,
        bucket_name=destination_bucket,
        object_name=destination_key,
        upload_id=upload_id,
        chunk_size=chunk_size,
    )
    return info


def finish_multipart_upload_from_keys(
    s3_client: BaseClient,
    source_bucket: str,
    parts: list[Part],
    destination_bucket: str,
    destination_key: str,
    chunk_size: int,  # 5MB default
    max_workers: int = 100,
) -> str | Exception:
    """
    Finish a multipart upload by copying parts from existing S3 objects.

    Args:
        s3_client: Boto3 S3 client
        source_bucket: Source bucket name
        source_keys: List of source object keys to copy from
        destination_bucket: Destination bucket name
        destination_key: Destination object key
        chunk_size: Size of each part in bytes
        retries: Number of retry attempts
        byte_ranges: Optional list of byte ranges corresponding to source_keys

    Returns:
        The URL of the completed object
    """

    # Create upload info
    info = begin_upload(
        s3_client=s3_client,
        parts=parts,
        destination_bucket=destination_bucket,
        destination_key=destination_key,
        chunk_size=chunk_size,
    )

    out = do_body_work(
        info=info,
        source_bucket=source_bucket,
        parts=parts,
        max_workers=max_workers,
        merge_state=MergeState(finished=[], all_parts=parts),
    )

    return out


_DEFAULT_MAX_WORKERS = 10


class S3MultiPartUploader:
    def __init__(self, s3_client: BaseClient, verbose: bool = False) -> None:
        self.verbose = verbose
        self.client: BaseClient = s3_client

    def begin_new_upload(
        self,
        parts: list[Part],
        destination_bucket: str,
        destination_key: str,
        chunk_size: int,
    ) -> MultipartUploadInfo:
        return begin_upload(
            s3_client=self.client,
            parts=parts,
            destination_bucket=destination_bucket,
            destination_key=destination_key,
            chunk_size=chunk_size,
        )

    def start_upload_resume(
        self,
        info: MultipartUploadInfo,
        state: MergeState,
        max_workers: int = _DEFAULT_MAX_WORKERS,
    ) -> MultipartUploadInfo | Exception:
        return Exception("Not implemented")

    def start_upload(
        self,
        info: MultipartUploadInfo,
        parts: list[Part],
        max_workers: int = _DEFAULT_MAX_WORKERS,
    ) -> str | Exception:
        return do_body_work(
            info=info,
            source_bucket=info.bucket_name,
            parts=parts,
            max_workers=max_workers,
            merge_state=MergeState(finished=[], all_parts=parts),
        )
