"""
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_part_copy.html
  *  client.upload_part_copy

This module provides functionality for S3 multipart uploads, including copying parts
from existing S3 objects using upload_part_copy.
"""

import json
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Semaphore
from typing import Any, Optional

from botocore.client import BaseClient

from rclone_api.s3.multipart.finished_piece import FinishedPiece
from rclone_api.util import locked_print


@dataclass
class Part:
    part_number: int
    s3_key: str

    def to_json(self) -> dict:
        return {"part_number": self.part_number, "s3_key": self.s3_key}

    @staticmethod
    def from_json(json_dict: dict) -> "Part | Exception":
        part_number = json_dict.get("part_number")
        s3_key = json_dict.get("s3_key")
        if part_number is None or s3_key is None:
            return Exception(f"Invalid JSON: {json_dict}")
        return Part(part_number=part_number, s3_key=s3_key)

    @staticmethod
    def from_json_array(json_array: list[dict]) -> list["Part"] | Exception:
        try:
            out: list[Part] = []
            for j in json_array:
                ok_or_err = Part.from_json(j)
                if isinstance(ok_or_err, Exception):
                    return ok_or_err
                else:
                    out.append(ok_or_err)
            return out
        except Exception as e:
            return e


class MergeState:

    def __init__(self, finished: list[FinishedPiece], all_parts: list[Part]) -> None:
        self.finished: list[FinishedPiece] = finished
        self.all_parts: list[Part] = all_parts

    def add_finished(self, finished: FinishedPiece) -> None:
        self.finished.append(finished)

    @staticmethod
    def from_json_array(json_array: dict) -> "MergeState | Exception":
        try:
            finished: list[FinishedPiece] = FinishedPiece.from_json_array(
                json_array["finished"]
            )
            all_parts: list[Part | Exception] = [
                Part.from_json(j) for j in json_array["all"]
            ]
            all_parts_no_err: list[Part] = [
                p for p in all_parts if not isinstance(p, Exception)
            ]
            errs: list[Exception] = [p for p in all_parts if isinstance(p, Exception)]
            if len(errs):
                return Exception(f"Errors in parts: {errs}")
            return MergeState(finished=finished, all_parts=all_parts_no_err)
        except Exception as e:
            return e

    def to_json(self) -> dict:
        finished = self.finished.copy()
        all_parts = self.all_parts.copy()
        return {
            "finished": FinishedPiece.to_json_array(finished),
            "all": [part.to_json() for part in all_parts],
        }

    def to_json_str(self) -> str:
        return json.dumps(self.to_json(), indent=1)

    def __str__(self):
        return self.to_json_str()

    def __repr__(self):
        return self.to_json_str()

    def write(self, rclone_impl: Any, dst: str) -> None:
        from rclone_api.rclone_impl import RcloneImpl

        assert isinstance(rclone_impl, RcloneImpl)
        json_str = self.to_json_str()
        rclone_impl.write_text(dst, json_str)

    def read(self, rclone_impl: Any, src: str) -> None:
        from rclone_api.rclone_impl import RcloneImpl

        assert isinstance(rclone_impl, RcloneImpl)
        json_str = rclone_impl.read_text(src)
        if isinstance(json_str, Exception):
            raise json_str
        json_dict = json.loads(json_str)
        ok_or_err = FinishedPiece.from_json_array(json_dict["finished"])
        if isinstance(ok_or_err, Exception):
            raise ok_or_err
        self.finished = ok_or_err


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
    retries: int = 3,
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

    retries = retries + 1  # Add one for the initial attempt
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
    retries: int,
) -> str | Exception:

    futures: list[Future[FinishedPiece | Exception]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # semaphore

        semaphore = Semaphore(max_workers * 2)
        for part in parts:
            part_number, s3_key = part.part_number, part.s3_key

            def task(
                info=info,
                source_bucket=source_bucket,
                s3_key=s3_key,
                part_number=part_number,
                retries=retries,
            ):
                return upload_part_copy_task(
                    info=info,
                    source_bucket=source_bucket,
                    source_key=s3_key,
                    part_number=part_number,
                    retries=retries,
                )

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
    retries: int = 3,
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
        retries=retries,
    )

    return out


_DEFAULT_RETRIES = 20
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
        retries: int = _DEFAULT_RETRIES,
        max_workers: int = _DEFAULT_MAX_WORKERS,
    ) -> MultipartUploadInfo | Exception:
        return Exception("Not implemented")

    def start_upload(
        self,
        info: MultipartUploadInfo,
        parts: list[Part],
        retries: int = _DEFAULT_RETRIES,
        max_workers: int = _DEFAULT_MAX_WORKERS,
    ) -> str | Exception:
        return do_body_work(
            info=info,
            source_bucket=info.bucket_name,
            parts=parts,
            max_workers=max_workers,
            retries=retries,
        )
