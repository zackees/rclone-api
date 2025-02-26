import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Optional

import boto3
from botocore.client import BaseClient
from botocore.config import Config


@dataclass
class UploadInfo:
    s3_client: BaseClient
    bucket_name: str
    object_name: str
    file_path: str
    upload_id: str
    retries: int
    chunk_size: int


@dataclass
class UploadResult:
    part_number: int
    etag: str


def _get_chunk_tmpdir() -> Path:
    out = Path("chunk_store")
    out.mkdir(exist_ok=True, parents=True)
    return out


class FileChunk:
    def __init__(self, src: Path, part_number: int, data: bytes):
        assert data is not None, f"{src}: Data must not be None"
        self.src = src
        self.part_number = part_number
        name = src.name
        self.tmpdir = _get_chunk_tmpdir()
        self.filepart = self.tmpdir / f"{name}.part_{part_number}.tmp"
        self.filepart.write_bytes(data)
        del data  # free up memory

    @property
    def data(self) -> bytes:
        assert self.filepart is not None
        with open(self.filepart, "rb") as f:
            return f.read()
        return b""


# Create a Boto3 session and S3 client, this is back blaze specific.
# Add a function if you want to use a different S3 provider.
# If AWS support is added in a fork then please merge it back here.
def create_backblaze_s3_client(
    access_key: str, secret_key: str, endpoint_url: str | None
) -> BaseClient:
    """Create and return an S3 client."""
    session = boto3.session.Session()  # type: ignore
    return session.client(
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        config=Config(
            signature_version="s3v4",
            # s3={"payload_signing_enabled": False},  # Disable checksum header
        ),
    )


def list_bucket_contents(s3_client: BaseClient, bucket_name: str) -> None:
    """List contents of the specified bucket."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            for obj in response["Contents"]:
                print(f"File: {obj['Key']} | Size: {obj['Size']} bytes")
        else:
            print(f"The bucket '{bucket_name}' is empty.")
    except Exception as e:
        print(f"Error listing bucket contents: {e}")


def upload_file(
    s3_client: BaseClient,
    bucket_name: str,
    file_path: str,
    object_name: Optional[str] = None,
) -> Exception | None:
    """Upload a file to the bucket."""
    try:
        object_name = object_name or file_path.split("/")[-1]
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"Uploaded {file_path} to {bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error uploading file: {e}")
        return e
    return None


def download_file(
    s3_client: BaseClient, bucket_name: str, object_name: str, file_path: str
) -> None:
    """Download a file from the bucket."""
    try:
        s3_client.download_file(bucket_name, object_name, file_path)
        print(f"Downloaded {object_name} from {bucket_name} to {file_path}")
    except Exception as e:
        print(f"Error downloading file: {e}")


def file_chunker(upload_info: UploadInfo, filechunks: Queue[FileChunk | None]) -> None:
    part_number = 1
    file_path = upload_info.file_path
    chunk_size = upload_info.chunk_size
    src = Path(upload_info.file_path)
    with open(file_path, "rb") as f:
        try:
            while data := f.read(chunk_size):
                file_chunk = FileChunk(
                    src,
                    part_number=part_number,
                    data=data,  # del data on the input will be called. Don't use data after this.
                )
                filechunks.put(file_chunk)
                part_number += 1
        except Exception as e:
            import warnings

            warnings.warn(f"Error reading file: {e}")
        finally:
            filechunks.put(None)


def upload_task(
    info: UploadInfo, chunk: bytes, part_number: int, retries: int
) -> UploadResult:
    retries = retries + 1  # Add one for the initial attempt
    for retry in range(retries):
        try:
            if retry > 0:
                print(f"Retrying part {part_number} for {info.file_path}")
            print(
                f"Uploading part {part_number} for {info.file_path} of size {len(chunk)}"
            )
            part = info.s3_client.upload_part(
                Bucket=info.bucket_name,
                Key=info.object_name,
                PartNumber=part_number,
                UploadId=info.upload_id,
                Body=chunk,
            )
            out: UploadResult = UploadResult(etag=part["ETag"], part_number=part_number)
            return out
        except Exception as e:
            if retry == retries - 1:
                print(f"Error uploading part {part_number}: {e}")
                raise e
            else:
                print(f"Error uploading part {part_number}: {e}, retrying")
                continue
    raise Exception("Should not reach here")


def handle_upload(
    upload_info: UploadInfo, file_chunk: FileChunk | None
) -> UploadResult | None:
    if file_chunk is None:
        return None
    chunk, part_number = file_chunk.data, file_chunk.part_number
    part: UploadResult = upload_task(
        info=upload_info,
        chunk=chunk,
        part_number=part_number,
        retries=upload_info.retries,
    )
    return part


def prepare_upload_file_multipart(
    s3_client: BaseClient,
    bucket_name: str,
    file_path: str,
    object_name: Optional[str] = None,
    chunk_size: int = 5 * 1024 * 1024,  # Default chunk size is 5MB; can be overridden
    retries: int = 20,
) -> UploadInfo:
    """Upload a file to the bucket using multipart upload with customizable chunk size."""

    object_name = object_name or os.path.basename(file_path)

    # Initiate multipart upload
    print(f"Creating multipart upload for {file_path} to {bucket_name}/{object_name}")
    mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_name)
    upload_id = mpu["UploadId"]

    upload_info: UploadInfo = UploadInfo(
        s3_client=s3_client,
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=file_path,
        upload_id=upload_id,
        retries=retries,
        chunk_size=chunk_size,
    )
    return upload_info


def upload_file_multipart(
    s3_client: BaseClient,
    bucket_name: str,
    file_path: str,
    object_name: Optional[str] = None,
    chunk_size: int = 5 * 1024 * 1024,  # Default chunk size is 5MB; can be overridden
    retries: int = 20,
) -> None:
    """Upload a file to the bucket using multipart upload with customizable chunk size."""

    upload_info: UploadInfo = prepare_upload_file_multipart(
        s3_client=s3_client,
        bucket_name=bucket_name,
        file_path=file_path,
        object_name=object_name,
        chunk_size=chunk_size,
        retries=retries,
    )

    parts_queue: Queue[UploadResult | None] = Queue()
    filechunks: Queue[FileChunk | None] = Queue(10)

    try:
        thread_chunker = Thread(
            target=file_chunker, args=(upload_info, filechunks), daemon=True
        )
        thread_chunker.start()

        with ThreadPoolExecutor() as executor:
            while True:
                file_chunk: FileChunk | None = filechunks.get()
                if file_chunk is None:
                    break

                def task(upload_info=upload_info, file_chunk=file_chunk):
                    return handle_upload(upload_info, file_chunk)

                fut = executor.submit(task)
                fut.add_done_callback(lambda fut: parts_queue.put(fut.result()))
            parts_queue.put(None)  # Signal the end of the queue

        thread_chunker.join()

        parts: list[UploadResult] = []
        while parts_queue.qsize() > 0:
            qpart = parts_queue.get()
            if qpart is not None:
                parts.append(qpart)

        print(f"Upload complete, sorting {len(parts)} parts to complete upload")
        parts.sort(key=lambda x: x.part_number)  # Some backends need this.
        parts_s3: list[dict] = [
            {"ETag": p.etag, "PartNumber": p.part_number} for p in parts
        ]
        print(f"Sending multi part completion message for {file_path}")
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_name,
            UploadId=upload_info.upload_id,
            MultipartUpload={"Parts": parts_s3},
        )
        print(f"Multipart upload completed: {file_path} to {bucket_name}/{object_name}")
    except Exception:
        if upload_info.upload_id:
            try:
                s3_client.abort_multipart_upload(
                    Bucket=bucket_name, Key=object_name, UploadId=upload_info.upload_id
                )
            except Exception:
                pass
        raise
