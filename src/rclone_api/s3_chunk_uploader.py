import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, fields
from pathlib import Path
from queue import Queue
from threading import Lock, Thread
from typing import Optional

from botocore.client import BaseClient


@dataclass
class UploadInfo:
    s3_client: BaseClient
    bucket_name: str
    object_name: str
    file_path: str
    upload_id: str
    retries: int
    chunk_size: int

    def to_json(self) -> dict:
        json_dict = {}
        for field in fields(self):
            value = getattr(self, field.name)
            # Convert non-serializable objects (like s3_client) to a string representation.
            if field.name == "s3_client":
                json_dict[field.name] = "RUNTIME OBJECT"
            else:
                json_dict[field.name] = value
        return json_dict

    @staticmethod
    def from_json(s3_client: BaseClient, json_dict: dict) -> "UploadInfo":
        return UploadInfo(s3_client=s3_client, **json_dict)


@dataclass
class FinishedPiece:
    part_number: int
    etag: str

    def to_json(self) -> str:
        return f'{{"part_number": {self.part_number}, "etag": "{self.etag}"}}'

    @staticmethod
    def from_json(json_str: str) -> "FinishedPiece":
        data = json.loads(json_str)
        return FinishedPiece(**data)


@dataclass
class UploadState:
    upload_info: UploadInfo
    finished_parts: Queue[FinishedPiece | None]
    peristant: Path | None

    def save(self) -> None:
        assert self.peristant is not None, "No path to save to"
        self.peristant.write_text(self.to_json_str(), encoding="utf-8")

    @staticmethod
    def load(s3_client: BaseClient, path: Path) -> "UploadState":
        return UploadState.from_json(s3_client, path)

    def to_json(self) -> dict:
        # queue -> list
        parts: list[FinishedPiece] = []
        while self.finished_parts.qsize() > 0:
            qpart = self.finished_parts.get()
            if qpart is not None:
                parts.append(qpart)
        parts.sort(key=lambda x: x.part_number)  # Some backends need this.
        return {
            "upload_info": self.upload_info.to_json(),
            "finished_parts": [p.to_json() for p in parts],
        }

    def to_json_str(self) -> str:
        return json.dumps(self.to_json(), indent=4)

    @staticmethod
    def from_json(s3_client: BaseClient, json_file: Path) -> "UploadState":
        json_str = json_file.read_text(encoding="utf-8")
        data = json.loads(json_str)
        upload_info = UploadInfo.from_json(s3_client, data["upload_info"])
        finished_parts = [FinishedPiece.from_json(p) for p in data["finished_parts"]]
        queue: Queue[FinishedPiece | None] = Queue()
        for part in finished_parts:
            queue.put(part)
        return UploadState(
            peristant=json_file, upload_info=upload_info, finished_parts=queue
        )


# lock

_TMP_DIR_ACCESS_LOCK = Lock()


def clean_old_files(out: Path) -> None:
    # clean up files older than 1 day

    now = time.time()
    # Erase all stale files and then purge empty directories.
    for root, dirs, files in os.walk(out):
        for name in files:
            f = Path(root) / name
            filemod = f.stat().st_mtime
            diff_secs = now - filemod
            diff_days = diff_secs / (60 * 60 * 24)
            if diff_days > 1:
                print(f"Removing old file: {f}")
                f.unlink()

    for root, dirs, _ in os.walk(out):
        for dir in dirs:
            d = Path(root) / dir
            if not list(d.iterdir()):
                print(f"Removing empty directory: {d}")
                d.rmdir()


def _get_chunk_tmpdir() -> Path:
    with _TMP_DIR_ACCESS_LOCK:
        dat = _get_chunk_tmpdir.__dict__
        if "out" in dat:
            return dat["out"]  # Folder already validated.
        out = Path("chunk_store")
        if out.exists():
            # first access, clean up directory
            clean_old_files(out)
        out.mkdir(exist_ok=True, parents=True)
        dat["out"] = out
        return out


class FileChunk:
    def __init__(self, src: Path, upload_id: str, part_number: int, data: bytes):
        assert data is not None, f"{src}: Data must not be None"
        self.upload_id = upload_id
        self.src = src
        self.part_number = part_number
        name = src.name
        self.tmpdir = _get_chunk_tmpdir()
        self.filepart = self.tmpdir / f"{name}_{upload_id}.part_{part_number}.tmp"
        self.filepart.write_bytes(data)
        del data  # free up memory

    @property
    def data(self) -> bytes:
        assert self.filepart is not None
        with open(self.filepart, "rb") as f:
            return f.read()
        return b""

    def close(self):
        if self.filepart.exists():
            self.filepart.unlink()

    def __del__(self):
        self.close()


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
                    upload_id=upload_info.upload_id,
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
) -> FinishedPiece:
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
            out: FinishedPiece = FinishedPiece(
                etag=part["ETag"], part_number=part_number
            )
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
    chunk_size: int = 16 * 1024 * 1024,  # Default chunk size is 16MB; can be overridden
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

    parts_queue: Queue[FinishedPiece | None] = Queue()
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

        parts: list[FinishedPiece] = []
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
