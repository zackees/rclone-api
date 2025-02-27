import json
import os
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, fields
from pathlib import Path
from queue import Queue
from threading import Lock, Thread
from typing import Optional

from botocore.client import BaseClient

_MIN_UPLOAD_CHUNK_SIZE = 5 * 1024 * 1024  # 5MB


@dataclass
class UploadInfo:
    s3_client: BaseClient
    bucket_name: str
    object_name: str
    src_file_path: str
    upload_id: str
    retries: int
    chunk_size: int
    file_size: int
    _total_chunks: int | None = None

    def total_chunks(self) -> int:
        out = self.file_size // self.chunk_size
        if self.file_size % self.chunk_size:
            return out + 1
        return out

    def __post_init__(self):
        if self._total_chunks is not None:
            return
        self._total_chunks = self.total_chunks()

    def to_json(self) -> dict:
        json_dict = {}
        for f in fields(self):
            value = getattr(self, f.name)
            # Convert non-serializable objects (like s3_client) to a string representation.
            if f.name == "s3_client":
                json_dict[f.name] = "RUNTIME OBJECT"
            else:
                json_dict[f.name] = value
        return json_dict

    @staticmethod
    def from_json(s3_client: BaseClient, json_dict: dict) -> "UploadInfo":
        json_dict.pop("s3_client")  # Remove the placeholder string
        return UploadInfo(s3_client=s3_client, **json_dict)


@dataclass
class FinishedPiece:
    part_number: int
    etag: str

    def to_json(self) -> dict:
        return {"part_number": self.part_number, "etag": self.etag}

    def to_json_str(self) -> str:
        return json.dumps(self.to_json(), indent=0)

    @staticmethod
    def to_json_array(parts: list["FinishedPiece | None"]) -> list[dict | None]:
        non_none: list[FinishedPiece] = [p for p in parts if p is not None]
        all_nones: list[None] = [None for p in parts if p is None]
        assert len(all_nones) <= 1, "Only one None should be present"
        return [p.to_json() for p in non_none] + all_nones[:1]

    @staticmethod
    def from_json(json_str: str | None) -> "FinishedPiece | None":
        if json_str is None:
            return None
        data = json.loads(json_str)
        return FinishedPiece(**data)


_SAVE_STATE_LOCK = Lock()


@dataclass
class UploadState:
    upload_info: UploadInfo
    # finished_parts: Queue[FinishedPiece | None]
    peristant: Path | None
    lock: Lock = Lock()
    parts: list[FinishedPiece | None] = field(default_factory=list)

    def is_done(self) -> bool:
        return self.remaining() == 0

    def count(self) -> tuple[int, int]:  # count, num_chunks
        num_chunks = self.upload_info.total_chunks()
        count = 0
        for p in self.parts:
            if p is not None:
                count += 1
        return count, num_chunks

    def remaining(self) -> int:
        count, num_chunks = self.count()
        assert (
            count <= num_chunks
        ), f"Count {count} is greater than num_chunks {num_chunks}"
        return num_chunks - count

    def add_finished(self, part: FinishedPiece | None) -> None:
        with self.lock:
            self.parts.append(part)
            self._save_no_lock()

    def __post_init__(self):
        if self.peristant is None:
            # upload_id = self.upload_info.upload_id
            object_name = self.upload_info.object_name
            chunk_size = self.upload_info.chunk_size
            parent = _get_chunk_tmpdir()
            self.peristant = parent / f"{object_name}_chunk_size_{chunk_size}_.json"

    def save(self) -> None:
        with _SAVE_STATE_LOCK:
            self._save_no_lock()

    def _save_no_lock(self) -> None:
        assert self.peristant is not None, "No path to save to"
        self.peristant.write_text(self.to_json_str(), encoding="utf-8")

    @staticmethod
    def load(s3_client: BaseClient, path: Path) -> "UploadState":
        with _SAVE_STATE_LOCK:
            return UploadState.from_json(s3_client, path)

    def to_json(self) -> dict:
        # queue -> list
        # parts: list[dict] = [f.to_json() for f in self.parts]
        parts: list[FinishedPiece | None] = list(self.parts)

        parts_json = FinishedPiece.to_json_array(parts)
        is_done = self.is_done()
        count_non_none: int = 0
        for p in parts:
            if p is not None:
                count_non_none += 1

        # self.count()
        finished_count, total = self.count()

        # parts.sort(key=lambda x: x.part_number)  # Some backends need this.
        return {
            "upload_info": self.upload_info.to_json(),
            "finished_parts": parts_json,
            "is_done": is_done,
            "finished_count": finished_count,
            "total_parts": total,
        }

    def to_json_str(self) -> str:
        return json.dumps(self.to_json(), indent=4)

    @staticmethod
    def from_json(s3_client: BaseClient, json_file: Path) -> "UploadState":
        json_str = json_file.read_text(encoding="utf-8")
        data = json.loads(json_str)
        upload_info = UploadInfo.from_json(s3_client, data["upload_info"])
        finished_parts = [FinishedPiece.from_json(p) for p in data["finished_parts"]]
        return UploadState(
            peristant=json_file, upload_info=upload_info, parts=finished_parts
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


def file_chunker(upload_state: UploadState, output: Queue[FileChunk | None]) -> None:
    upload_info = upload_state.upload_info
    file_path = upload_info.src_file_path
    chunk_size = upload_info.chunk_size
    src = Path(file_path)
    file_size = os.path.getsize(file_path)
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

    try:
        while True:
            curr_parth_num = next_part_number()
            if curr_parth_num is None:
                print(f"File {file_path} is complete")
                break
            assert curr_parth_num is not None
            offset = (curr_parth_num - 1) * chunk_size

            assert offset < file_size, f"Offset {offset} is greater than file size"

            # Open the file, seek, read the chunk, and close immediately.
            with open(file_path, "rb") as f:
                f.seek(offset)
                data = f.read(chunk_size)

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
            part_number += 1
    except Exception as e:

        warnings.warn(f"Error reading file: {e}")
    finally:
        output.put(None)


def upload_task(
    info: UploadInfo, chunk: bytes, part_number: int, retries: int
) -> FinishedPiece:
    retries = retries + 1  # Add one for the initial attempt
    for retry in range(retries):
        try:
            if retry > 0:
                print(f"Retrying part {part_number} for {info.src_file_path}")
            print(
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


def upload_file_multipart(
    s3_client: BaseClient,
    bucket_name: str,
    file_path: str,
    resumable_info_path: Path | None,
    object_name: Optional[str] = None,
    chunk_size: int = 16 * 1024 * 1024,  # Default chunk size is 16MB; can be overridden
    retries: int = 20,
) -> None:
    """Upload a file to the bucket using multipart upload with customizable chunk size."""
    file_size = os.path.getsize(file_path)
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
        if resumable_info_path is None or not resumable_info_path.exists():
            print(
                f"No resumable info found for {file_path}, so creating new upload state"
            )
            return None
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

    def make_new_state() -> UploadState:
        print(f"Creating new upload state for {file_path}")
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
    assert upload_state.is_done() is False, "Upload is already complete"
    upload_info = upload_state.upload_info
    max_workers = 8

    def chunker_task(upload_state=upload_state, output=filechunks) -> None:
        file_chunker(upload_state=upload_state, output=output)

    try:
        thread_chunker = Thread(target=chunker_task, daemon=True)
        thread_chunker.start()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                file_chunk: FileChunk | None = filechunks.get()
                if file_chunk is None:
                    break

                def task(upload_info=upload_info, file_chunk=file_chunk):
                    return handle_upload(upload_info, file_chunk)

                fut = executor.submit(task)

                def done_cb(fut=fut):
                    result = fut.result()
                    # upload_state.finished_parts.put(result)
                    upload_state.add_finished(result)

                fut.add_done_callback(done_cb)
        # upload_state.finished_parts.put(None)  # Signal the end of the queue
        upload_state.add_finished(None)
        thread_chunker.join()
        parts: list[FinishedPiece] = [p for p in upload_state.parts if p is not None]
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
