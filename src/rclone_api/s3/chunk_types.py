import hashlib
import json
import os
import time
from dataclasses import dataclass, field, fields
from pathlib import Path
from threading import Lock

from botocore.client import BaseClient

from rclone_api.types import SizeSuffix
from rclone_api.util import locked_print

_MIN_UPLOAD_CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
_SAVE_STATE_LOCK = Lock()

_TMP_DIR_ACCESS_LOCK = Lock()


def _clean_old_files(out: Path) -> None:
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
                locked_print(f"Removing old file: {f}")
                f.unlink()

    for root, dirs, _ in os.walk(out):
        for dir in dirs:
            d = Path(root) / dir
            if not list(d.iterdir()):
                locked_print(f"Removing empty directory: {d}")
                d.rmdir()


def _get_chunk_tmpdir() -> Path:
    with _TMP_DIR_ACCESS_LOCK:
        dat = _get_chunk_tmpdir.__dict__
        if "out" in dat:
            return dat["out"]  # Folder already validated.
        out = Path("chunk_store")
        if out.exists():
            # first access, clean up directory
            _clean_old_files(out)
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


@dataclass
class UploadInfo:
    s3_client: BaseClient
    bucket_name: str
    object_name: str
    src_file_path: Path
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

    def fingerprint(self) -> str:
        # hash the attributes that are used to identify the upload
        hasher = hashlib.sha256()
        # first is file size
        hasher.update(str(self.file_size).encode("utf-8"))
        # second is the file path
        hasher.update(str(self.src_file_path).encode("utf-8"))
        # next is chunk size
        hasher.update(str(self.chunk_size).encode("utf-8"))
        # next is the number of parts
        hasher.update(str(self._total_chunks).encode("utf-8"))
        return hasher.hexdigest()

    def to_json(self) -> dict:
        json_dict = {}
        for f in fields(self):
            value = getattr(self, f.name)
            # Convert non-serializable objects (like s3_client) to a string representation.
            if f.name == "s3_client":
                continue
            else:
                if isinstance(value, Path):
                    value = str(value)
                json_dict[f.name] = value

        return json_dict

    @staticmethod
    def from_json(s3_client: BaseClient, json_dict: dict) -> "UploadInfo":
        # json_dict.pop("s3_client")  # Remove the placeholder string
        if "s3_client" in json_dict:
            json_dict.pop("s3_client")

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
        non_none.sort(key=lambda x: x.part_number)
        all_nones: list[None] = [None for p in parts if p is None]
        assert len(all_nones) <= 1, "Only one None should be present"
        return [p.to_json() for p in non_none]

    @staticmethod
    def from_json(json: dict | None) -> "FinishedPiece | None":
        if json is None:
            return None
        return FinishedPiece(**json)


@dataclass
class UploadState:
    upload_info: UploadInfo
    # finished_parts: Queue[FinishedPiece | None]
    peristant: Path | None
    lock: Lock = Lock()
    parts: list[FinishedPiece | None] = field(default_factory=list)

    def update_source_file(self, src_file: Path) -> None:
        new_file_size = os.path.getsize(src_file)
        if new_file_size != self.upload_info.file_size:
            raise ValueError("File size changed, cannot resume")
        self.upload_info.src_file_path = src_file
        self.save()

    def is_done(self) -> bool:
        return self.remaining() == 0

    def fingerprint(self) -> str:
        return self.upload_info.fingerprint()

    def count(self) -> tuple[int, int]:  # count, num_chunks
        num_chunks = self.upload_info.total_chunks()
        count = 0
        for p in self.parts:
            if p is not None:
                count += 1
        return count, num_chunks

    def finished(self) -> int:
        count, _ = self.count()
        return count

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

            self._check_fingerprint_no_lock()

            self._save_no_lock()

    def _check_fingerprint_no_lock(self) -> None:
        if self.peristant is None:
            raise ValueError("No path to save to")
        s3_client = self.upload_info.s3_client
        path = self.peristant
        last_upload_state: UploadState | None = None
        if path.exists():
            try:
                last_upload_state = UploadState.from_json(s3_client, path)
            except Exception as e:
                locked_print(f"Error loading state: {e}")
                last_upload_state = None
            # now check that the fingerprint is the same
            if last_upload_state is not None:
                curr_fingerprint = self.fingerprint()
                if curr_fingerprint != last_upload_state.fingerprint():
                    raise ValueError(
                        f"Cannot save state, fingerprint changed from {curr_fingerprint} to {self.upload_info.fingerprint()}"
                    )

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
        total_finished: SizeSuffix = SizeSuffix(
            finished_count * self.upload_info.chunk_size
        )
        total_remaining: SizeSuffix = SizeSuffix(
            self.remaining() * self.upload_info.chunk_size
        )

        # parts.sort(key=lambda x: x.part_number)  # Some backends need this.
        out_json = {
            "upload_info": self.upload_info.to_json(),
            "finished_parts": parts_json,
            "is_done": is_done,
            "finished_count": finished_count,
            "total_parts": total,
            "total_size": SizeSuffix(self.upload_info.file_size).as_str(),
            "total_finished": total_finished.as_str(),
            "total_remaining": total_remaining.as_str(),
            "completed": f"{(finished_count / total) * 100:.2f}%",
        }

        # check that we can sererialize
        # json.dumps(out_json)
        return out_json

    def to_json_str(self) -> str:
        return json.dumps(self.to_json(), indent=4)

    @staticmethod
    def from_json(s3_client: BaseClient, json_file: Path) -> "UploadState":
        json_str = json_file.read_text(encoding="utf-8")
        data = json.loads(json_str)
        upload_info_json = data["upload_info"]
        finished_parts_json = data["finished_parts"]
        upload_info = UploadInfo.from_json(s3_client, upload_info_json)
        finished_parts = [FinishedPiece.from_json(p) for p in finished_parts_json]
        return UploadState(
            peristant=json_file, upload_info=upload_info, parts=finished_parts
        )
