import hashlib
from dataclasses import dataclass, fields
from pathlib import Path

from botocore.client import BaseClient


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
