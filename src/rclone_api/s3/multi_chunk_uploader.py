import json
import os
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from tempfile import TemporaryFile

from botocore.client import BaseClient  # Correct typing for S3 client

from rclone_api.s3.create import S3Provider, create_s3_client
from rclone_api.s3.types import S3Credentials, S3UploadTarget

_CHUNK_SIZE = 1024 * 1024 * 16


@dataclass
class UploadProgress:
    file_size: int
    total_chunks: int
    uploaded_chunks: list[int] = field(default_factory=list)

    def to_json(self) -> str:
        """Serialize the dataclass to a JSON string."""
        return json.dumps(asdict(self), indent=4)

    @staticmethod
    def from_json(json_str: str) -> "UploadProgress":
        """Deserialize a JSON string into an UploadProgress instance."""
        data = json.loads(json_str)
        return UploadProgress(**data)


class S3MultiChunkUploader:
    def __init__(
        self,
        credentials: S3Credentials,
        target: S3UploadTarget,
        chunk_size: int = _CHUNK_SIZE,
        metadata_file: str = "upload_progress.json",
    ) -> None:
        self.file_path: str = target.file_path
        self.bucket_name: str = target.bucket_name
        self.s3_key: str = target.s3_key
        self.chunk_size: int = chunk_size
        self.metadata_file: str = metadata_file

        self.s3: BaseClient = create_s3_client(
            provider=S3Provider.BACKBLAZE,
            access_key=credentials.access_key_id,
            secret_key=credentials.secret_access_key,
            endpoint_url=credentials.endpoint_url,
        )

        self.file_size, self.total_chunks = self._inspect_file()
        self.progress: UploadProgress = self._load_progress()

    def _inspect_file(self) -> tuple[int, int]:
        """Get file size and calculate total chunks."""
        file_size: int = os.path.getsize(self.file_path)
        total_chunks: int = (file_size + self.chunk_size - 1) // self.chunk_size
        return file_size, total_chunks

    def _load_progress(self) -> UploadProgress:
        """Load upload progress from JSON if available; otherwise, initialize a new instance."""
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, "r") as f:
                json_str = f.read()
            return UploadProgress.from_json(json_str)
        return UploadProgress(
            file_size=self.file_size, total_chunks=self.total_chunks, uploaded_chunks=[]
        )

    def _save_progress(self) -> None:
        """Save the current progress to a JSON file."""
        with open(self.metadata_file, "w") as f:
            f.write(self.progress.to_json())

    def _upload_chunk(self, chunk_number: int, offset: int, size: int) -> str:
        """Upload a specific chunk of the file to S3 by copying the byte range into a temporary file."""
        chunk_key: str = f"{self.s3_key}.part{chunk_number}"

        # Read the specified byte range from the original file.
        # print(f"Reading chunk {chunk_number} ({size} bytes)...")
        print(f"@{chunk_number} is ({size} bytes)...")
        # print(f"Opening file {self.file_path}...")
        print(f"@{chunk_number} is opening file {self.file_path}...")
        with open(self.file_path, "rb") as src:
            # print(f"Seeking to offset {offset}...")
            print(f"@{chunk_number} is seeking to offset {offset}...")
            src.seek(offset)
            # print(f"Reading {size} bytes...")
            print(f"@{chunk_number} is reading {size} bytes...")
            chunk_data = src.read(size)

        # Write the data into a temporary file and then upload that file.
        with TemporaryFile("w+b") as tmp:
            print(f"@{chunk_number} is writing to temporary file...")
            tmp.write(chunk_data)
            print(f"@{chunk_number} is finished writing to temporary file...")
            print(f"@{chunk_number} is seeking to start of temporary file...")
            tmp.seek(0)
            print(f"@{chunk_number} is uploading to S3...")
            self.s3.upload_fileobj(tmp, self.bucket_name, chunk_key)
            print(f"@{chunk_number} is finished uploading to S3...")

        print(f"@{chunk_number} is returning chunk key {chunk_key}...")
        return chunk_key

    def _chunk_ranges(self) -> Iterator[tuple[int, int, int]]:
        """Generate chunk ranges as (chunk_number, offset, size)."""
        for chunk_number in range(self.total_chunks):
            offset: int = chunk_number * self.chunk_size
            size: int = min(self.chunk_size, self.file_size - offset)
            yield chunk_number, offset, size

    def upload_file(self) -> None:
        """Upload the file in chunks, resuming if needed."""
        print(f"File Size: {self.file_size} bytes, Total Chunks: {self.total_chunks}")

        for chunk_number, offset, size in self._chunk_ranges():
            if chunk_number in self.progress.uploaded_chunks:
                print(f"Skipping chunk {chunk_number}, already uploaded.")
                continue

            print(f"Uploading chunk {chunk_number} ({size} bytes)...")
            start_time = time.time()
            chunk_key: str = self._upload_chunk(chunk_number, offset, size)
            diff_time = time.time() - start_time
            print(
                f"Chunk {chunk_number} uploaded to {chunk_key} in {int(diff_time)} seconds."
            )

            # Update progress and save progress after each chunk
            self.progress.uploaded_chunks.append(chunk_number)
            self._save_progress()
            print(f"Chunk {chunk_number} uploaded successfully.")

        print("Upload completed.")


def upload_file(credentials: S3Credentials, target: S3UploadTarget) -> None:
    uploader = S3MultiChunkUploader(credentials, target)
    uploader.upload_file()


if __name__ == "__main__":
    credentials = S3Credentials(
        provider=S3Provider.BACKBLAZE,
        access_key_id="YOUR_ACCESS_KEY",
        secret_access_key="YOUR_SECRET_KEY",
        # Optional parameters
        # session_token="YOUR_SESSION_TOKEN",
        # region_name="us-east-1",
        # endpoint_url="https://s3.amazonaws.com"
    )

    target = S3UploadTarget(
        file_path="mount/world_lending_library_2024_11.tar.zst",
        bucket_name="TorrentBooks",
        s3_key="aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst",
    )

    upload_file(credentials=credentials, target=target)
