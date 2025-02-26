import io
import json
import os
from collections.abc import Iterator
from typing import Any

import boto3
from botocore.client import BaseClient  # FIX: Correct typing for S3 client

_CHUNK_SIZE = 1 * 1024 * 1024 * 1024  # 1GB


class S3Uploader:
    def __init__(
        self,
        file_path: str,
        bucket_name: str,
        s3_key: str,
        chunk_size: int = _CHUNK_SIZE,  # 1GB
        metadata_file: str = "upload_progress.json",
    ) -> None:
        self.file_path: str = file_path
        self.bucket_name: str = bucket_name
        self.s3_key: str = s3_key
        self.chunk_size: int = chunk_size
        self.metadata_file: str = metadata_file
        self.s3: BaseClient = boto3.client("s3")  # FIX: Explicitly typed S3 client

        self.file_size: int
        self.total_chunks: int
        self.file_size, self.total_chunks = self._inspect_file()

        self.progress: dict[str, Any] = self._load_progress()

    def _inspect_file(self) -> tuple[int, int]:
        """Get file size and calculate total chunks."""
        file_size: int = os.path.getsize(self.file_path)
        total_chunks: int = (
            file_size + self.chunk_size - 1
        ) // self.chunk_size  # Ceiling division
        return file_size, total_chunks

    def _load_progress(self) -> dict[str, Any]:
        """Load upload progress from JSON if available, else initialize."""
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, "r") as f:
                return json.load(f)
        return {
            "file_size": self.file_size,
            "total_chunks": self.total_chunks,
            "uploaded_chunks": [],
        }

    def _save_progress(self) -> None:
        """Save progress to a JSON file."""
        with open(self.metadata_file, "w") as f:
            json.dump(self.progress, f, indent=4)

    def _upload_chunk(self, chunk_number: int, offset: int, size: int) -> str:
        """Upload a specific chunk of the file to S3."""
        with open(self.file_path, "rb") as f:
            f.seek(offset)  # Move to the chunk start
            data: bytes = f.read(size)  # Read 1GB chunk

        chunk_key: str = f"{self.s3_key}.part{chunk_number}"
        self.s3.upload_fileobj(io.BytesIO(data), self.bucket_name, chunk_key)

        return chunk_key

    def _chunk_ranges(self) -> Iterator[tuple[int, int, int]]:
        """Generate chunk ranges as (chunk_number, offset, size)."""
        for chunk_number in range(self.total_chunks):
            offset: int = chunk_number * self.chunk_size
            size: int = min(self.chunk_size, self.file_size - offset)
            yield chunk_number, offset, size

    def upload_file(self) -> None:
        """Upload the file in 1GB chunks, resuming if needed."""
        print(f"File Size: {self.file_size} bytes, Total Chunks: {self.total_chunks}")

        for chunk_number, offset, size in self._chunk_ranges():
            if chunk_number in self.progress["uploaded_chunks"]:
                print(f"Skipping chunk {chunk_number}, already uploaded.")
                continue

            print(f"Uploading chunk {chunk_number} ({size} bytes)...")
            chunk_key: str = self._upload_chunk(chunk_number, offset, size)
            print(f"Chunk {chunk_number} uploaded to {chunk_key}.")

            # Update progress
            self.progress["uploaded_chunks"].append(chunk_number)
            self._save_progress()
            print(f"Chunk {chunk_number} uploaded successfully.")

        print("Upload completed.")


def upload_file(file_path: str, bucket_name: str, s3_key: str) -> None:
    uploader = S3Uploader(file_path, bucket_name, s3_key)
    uploader.upload_file()


if __name__ == "__main__":
    upload_file(
        file_path="mount/world_lending_library_2024_11.tar.zst",
        bucket_name="TorrentBooks",
        s3_key="aa_misc_data/aa_misc_data/",
    )
