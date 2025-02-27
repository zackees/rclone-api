from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class S3Provider(Enum):
    S3 = "s3"  # generic S3
    BACKBLAZE = "b2"
    DIGITAL_OCEAN = "DigitalOcean"

    @staticmethod
    def from_str(value: str) -> "S3Provider":
        """Convert string to S3Provider."""
        if value == "b2":
            return S3Provider.BACKBLAZE
        if value == "DigitalOcean":
            return S3Provider.DIGITAL_OCEAN
        raise ValueError(f"Unknown S3Provider: {value}")


@dataclass
class S3Credentials:
    """Credentials for accessing S3."""

    provider: S3Provider
    access_key_id: str
    secret_access_key: str
    session_token: str | None = None
    region_name: str | None = None
    endpoint_url: str | None = None


@dataclass
class S3UploadTarget:
    """Target information for S3 upload."""

    src_file: Path
    bucket_name: str
    s3_key: str


@dataclass
class S3MutliPartUploadConfig:
    """Input for multi-part upload."""

    chunk_size: int
    retries: int
    resume_path_json: Path
    max_chunks_before_suspension: int | None = None


class MultiUploadResult(Enum):
    UPLOADED_FRESH = 1
    UPLOADED_RESUME = 2
    SUSPENDED = 3
    ALREADY_DONE = 4
