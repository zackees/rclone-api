from dataclasses import dataclass
from enum import Enum


class S3Provider(Enum):
    BACKBLAZE = "Backblaze"
    DIGITAL_OCEAN = "DigitalOcean"


@dataclass
class S3Credentials:
    """Credentials for accessing S3."""

    access_key_id: str
    secret_access_key: str
    session_token: str | None = None
    region_name: str | None = None
    endpoint_url: str | None = None


@dataclass
class S3UploadTarget:
    """Target information for S3 upload."""

    file_path: str
    bucket_name: str
    s3_key: str
