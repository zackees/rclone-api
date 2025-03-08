from dataclasses import dataclass


@dataclass
class S3FileInfo:
    upload_id: str
    part_number: int
