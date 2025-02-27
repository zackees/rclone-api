from pathlib import Path

from botocore.client import BaseClient

from rclone_api.s3.basic_ops import download_file, list_bucket_contents, upload_file
from rclone_api.s3.chunk_uploader import MultiUploadResult, upload_file_multipart
from rclone_api.s3.create import create_s3_client
from rclone_api.s3.types import S3Credentials, S3UploadTarget


class S3Client:
    def __init__(self, credentials: S3Credentials):
        self.credentials: S3Credentials = credentials
        self.client: BaseClient = create_s3_client(credentials)

    def list_bucket_contents(self, bucket_name: str) -> None:
        list_bucket_contents(self.client, bucket_name)

    def upload_file(
        self, bucket_name: str, file_path: str, object_name: str
    ) -> Exception | None:
        return upload_file(self.client, bucket_name, file_path, object_name)

    def download_file(self, bucket_name: str, object_name: str, file_path: str) -> None:
        download_file(self.client, bucket_name, object_name, file_path)

    def upload_file_multipart(
        self,
        upload_target: S3UploadTarget,
        chunk_size: int,
        resume_path_json: Path,
        retries: int,
        max_chunks_before_suspension: int | None = None,
    ) -> MultiUploadResult:
        bucket_name = upload_target.bucket_name
        out = upload_file_multipart(
            s3_client=self.client,
            bucket_name=bucket_name,
            file_path=upload_target.src_file,
            object_name=upload_target.s3_key,
            resumable_info_path=resume_path_json,
            chunk_size=chunk_size,
            retries=retries,
            max_chunks_before_suspension=max_chunks_before_suspension,
        )
        return out
