import warnings

from botocore.client import BaseClient

from rclone_api.s3.basic_ops import (
    download_file,
    head,
    list_bucket_contents,
    upload_file,
)
from rclone_api.s3.chunk_uploader import MultiUploadResult, upload_file_multipart
from rclone_api.s3.create import create_s3_client
from rclone_api.s3.types import S3Credentials, S3MutliPartUploadConfig, S3UploadTarget

_MIN_THRESHOLD_FOR_CHUNKING = 5 * 1024 * 1024


class S3Client:
    def __init__(self, credentials: S3Credentials):
        self.credentials: S3Credentials = credentials
        self.client: BaseClient = create_s3_client(credentials)

    def list_bucket_contents(self, bucket_name: str) -> None:
        list_bucket_contents(self.client, bucket_name)

    def upload_file(self, target: S3UploadTarget) -> Exception | None:
        bucket_name = target.bucket_name
        file_path = target.src_file
        object_name = target.s3_key
        return upload_file(
            s3_client=self.client,
            bucket_name=bucket_name,
            file_path=file_path,
            object_name=object_name,
        )

    def download_file(self, bucket_name: str, object_name: str, file_path: str) -> None:
        download_file(self.client, bucket_name, object_name, file_path)

    def head(self, bucket_name: str, object_name: str) -> dict | None:
        return head(self.client, bucket_name, object_name)

    def upload_file_multipart(
        self,
        upload_target: S3UploadTarget,
        upload_config: S3MutliPartUploadConfig,
    ) -> MultiUploadResult:
        filesize = upload_target.src_file.stat().st_size
        if filesize < _MIN_THRESHOLD_FOR_CHUNKING:
            warnings.warn(
                f"File size {filesize} is less than the minimum threshold for chunking ({_MIN_THRESHOLD_FOR_CHUNKING}), switching to single threaded upload."
            )
            err = self.upload_file(upload_target)
            if err:
                raise err
            return MultiUploadResult.UPLOADED_FRESH
        chunk_size = upload_config.chunk_size
        retries = upload_config.retries
        resume_path_json = upload_config.resume_path_json
        max_chunks_before_suspension = upload_config.max_chunks_before_suspension
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
