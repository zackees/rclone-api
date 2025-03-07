"""
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_part_copy.html
  *  client.upload_part_copy


"""

# import _thread
# import os
# import traceback
# import warnings
# from concurrent.futures import Future, ThreadPoolExecutor
# from pathlib import Path
# from queue import Queue
# from threading import Event, Thread
# from typing import Any, Callable

# from botocore.client import BaseClient

# from rclone_api.mount_read_chunker import FilePart
# from rclone_api.s3.chunk_task import S3FileInfo, file_chunker
# from rclone_api.s3.chunk_types import (
#     FinishedPiece,
#     UploadInfo,
#     UploadState,
# )
# from rclone_api.s3.types import MultiUploadResult
# from rclone_api.types import EndOfStream
# from rclone_api.util import locked_print


# This is how you upload large parts through multi part upload, then the final call
# is to assemble the parts that have already been uploaded through a multi part uploader
# and then call complete_multipart_upload to finish the upload
# response = (
#  client.upload_part_copy(
#     Bucket='string',
#     CopySource='string' or {'Bucket': 'string', 'Key': 'string', 'VersionId': 'string'},
#     CopySourceIfMatch='string',
#     CopySourceIfModifiedSince=datetime(2015, 1, 1),
#     CopySourceIfNoneMatch='string',
#     CopySourceIfUnmodifiedSince=datetime(2015, 1, 1),
#     CopySourceRange='string',
#     Key='string',
#     PartNumber=123,
#     UploadId='string',
#     SSECustomerAlgorithm='string',
#     SSECustomerKey='string',
#     CopySourceSSECustomerAlgorithm='string',
#     CopySourceSSECustomerKey='string',
#     RequestPayer='requester',
#     ExpectedBucketOwner='string',
#     ExpectedSourceBucketOwner='string'
# )


# def upload_task(
#     info: UploadInfo,
#     chunk: FilePart,
#     part_number: int,
#     retries: int,
# ) -> FinishedPiece:
#     file_or_err: Path | Exception = chunk.get_file()
#     if isinstance(file_or_err, Exception):
#         raise file_or_err
#     file: Path = file_or_err
#     size = os.path.getsize(file)
#     retries = retries + 1  # Add one for the initial attempt
#     for retry in range(retries):
#         try:
#             if retry > 0:
#                 locked_print(f"Retrying part {part_number} for {info.src_file_path}")
#             locked_print(
#                 f"Uploading part {part_number} for {info.src_file_path} of size {size}"
#             )

#             with open(file, "rb") as f:
#                 part = info.s3_client.upload_part(
#                     Bucket=info.bucket_name,
#                     Key=info.object_name,
#                     PartNumber=part_number,
#                     UploadId=info.upload_id,
#                     Body=f,
#                 )
#                 out: FinishedPiece = FinishedPiece(
#                     etag=part["ETag"], part_number=part_number
#                 )
#             chunk.dispose()
#             return out
#         except Exception as e:
#             if retry == retries - 1:
#                 locked_print(f"Error uploading part {part_number}: {e}")
#                 chunk.dispose()
#                 raise e
#             else:
#                 locked_print(f"Error uploading part {part_number}: {e}, retrying")
#                 continue
#     raise Exception("Should not reach here")


# def prepare_upload_file_multipart(
#     s3_client: BaseClient,
#     bucket_name: str,
#     file_path: Path,
#     file_size: int | None,
#     object_name: str,
#     chunk_size: int,
#     retries: int,
# ) -> UploadInfo:
#     """Upload a file to the bucket using multipart upload with customizable chunk size."""

#     # Initiate multipart upload
#     locked_print(
#         f"Creating multipart upload for {file_path} to {bucket_name}/{object_name}"
#     )
#     mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_name)
#     upload_id = mpu["UploadId"]

#     file_size = file_size if file_size is not None else os.path.getsize(file_path)

#     upload_info: UploadInfo = UploadInfo(
#         s3_client=s3_client,
#         bucket_name=bucket_name,
#         object_name=object_name,
#         src_file_path=file_path,
#         upload_id=upload_id,
#         retries=retries,
#         chunk_size=chunk_size,
#         file_size=file_size,
#     )
#     return upload_info

# class S3MultiPartUploader:
#     def __init__(self, s3_client: BaseClient, verbose: bool) -> None:
#         self.s3_client = s3_client
#         self.verbose = verbose

#     def prepare(self) -> UploadInfo:
