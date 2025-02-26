import os
from typing import Optional

import boto3
from botocore.client import BaseClient
from botocore.config import Config


# Create a Boto3 session and S3 client, this is back blaze specific.
# Add a function if you want to use a different S3 provider.
# If AWS support is added in a fork then please merge it back here.
def create_backblaze_s3_client(
    access_key: str, secret_key: str, endpoint_url: str | None
) -> BaseClient:
    """Create and return an S3 client."""
    session = boto3.session.Session()  # type: ignore
    return session.client(
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        config=Config(
            signature_version="s3v4",
            s3={"payload_signing_enabled": False},  # Disable checksum header
        ),
    )


def list_bucket_contents(s3_client: BaseClient, bucket_name: str) -> None:
    """List contents of the specified bucket."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            for obj in response["Contents"]:
                print(f"File: {obj['Key']} | Size: {obj['Size']} bytes")
        else:
            print(f"The bucket '{bucket_name}' is empty.")
    except Exception as e:
        print(f"Error listing bucket contents: {e}")


def upload_file(
    s3_client: BaseClient,
    bucket_name: str,
    file_path: str,
    object_name: Optional[str] = None,
) -> Exception | None:
    """Upload a file to the bucket."""
    try:
        object_name = object_name or file_path.split("/")[-1]
        result = s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"The result is {result}, which is type {type(result)}")
        print(f"Uploaded {file_path} to {bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error uploading file: {e}")
        return e
    return None


def download_file(
    s3_client: BaseClient, bucket_name: str, object_name: str, file_path: str
) -> None:
    """Download a file from the bucket."""
    try:
        s3_client.download_file(bucket_name, object_name, file_path)
        print(f"Downloaded {object_name} from {bucket_name} to {file_path}")
    except Exception as e:
        print(f"Error downloading file: {e}")


def upload_file_multipart(
    s3_client: BaseClient,
    bucket_name: str,
    file_path: str,
    object_name: Optional[str] = None,
    chunk_size: int = 5 * 1024 * 1024,  # Default chunk size is 5MB; can be overridden
) -> None:
    """Upload a file to the bucket using multipart upload with customizable chunk size."""

    object_name = object_name or os.path.basename(file_path)
    upload_id = None
    exceptions: list[Exception] = []
    try:
        # Initiate multipart upload
        mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_name)
        upload_id = mpu["UploadId"]

        parts = []
        part_number = 1

        with open(file_path, "rb") as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                part = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=object_name,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data,
                )
                parts.append({"ETag": part["ETag"], "PartNumber": part_number})
                part_number += 1

        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_name,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        print(f"Multipart upload completed: {file_path} to {bucket_name}/{object_name}")
    except Exception as e:
        exceptions.append(e)
        print(f"Error during multipart upload: {e}")
        if upload_id:
            try:
                s3_client.abort_multipart_upload(
                    Bucket=bucket_name, Key=object_name, UploadId=upload_id
                )
            except Exception:
                pass
    if exceptions:
        all_errors = "\n".join([str(e) for e in exceptions])
        print(f"Errors during multipart upload: {all_errors}")
        raise Exception("Errors during multipart upload", all_errors)
