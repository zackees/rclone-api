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
            # s3={"payload_signing_enabled": False},  # Disable checksum header
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
        s3_client.upload_file(file_path, bucket_name, object_name)
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
