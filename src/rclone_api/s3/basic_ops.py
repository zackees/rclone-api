from pathlib import Path

from botocore.client import BaseClient


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
    file_path: Path,
    object_name: str,
) -> Exception | None:
    """Upload a file to the bucket."""
    try:
        s3_client.upload_file(str(file_path), bucket_name, object_name)
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


def head(s3_client: BaseClient, bucket_name: str, object_name: str) -> dict | None:
    """
    Retrieve metadata for the specified object using a HEAD operation.

    :param s3_client: The S3 client to use.
    :param bucket_name: The name of the bucket containing the object.
    :param object_name: The key of the object.
    :return: A dictionary containing the object's metadata if successful, otherwise None.
    """
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_name)
        print(f"Metadata for {object_name} in {bucket_name}: {response}")
        return response
    except Exception as e:
        print(f"Error retrieving metadata for {object_name}: {e}")
        return None
