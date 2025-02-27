import warnings

import boto3
from botocore.client import BaseClient
from botocore.config import Config

from rclone_api.s3.types import S3Credentials, S3Provider


# Create a Boto3 session and S3 client, this is back blaze specific.
# Add a function if you want to use a different S3 provider.
# If AWS support is added in a fork then please merge it back here.
def _create_backblaze_s3_client(
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


def create_s3_client(credentials: S3Credentials) -> BaseClient:
    """Create and return an S3 client."""
    provider = credentials.provider
    access_key = credentials.access_key_id
    secret_key = credentials.secret_access_key
    endpoint_url = credentials.endpoint_url
    region_name = credentials.region_name
    if provider == S3Provider.BACKBLAZE:
        if region_name is not None:
            warnings.warn(f"Region name is not used for provider: {provider}")
        return _create_backblaze_s3_client(access_key, secret_key, endpoint_url)
    else:
        raise NotImplementedError(f"Provider not implemented: {provider}")
