import warnings

import boto3
from botocore.client import BaseClient
from botocore.config import Config

from rclone_api.s3.types import S3Credentials, S3Provider

_DEFAULT_BACKBLAZE_ENDPOINT = "https://s3.us-west-002.backblazeb2.com"


# Create a Boto3 session and S3 client, this is back blaze specific.
# Add a function if you want to use a different S3 provider.
# If AWS support is added in a fork then please merge it back here.
def _create_backblaze_s3_client(creds: S3Credentials, verbose: bool) -> BaseClient:
    """Create and return an S3 client."""
    region_name = creds.region_name
    access_key = creds.access_key_id
    secret_key = creds.secret_access_key
    endpoint_url = creds.endpoint_url
    endpoint_url = endpoint_url or _DEFAULT_BACKBLAZE_ENDPOINT
    session = boto3.session.Session()  # type: ignore
    return session.client(
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        verify=False,  # Disables SSL certificate verification
        config=Config(
            signature_version="s3v4",
            region_name=region_name,
            # Note that BackBlase has a boko3 bug where it doesn't support the new
            # checksum header, the following line was an attempt of fix it on the newest
            # version of boto3, but it didn't work.
            s3={"payload_signing_enabled": False},  # Disable checksum header
        ),
    )


def _create_unknown_s3_client(creds: S3Credentials, verbose: bool) -> BaseClient:
    """Create and return an S3 client."""
    access_key = creds.access_key_id
    secret_key = creds.secret_access_key
    endpoint_url = creds.endpoint_url
    if (endpoint_url is not None) and not (endpoint_url.startswith("http")):
        if verbose:
            warnings.warn(
                f"Endpoint URL is schema naive: {endpoint_url}, assuming HTTPS"
            )
        endpoint_url = f"https://{endpoint_url}"

    session = boto3.session.Session()  # type: ignore
    return session.client(
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        config=Config(
            signature_version="s3v4",
            region_name=creds.region_name,
            # Note that BackBlase has a boko3 bug where it doesn't support the new
            # checksum header, the following line was an attempt of fix it on the newest
            # version of boto3, but it didn't work.
            # s3={"payload_signing_enabled": False},  # Disable checksum header
        ),
    )


def create_s3_client(credentials: S3Credentials, verbose=False) -> BaseClient:
    """Create and return an S3 client."""
    provider = credentials.provider
    if provider == S3Provider.BACKBLAZE:
        if verbose:
            print("Creating BackBlaze S3 client")
        return _create_backblaze_s3_client(creds=credentials, verbose=verbose)
    else:
        if verbose:
            print("Creating generic/unknown S3 client")
        return _create_unknown_s3_client(creds=credentials, verbose=verbose)
