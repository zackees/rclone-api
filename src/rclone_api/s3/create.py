import warnings
from dataclasses import dataclass

import boto3
from botocore.client import BaseClient
from botocore.config import Config

from rclone_api.s3.types import S3Credentials, S3Provider

_DEFAULT_BACKBLAZE_ENDPOINT = "https://s3.us-west-002.backblazeb2.com"
_MAX_CONNECTIONS = 10
_TIMEOUT_READ = 120
_TIMEOUT_CONNECT = 60


@dataclass
class S3Config:
    max_pool_connections: int | None = None
    timeout_connection: int | None = None
    timeout_read: int | None = None
    verbose: bool | None = None

    def resolve_defaults(self) -> None:
        self.max_pool_connections = self.max_pool_connections or _MAX_CONNECTIONS
        self.timeout_connection = self.timeout_connection or _TIMEOUT_CONNECT
        self.timeout_read = self.timeout_read or _TIMEOUT_READ
        self.verbose = self.verbose or False


# Create a Boto3 session and S3 client, this is back blaze specific.
# Add a function if you want to use a different S3 provider.
# If AWS support is added in a fork then please merge it back here.
def _create_backblaze_s3_client(
    s3_creds: S3Credentials, s3_config: S3Config
) -> BaseClient:
    """Create and return an S3 client."""
    region_name = s3_creds.region_name
    access_key = s3_creds.access_key_id
    secret_key = s3_creds.secret_access_key
    endpoint_url = s3_creds.endpoint_url
    endpoint_url = endpoint_url or _DEFAULT_BACKBLAZE_ENDPOINT
    s3_config.resolve_defaults()
    session = boto3.session.Session()  # type: ignore
    return session.client(
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        # verify=False,  # Disables SSL certificate verification
        config=Config(
            signature_version="s3v4",
            region_name=region_name,
            max_pool_connections=s3_config.max_pool_connections,
            read_timeout=s3_config.timeout_read,
            connect_timeout=s3_config.timeout_connection,
            # Note that BackBlase has a boko3 bug where it doesn't support the new
            # checksum header, the following line was an attempt of fix it on the newest
            # version of boto3, but it didn't work.
            s3={"payload_signing_enabled": False},  # Disable checksum header
        ),
    )


def _create_unknown_s3_client(
    s3_creds: S3Credentials, s3_config: S3Config
) -> BaseClient:
    """Create and return an S3 client."""
    access_key = s3_creds.access_key_id
    secret_key = s3_creds.secret_access_key
    endpoint_url = s3_creds.endpoint_url
    if (endpoint_url is not None) and not (endpoint_url.startswith("http")):
        if s3_config.verbose:
            warnings.warn(
                f"Endpoint URL is schema naive: {endpoint_url}, assuming HTTPS"
            )
        endpoint_url = f"https://{endpoint_url}"
    s3_config.resolve_defaults()
    session = boto3.session.Session()  # type: ignore
    return session.client(
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        config=Config(
            signature_version="s3v4",
            region_name=s3_creds.region_name,
            max_pool_connections=s3_config.max_pool_connections,
            read_timeout=s3_config.timeout_read,
            connect_timeout=s3_config.timeout_connection,
        ),
    )


def create_s3_client(
    s3_creds: S3Credentials, s3_config: S3Config | None = None
) -> BaseClient:
    """Create and return an S3 client."""
    s3_config = s3_config or S3Config()
    provider = s3_creds.provider
    if provider == S3Provider.BACKBLAZE:
        if s3_config.verbose:
            print("Creating BackBlaze S3 client")
        return _create_backblaze_s3_client(s3_creds=s3_creds, s3_config=s3_config)
    else:
        if s3_config.verbose:
            print("Creating generic/unknown S3 client")
        return _create_unknown_s3_client(s3_creds=s3_creds, s3_config=s3_config)
