"""
Unit test file.

Notes:

We want seekable writes, but since s3 doesn't allow appends, we can't use the full cache mode.
--vfs-cache-mode writes

Seekable Reads:
--vfs-cache-mode minimal

we cannot use:
--vfs-cache-mode full

Because it will place a 9TB file in the cache directory.


Workflow:

  1. Develop on local machine but test on remote machine.
  2. Mount read only remote
  3. S3 API for upload
    -> Very certain mounting seek doesn't really work for S3


Mount target:
  rclone --config rclone.conf mount 45061:/aa_misc_data/aa_misc_data/ mount
  file -> world_lending_library_2024_11.tar.zst

"""

import os
import unittest

# context lib
from pathlib import Path

from dotenv import load_dotenv

from rclone_api import Rclone
from rclone_api.s3.api import S3Client
from rclone_api.s3.types import (
    MultiUploadResult,
    S3Credentials,
    S3MutliPartUploadConfig,
    S3Provider,
    S3UploadTarget,
)

_HERE = Path(__file__).parent
_PROJECT_ROOT = _HERE.parent
_CONFIG_PATH = _PROJECT_ROOT / "rclone-mounted-ranged-download.conf"

_CHUNK_SIZE = 1024 * 1024 * 16

_CHUNK_SIZE *= 10

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")  # Default if not in .env


def _generate_rclone_config(port: int) -> str:

    # BUCKET_NAME = os.getenv("BUCKET_NAME", "TorrentBooks")  # Default if not in .env

    # Load additional environment variables
    BUCKET_KEY_SECRET = os.getenv("BUCKET_KEY_SECRET")
    BUCKET_KEY_PUBLIC = os.getenv("BUCKET_KEY_PUBLIC")
    SRC_SFTP_HOST = os.getenv("SRC_SFTP_HOST")
    SRC_SFTP_USER = os.getenv("SRC_SFTP_USER")
    SRC_SFTP_PORT = os.getenv("SRC_SFTP_PORT")
    SRC_SFTP_PASS = os.getenv("SRC_SFTP_PASS")
    # BUCKET_URL = os.getenv("BUCKET_URL")
    BUCKET_URL = "sfo3.digitaloceanspaces.com"

    config_text = f"""
[dst]
type = s3
provider = DigitalOcean
access_key_id = {BUCKET_KEY_PUBLIC}
secret_access_key = {BUCKET_KEY_SECRET}
endpoint = {BUCKET_URL}
bucket = {BUCKET_NAME}

[src]
type = sftp
host = {SRC_SFTP_HOST}
user = {SRC_SFTP_USER}
port = {SRC_SFTP_PORT}
pass = {SRC_SFTP_PASS}


[webdav]
type = webdav
user = guest
# obscured password for "1234", use Rclone.obscure("1234") to generate
pass = d4IbQLV9W0JhI2tm5Zp88hpMtEg
url = http://localhost:{port}
vendor = rclone
"""
    # _CONFIG_PATH.write_text(config_text, encoding="utf-8")
    # print(f"Config file written to: {_CONFIG_PATH}")
    return config_text


PORT = 8095


class RcloneMountWebdavTester(unittest.TestCase):
    """Test rclone functionality."""

    def setUp(self) -> None:
        """Check if all required environment variables are set before running tests."""
        required_vars = [
            "BUCKET_NAME",
            "BUCKET_KEY_SECRET",
            "BUCKET_KEY_PUBLIC",
            "BUCKET_URL",
        ]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            self.skipTest(
                f"Missing required environment variables: {', '.join(missing)}"
            )
        os.environ["RCLONE_API_VERBOSE"] = "1"

    # @unittest.skipIf(not _ENABLED, "Test not enabled")
    # @unittest.skipIf(True, "Test not enabled")
    def test_upload_chunks(self) -> None:
        """Test basic Webdav serve functionality."""
        # config = _generate_rclone_config(PORT)
        BUCKET_NAME: str | None = os.getenv("B2_BUCKET_NAME")
        ACCESS_KEY_ID: str | None = os.getenv("B2_ACCESS_KEY_ID")
        SECRET_ACCESS_KEY: str | None = os.getenv("B2_SECRET_ACCESS_KEY")
        ENDPOINT_URL: str | None = os.getenv("B2_ENDPOINT_URL")
        assert BUCKET_NAME
        assert ACCESS_KEY_ID
        assert SECRET_ACCESS_KEY
        assert ENDPOINT_URL
        print(f"BUCKET_KEY_SECRET: {SECRET_ACCESS_KEY}")
        config_text = _generate_rclone_config(PORT)
        _CONFIG_PATH.write_text(config_text, encoding="utf-8")
        print(f"Config file written to: {_CONFIG_PATH}")
        src_path = "src:aa_misc_data/aa_misc_data/"
        # target_file = "world_lending_library_2024_11.tar.zst"
        # from concurrent.futures import ThreadPoolExecutor

        OUT_DIR = Path("mount")
        other_args = [
            "--vfs-read-chunk-size",
            "16M",
            "--vfs-read-chunk-size-limit",
            "1G",
            "--vfs-read-chunk-streams",
            "64",
            "--no-modtime",
            "--vfs-read-wait",
            "1m",
            "--vfs-fast-fingerprint",
        ]

        rclone = Rclone(_CONFIG_PATH)
        with rclone.scoped_mount(
            src=src_path,
            outdir=OUT_DIR,
            vfs_cache_mode="minimal",
            other_args=other_args,
        ):
            print(f"Mounted at: {OUT_DIR}")

            credentials = S3Credentials(
                provider=S3Provider.BACKBLAZE,
                access_key_id=ACCESS_KEY_ID,
                secret_access_key=SECRET_ACCESS_KEY,
                endpoint_url=ENDPOINT_URL,
            )

            bucket_name = BUCKET_NAME
            assert bucket_name is not None

            # Create upload target
            target = S3UploadTarget(
                src_file=Path("mount/world_lending_library_2024_11.tar.zst.torrent"),
                bucket_name=bucket_name,
                # s3_key="aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst.torrent",
                s3_key="test_data/world_lending_library_2024_11.tar.zst.torrent",
            )

            config: S3MutliPartUploadConfig = S3MutliPartUploadConfig(
                chunk_size=_CHUNK_SIZE,
                retries=0,
                resume_path_json=Path("state.json"),
                max_chunks_before_suspension=1,
            )

            s3_client = S3Client(credentials)
            rslt: MultiUploadResult = s3_client.upload_file_multipart(
                upload_target=target, upload_config=config
            )
            print(f"Upload result: {rslt}")

        print("Done")


if __name__ == "__main__":
    unittest.main()
