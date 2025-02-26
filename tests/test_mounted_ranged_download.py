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
import time

# context lib
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from dotenv import load_dotenv

from rclone_api import Config, Process, Rclone
from rclone_api.s3_multi_chunk_uploader import upload_file, S3Credentials, S3UploadTarget

_HERE = Path(__file__).parent
_PROJECT_ROOT = _HERE.parent
_CONFIG_PATH = _PROJECT_ROOT / "rclone-mounted-ranged-download.conf"

_IS_WINDOWS = os.name == "nt"

_ENABLED = not _IS_WINDOWS
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


@contextmanager
def rclone_served_webdav(
    src_path: str,
    config: Config,
    port: int | None = None,
) -> Generator[Process, None, None]:
    rclone = Rclone(config)
    port = port or PORT

    test_addr = f"localhost:{port}"
    user = "guest"
    password = "1234"
    process = rclone.serve_webdav(
        src_path,
        addr=test_addr,
        user=user,
        password=password,
    )
    try:
        yield process
    finally:
        process.terminate()
        process.wait()


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
        proc: Process = rclone.mount(
            src=src_path,
            outdir=OUT_DIR,
            vfs_cache_mode="minimal",
            other_args=other_args,
        )

        try:
            # Create credentials from environment variables
            credentials = S3Credentials(
                access_key_id=os.getenv("BUCKET_KEY_PUBLIC"),
                secret_access_key=os.getenv("BUCKET_KEY_SECRET"),
                #endpoint_url=f"https://{os.getenv('BUCKET_URL')}"
            )
            
            # Create upload target
            target = S3UploadTarget(
                file_path="mount/world_lending_library_2024_11.tar.zst",
                bucket_name=BUCKET_NAME,
                s3_key="aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst",
            )
            
            # Call the updated upload_file function with the new parameters
            upload_file(credentials=credentials, target=target)

        finally:
            proc.terminate()
            proc.wait()

        print("Done")


if __name__ == "__main__":
    unittest.main()
