"""
Unit test file.

Notes:

We want seekable writes.
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

"""

import os
import time
import unittest

# context lib
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from dotenv import load_dotenv
from webdav3.client import Client

from rclone_api import Config, Process, Rclone

_IS_WINDOWS = os.name == "nt"

_ENABLED = not _IS_WINDOWS
_CHUNK_SIZE = 1024 * 1024 * 16

_CHUNK_SIZE *= 10

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")  # Default if not in .env


def _generate_rclone_config(port: int) -> Config:

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

    out = Config(config_text)
    return out


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


def _download_range(
    file_path: str, local_path: str, byte_range: tuple[int, int], port: int
) -> None:
    # Open the local file in write-binary mode
    options = {
        "webdav_hostname": f"http://localhost:{port}/",
        "webdav_login": "guest",
        "webdav_password": "1234",
        "webdav_timeout": 600,
    }
    header_list: list[str] = [
        # range
        # "Range: bytes=0-1000",
        f"Range: bytes={byte_range[0]}-{byte_range[1]}",
    ]
    client = Client(options)
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as local_file:
        # Download the specified byte range
        print(f"Downloading {file_path} to {local_path} with range {byte_range}")
        response = client.execute_request(
            "download",
            file_path,
            headers_ext=header_list,
        )
        byte_content: bytes = response.content
        assert isinstance(byte_content, bytes)
        # Write the content to the local file
        local_file.write(byte_content)


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
    @unittest.skipIf(True, "Test not enabled")
    def test_serve_webdav(self) -> None:
        """Test basic Webdav serve functionality."""
        config = _generate_rclone_config(PORT)
        src_path = "src:aa_misc_data/aa_misc_data/"
        target_file = "/world_lending_library_2024_11.tar.zst"
        from concurrent.futures import ThreadPoolExecutor

        def task(
            remote_file: str, local_file: str, byte_range: tuple[int, int], port: int
        ) -> None:
            start_time = time.time()
            _download_range(remote_file, local_file, byte_range, port)
            print(f"Download took {time.time() - start_time} seconds")

        with ThreadPoolExecutor() as executor:
            if False:
                print(executor)
            # futures: list[Future] = []
            with rclone_served_webdav(src_path, config, PORT):
                with rclone_served_webdav(src_path, config, PORT + 1):
                    # Download the first 16MB of the file
                    print("First download")
                    # start_time = time.time()
                    byte_range = (0, _CHUNK_SIZE)
                    # _download_range(target_file, "test_mount2/chunk1", byte_range)
                    # print(f"Download took {time.time() - start_time} seconds")
                    # fut = executor.submit(
                    #     task, target_file, "test_mount2/chunk1", byte_range, PORT
                    # )

                    # futures.append(fut)

                    task(target_file, "test_mount2/chunk1", byte_range, PORT)

                    offset = 1000 * 1000 * 1000 * 150
                    # offset =
                    byte_range = byte_range[0] + offset, byte_range[1] + offset

                    # fut = executor.submit(
                    #     task, target_file, "test_mount2/chunk2", byte_range, PORT + 1
                    # )
                    # futures.append(fut)

                    task(target_file, "test_mount2/chunk2", byte_range, PORT + 1)

                    # for fut in futures:
                    #     fut.result()

                # # offset byte range by 100GB

                # print("Second download")
                # start_time = time.time()

                # _download_range(target_file, "test_mount2/chunk2", byte_range)
                # print(f"Second download took {time.time() - start_time} seconds")

        print("Done")


if __name__ == "__main__":
    unittest.main()
