"""
Unit test file.
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

_ENABLED = False

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


PORT = 8093

CHUNK_SIZE = "16M"
CHUNK_SIZE_READ_AHEAD = "32M"
CHUNK_SIZE_BYTES = 1000 * 1000 * 16


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


@contextmanager
def rclone_mounted_webdav(
    src_path: str,
    config: Config,
    mount_point: Path,
    port: int | None = None,
) -> Generator[Process, None, None]:
    rclone = Rclone(config)
    port = port or PORT

    with rclone_served_webdav(src_path, config, port):
        other_args: list[str] = [
            "--vfs-cache-mode",
            "off",
            "--vfs-read-chunk-size-limit",
            CHUNK_SIZE_READ_AHEAD,
            "--vfs-read-chunk-size",
            CHUNK_SIZE,
            "--vfs-read-chunk-streams",
            "1",
            "--links",
        ]
        mount_proc = rclone.mount_webdav("webdav:", mount_point, other_args=other_args)
        try:
            yield mount_proc
        finally:
            mount_proc.terminate()
            mount_proc.wait()


# okay let's also make the serving of webdav in a @contextmanager


# # WebDAV server options
# options = {
#     'webdav_hostname': "https://webdav.server.com",
#     'webdav_login':    "your_username",
#     'webdav_password': "your_password"
# }

# # Initialize the client
# client = Client(options)

# # Remote file path on the WebDAV server
# remote_path = '/path/to/remote/file'

# # Local file path where the downloaded content will be saved
# local_path = '/path/to/local/file'

# # Define the range in bytes (e.g., download the first 1024 bytes)
# byte_range = (0, 1023)

# # Open the local file in write-binary mode
# with open(local_path, 'wb') as local_file:
#     # Download the specified byte range
#     response = client.execute_request(
#         'GET',
#         remote_path,
#         headers={'Range': f'bytes={byte_range[0]}-{byte_range[1]}'}
#     )
#     # Write the content to the local file
#     local_file.write(response.content)


def _download_range(
    remote_path: str, local_path: str, byte_range: tuple[int, int]
) -> None:
    # Open the local file in write-binary mode
    options = {
        "webdav_hostname": f"http://localhost:{PORT}/",
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
        print(f"Downloading {remote_path} to {local_path} with range {byte_range}")
        response = client.execute_request(
            "download",
            remote_path,
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

    @unittest.skip("Test is disabled by default")
    def test_serve_webdav_and_mount(self) -> None:
        """Test basic Webdav serve functionality."""
        config = _generate_rclone_config(PORT)
        src_path = "src:aa_misc_data/aa_misc_data/"
        is_windows = os.name == "nt"
        if is_windows:
            mount_path = Path("test_mount2")
        else:
            mount_path = Path("/tmp/test_mount2")
        expected_file = "world_lending_library_2024_11.tar.zst"
        with rclone_mounted_webdav(src_path, config, mount_path):
            self.assertTrue(mount_path.exists())
            # self.assertIsNotNone(next(expected_file).iterdir())
            for file in mount_path.iterdir():
                print(file)
                self.assertTrue(file.exists())
                if file.name == expected_file:
                    break
            else:
                self.fail(f"Expected file {expected_file} not found")

            # Now do a test where the file is downloaded from 0-128MB, then 100GB to 100GB + 128MB
            # This will test the ranged download functionality
            source_file = mount_path / expected_file
            # now do the first read
            import time

            print("Starting first read")
            start_time = time.time()
            with source_file.open("rb") as f:
                print("Seeking to 0")
                f.seek(0)
                print("Reading first chunk")
                first_chunk = f.read(CHUNK_SIZE_BYTES)
                print("First chunk read")
                self.assertEqual(len(first_chunk), CHUNK_SIZE_BYTES)
            print(f"First read took {time.time() - start_time} seconds")

            print("Starting second read")
            start_time = time.time()
            # now do the second read
            with source_file.open("rb") as f:
                print("Seeking to next chunk")
                f.seek(CHUNK_SIZE_BYTES + CHUNK_SIZE_BYTES)
                print("Reading second chunk")
                second_chunk = f.read(CHUNK_SIZE_BYTES)
                print("Second chunk read")
                self.assertEqual(len(second_chunk), CHUNK_SIZE_BYTES)
            print(f"Second read took {time.time() - start_time} seconds")

    def test_serve_webdav(self) -> None:
        """Test basic Webdav serve functionality."""
        config = _generate_rclone_config(PORT)
        src_path = "src:aa_misc_data/aa_misc_data/"
        target_file = "/world_lending_library_2024_11.tar.zst"
        with rclone_served_webdav(src_path, config, PORT):
            start_time = time.time()
            byte_range = (0, 1024 * 1024 * 16)
            _download_range(target_file, "test_mount2/chunk1", byte_range)
            print(f"Download took {time.time() - start_time} seconds")
            # offset byte range by 100GB
            offset = 1000 * 1000 * 100
            byte_range = byte_range[0] + offset, byte_range[1] + offset
            print("Second download")
            start_time = time.time()

            _download_range(target_file, "test_mount2/chunk2", byte_range)
            print(f"Second download took {time.time() - start_time} seconds")

        print("Done")


if __name__ == "__main__":
    unittest.main()
