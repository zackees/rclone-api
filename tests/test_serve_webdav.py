"""
Unit test file.
"""

import os
import unittest

import httpx
from dotenv import load_dotenv

from rclone_api import Config, Process, Rclone, Remote

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")  # Default if not in .env


def _generate_rclone_config(port: int) -> Config:

    # BUCKET_NAME = os.getenv("BUCKET_NAME", "TorrentBooks")  # Default if not in .env

    # Load additional environment variables
    BUCKET_KEY_SECRET = os.getenv("BUCKET_KEY_SECRET")
    BUCKET_KEY_PUBLIC = os.getenv("BUCKET_KEY_PUBLIC")
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


class RcloneServeWebdavTester(unittest.TestCase):
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

    def test_serve_webdav(self) -> None:
        """Test basic NFS serve functionality."""
        port = 8089
        config = _generate_rclone_config(port)
        rclone = Rclone(config)

        # Start NFS server for the remote
        remote = Remote("dst", rclone=rclone)
        # serve = Remote("webdav", rclone=rclone)
        test_addr = f"localhost:{port}"
        user = "guest"
        password = "1234"

        process = rclone.serve_webdav(
            f"{remote.name}:{BUCKET_NAME}", addr=test_addr, user=user, password=password
        )
        mount_proc: Process | None = None

        try:
            # Verify process is running
            self.assertIsNone(process.poll())
            response = httpx.get(f"http://{test_addr}/", auth=(user, password))
            # Note that windows is kinda broken and returns internal server error
            is_serving = response.status_code == 200
            self.assertTrue(is_serving)

        finally:
            # Clean up
            process.terminate()
            process.wait()
            if mount_proc:
                mount_proc.terminate()
                mount_proc.wait()

        # Verify process terminated
        self.assertIsNotNone(process.poll())


if __name__ == "__main__":
    unittest.main()
