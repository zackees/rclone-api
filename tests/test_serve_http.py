"""
Unit test file for testing rclone mount functionality.
"""

import os
import subprocess
import unittest
from pathlib import Path

from dotenv import load_dotenv

from rclone_api import Config, Process, Rclone
from rclone_api.http_server import HttpServer

load_dotenv()


def _generate_rclone_config() -> Config:
    # Load environment variables
    BUCKET_KEY_SECRET = os.getenv("BUCKET_KEY_SECRET")
    BUCKET_KEY_PUBLIC = os.getenv("BUCKET_KEY_PUBLIC")
    BUCKET_URL = "sfo3.digitaloceanspaces.com"

    config_text = f"""
[dst]
type = s3
provider = DigitalOcean
access_key_id = {BUCKET_KEY_PUBLIC}
secret_access_key = {BUCKET_KEY_SECRET}
endpoint = {BUCKET_URL}
"""
    return Config(config_text)


class RcloneServeHttpTester(unittest.TestCase):
    """Test rclone mount functionality."""

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

        self.bucket_name = os.getenv("BUCKET_NAME")
        self.mount_point = Path("test_mount")
        # Create mount point directory if it doesn't exist
        # self.mount_point.mkdir(exist_ok=True)
        # make parents
        parent = self.mount_point.parent
        if not parent.exists():
            parent.mkdir(parents=True)

        os.environ["RCLONE_API_VERBOSE"] = "1"
        self.rclone = Rclone(_generate_rclone_config())

    def test_server_http(self) -> None:
        """Test mounting a remote bucket."""
        remote_path = f"dst:{self.bucket_name}"
        process: Process | None = None
        http_server: HttpServer | None = None

        try:
            # Start the mount process
            process = self.rclone.serve_http(remote_path)
            http_server = HttpServer(process=process, url="http://localhost:8080")

            # url = "http://localhost:8080"
            # response = httpx.get(url)
            # print(f"Response: {response}")
            # print("done")

            content: bytes | Exception = http_server.get("first.txt")
            print(f"Content: {str(content)}")
            self.assertIsInstance(content, bytes)

            content = http_server.get("first.txt")
            print(content)
            print("done")

        except subprocess.CalledProcessError as e:
            self.fail(f"Mount operation failed: {str(e)}")
        finally:
            # Cleanup will happen in tearDown
            if process:
                if process.poll() is None:
                    process.kill()
                stdout = process.stdout
                if stdout:
                    # stdout is a buffered reader
                    for line in stdout:
                        print(line)
                stderr = process.stderr
                if stderr:
                    for line in stderr:
                        print(line)
                process.kill()


if __name__ == "__main__":
    unittest.main()
