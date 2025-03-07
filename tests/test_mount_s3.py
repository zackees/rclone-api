"""
Unit test file for testing rclone mount functionality.
"""

import os
import subprocess
import unittest
from pathlib import Path

from dotenv import load_dotenv

from rclone_api import Config, Process, Rclone

load_dotenv()

_ENABLED = False


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


class RcloneMountS3Tests(unittest.TestCase):
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

    @unittest.skipUnless(_ENABLED, "Test is disabled by default")
    def test_mount(self) -> None:
        """Test mounting a remote bucket."""
        remote_path = f"dst:{self.bucket_name}"
        process: Process | None = None

        try:
            # Start the mount process
            mount = self.rclone.impl.mount_s3(remote_path, self.mount_point)
            process = mount.process
            assert process
            self.assertIsNone(
                process.poll(), "Mount process should still be running after 2 seconds"
            )

            # Verify mount point exists and is accessible
            self.assertTrue(self.mount_point.exists())
            self.assertTrue(self.mount_point.is_dir())

            # Check if we can list contents
            contents = list(self.mount_point.iterdir())
            self.assertGreater(
                len(contents), 0, "Mounted directory should not be empty"
            )

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
