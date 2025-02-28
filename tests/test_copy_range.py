"""
Unit test file.
"""

import os
import unittest

from dotenv import load_dotenv

from rclone_api import Config, Rclone

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")  # Default if not in .env


def _generate_rclone_config() -> Config:

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
"""

    out = Config(config_text)
    return out


class RcloneCopyBytesTester(unittest.TestCase):
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

    def test_copy_bytes(self) -> None:
        rclone = Rclone(_generate_rclone_config())
        bytes_or_err: bytes | Exception = rclone.copy_bytes(
            src="dst:rclone-api-unit-test/zachs_video/breaking_ai_mind.mp4",
            offset=0,
            length=1024 * 1024,
        )
        if isinstance(bytes_or_err, Exception):
            print(bytes_or_err)
            self.fail(f"Error: {bytes_or_err}")
        assert isinstance(bytes_or_err, bytes)
        self.assertEqual(
            len(bytes_or_err), 1024 * 1024
        )  # , f"Length: {len(bytes_or_err)}"


if __name__ == "__main__":
    unittest.main()
