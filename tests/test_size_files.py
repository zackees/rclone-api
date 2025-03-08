"""
Unit test file.
"""

import os
import unittest

from dotenv import load_dotenv

from rclone_api import Config, DirListing, Rclone, SizeResult

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


class RcloneSizeFilesTester(unittest.TestCase):
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

    def test_size(self) -> None:
        rclone = Rclone(_generate_rclone_config())
        # rclone.walk
        dirlisting: DirListing
        is_first = True
        files: list[str] = []
        src = f"dst:{BUCKET_NAME}"
        for dirlisting in rclone.walk(src, max_depth=1):
            if is_first:
                self.assertGreaterEqual(len(dirlisting.files), 1)
                self.assertEqual(dirlisting.files[0].name, "first.txt")
                is_first = False
            for file in dirlisting.files_relative(src):
                files.append(file)
        size_map: SizeResult = rclone.size_files(src=src, files=files, check=True)
        print(size_map)
        print("done")


if __name__ == "__main__":
    unittest.main()
