"""
Unit test file.
"""

import os
import unittest

from dotenv import load_dotenv

from rclone_api import Config, DirListing, Rclone

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


class RcloneWalkTest(unittest.TestCase):
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

    def test_walk(self) -> None:
        rclone = Rclone(_generate_rclone_config())
        # rclone.walk
        dirlisting: DirListing
        is_first = True
        for dirlisting in rclone.walk(f"dst:{BUCKET_NAME}", max_depth=1):
            if is_first:
                # assert just one file
                # assert len(dirlisting.files) == 1
                self.assertEqual(len(dirlisting.files), 1)
                # assert it's first.txt
                self.assertEqual(dirlisting.files[0].name, "first.txt")
                is_first = False
            print(dirlisting)
        print("done")

    def test_walk_depth_first(self) -> None:
        rclone = Rclone(_generate_rclone_config())
        # rclone.walk
        dirlisting: DirListing
        for dirlisting in rclone.walk(
            f"dst:{BUCKET_NAME}", max_depth=1, breadth_first=False
        ):
            print(dirlisting)
        print("done")


if __name__ == "__main__":
    unittest.main()
