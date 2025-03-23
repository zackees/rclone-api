"""
Unit test file.
"""

import os
import unittest

from dotenv import load_dotenv

from rclone_api import Config, Dir, DirListing, File, Rclone, Remote

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
bucket = {BUCKET_NAME}
"""

    out = Config(config_text)
    return out


class RcloneLsTests(unittest.TestCase):
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

    def test_list_remotes(self) -> None:
        rclone = Rclone(_generate_rclone_config())

        remotes: list[Remote] = rclone.listremotes()
        self.assertGreater(len(remotes), 0)
        for remote in remotes:
            self.assertIsInstance(remote, Remote)
            print(remote)
        print("done")

    def test_ls_root(self) -> None:
        """Test listing the root directory of the bucket.

        Verifies that we can:
        1. Connect to the bucket
        2. List its contents
        3. Get both directories and files as proper types
        """
        self.assertIsNotNone(BUCKET_NAME)
        rclone = Rclone(_generate_rclone_config())
        listing: DirListing = rclone.ls(f"dst:{BUCKET_NAME}", max_depth=-1)

        # Verify we got a valid listing
        self.assertIsInstance(listing, DirListing)
        self.assertGreater(len(listing.dirs), 0)
        self.assertGreater(len(listing.files), 0)

        # Verify dirs are properly typed
        for dir in listing.dirs:
            self.assertIsInstance(dir, Dir)
            print(dir)

        # Verify files are properly typed
        for file in listing.files:
            self.assertIsInstance(file, File)
            print(file)

        print("done")

    def test_ls_subdir(self) -> None:
        rclone = Rclone(_generate_rclone_config())
        path = f"dst:{BUCKET_NAME}/zachs_video"
        listing: DirListing = rclone.ls(path)
        print(listing)

    def test_ls_glob_png(self) -> None:
        rclone = Rclone(_generate_rclone_config())
        path = f"dst:{BUCKET_NAME}/zachs_video"
        listing: DirListing = rclone.ls(path, glob="*.png")
        self.assertGreater(len(listing.files), 0)
        for file in listing.files:
            self.assertIsInstance(file, File)
            # test that it ends with .png
            self.assertTrue(file.name.endswith(".png"))
        # there should be no directories with this glob
        self.assertEqual(len(listing.dirs), 0)


if __name__ == "__main__":
    unittest.main()
