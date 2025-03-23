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


class RcloneCopyTests(unittest.TestCase):
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

    def test_copyfile(self) -> None:
        """Test copying a single file to remote storage."""
        rclone = Rclone(_generate_rclone_config())
        path = f"dst:{BUCKET_NAME}/zachs_video"
        listing: DirListing = rclone.ls(path, glob="*.png")
        self.assertGreater(len(listing.files), 0)
        file = listing.files[0]

        # Copy the file to the same location with a different name
        new_name = file.name + "_copy"
        new_path = f"dst:{BUCKET_NAME}/zachs_video/{new_name}"
        rclone.copy_to(file, new_path)
        # now test that the new file exists
        listing = rclone.ls(f"dst:{BUCKET_NAME}/zachs_video/", glob=f"*{new_name}")
        self.assertEqual(len(listing.files), 1)
        self.assertEqual(listing.dirs, [])
        rclone.delete_files([new_path])
        print("done")

    def test_copyfiles(self) -> None:
        """Test copying multiple files to remote storage."""
        rclone = Rclone(_generate_rclone_config())
        path = f"dst:{BUCKET_NAME}/zachs_video"
        listing: DirListing = rclone.ls(path, glob="*.png")
        self.assertGreater(len(listing.files), 0)
        first_file = str(listing.files[0])
        dest_file = first_file + "_copy"

        # Copy the file to the same location with different names
        rclone.copy_to(first_file, dest_file)

        # now test that the new file exists
        exists = rclone.exists(dest_file)
        self.assertTrue(exists)

        rclone.delete_files(dest_file)
        print(f"Checking that {dest_file} was deleted")
        deleted = rclone.exists(dest_file)
        self.assertFalse(deleted)
        print("done")


if __name__ == "__main__":
    unittest.main()
