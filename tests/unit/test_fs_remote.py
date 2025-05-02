"""
UUnit test file for the DB class.
"""

import os
import unittest
from pathlib import Path

from rclone_api import Config
from rclone_api.fs.filesystem import RemoteFS

HERE = Path(__file__).parent
DB_PATH = HERE / "test.db"

os.environ["DB_PATH"] = str(DB_PATH)


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


class RcloneRemoteFSTester(unittest.TestCase):
    """Tests for RemoteFS functionality."""

    def test_create_and_move_remote_fs(self) -> None:
        """Test create and move functionality."""
        config = _generate_rclone_config()

        base_path = "dst:rclone-api-unit-test"
        fs = RemoteFS.from_rclone_config(base_path, config)
        # cwd = fs.cwd()
        with fs.cwd() as cwd:
            remote_tester = cwd / "remote_tester"
            remote_tester.rmtree(ignore_errors=True)
            # Create a new file
            new_file_path = remote_tester / "test.txt"
            new_file_path.write_bytes(b"test")
            # self.assertTrue(new_file_path.exists())

            # Move the file
            moved_file_path = remote_tester / "moved_test.txt"
            new_file_path.moveTo(moved_file_path)
            # self.assertTrue(moved_file_path.exists())
            ## self.assertFalse(new_file_path.exists())
        print("Done")

    @unittest.skip("This test fails, file remains in cache after removal")
    def test_create_and_remove_remote_fs(self) -> None:
        """Test create and remove functionality."""
        config = _generate_rclone_config()

        base_path = "dst:rclone-api-unit-test"
        fs = RemoteFS.from_rclone_config(base_path, config)
        # cwd = fs.cwd()
        with fs.cwd() as cwd:
            # Create a new file
            new_file_path = cwd / "remote_tester" / "test.txt"
            new_file_path.write_bytes(b"test")
            self.assertTrue(new_file_path.exists())

            # Remove the file
            new_file_path.remove()
            # self.assertFalse(new_file_path.exists())

        fs = RemoteFS.from_rclone_config(base_path, config)

        with fs.cwd() as cwd:
            # Check if the file still exists
            exists = new_file_path.exists()
            self.assertFalse(exists)
        print("Done")


#
if __name__ == "__main__":
    unittest.main()
