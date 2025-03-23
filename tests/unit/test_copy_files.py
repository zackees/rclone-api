"""
Unit test file.
"""

import os
import unittest

from dotenv import load_dotenv

from rclone_api import (
    CompletedProcess,
    Config,
    DirListing,
    File,
    Rclone,
    rclone_verbose,
)

load_dotenv()
rclone_verbose(True)

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


class RcloneCopyFilesTest(unittest.TestCase):
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

    def test_copylist(self) -> None:
        """Test copying a list of files to remote storage."""
        rclone = Rclone(_generate_rclone_config())
        dst_prefix = f"dst:{BUCKET_NAME}/test_out"
        src_prefix = f"dst:{BUCKET_NAME}/zachs_video"
        listing: DirListing = rclone.ls(src_prefix, glob="*.png")
        self.assertGreater(len(listing.files), 0)
        first_file: File = listing.files[0]
        first_file_str = first_file.to_string(include_remote=False)
        print(f"first_file: {first_file_str}")
        include_files: list[str] = [first_file.name]
        completed_procs: list[CompletedProcess] = rclone.copy_files(
            src=src_prefix, dst=dst_prefix, files=include_files, max_partition_workers=2
        )
        self.assertGreater(len(completed_procs), 0)
        for proc in completed_procs:
            print(proc.stdout)
            print(proc.stderr)
            self.assertTrue(proc.returncode == 0)
        self.assertTrue(rclone.exists(dst_prefix))
        rclone.purge(dst_prefix)
        print(f"Checking that {dst_prefix} was deleted")
        still_exists = rclone.exists(dst_prefix)
        self.assertFalse(still_exists)
        print("done")

    def test_copylist_one_worker(self) -> None:
        """Test copying a list of files to remote storage."""
        rclone = Rclone(_generate_rclone_config())
        dst_prefix = f"dst:{BUCKET_NAME}/test_out"
        src_prefix = f"dst:{BUCKET_NAME}/zachs_video"
        listing: DirListing = rclone.ls(src_prefix, glob="*.png")
        self.assertGreater(len(listing.files), 0)
        first_file: File = listing.files[0]
        first_file_str = first_file.to_string(include_remote=False)
        print(f"first_file: {first_file_str}")
        include_files: list[str] = [first_file.name]
        completed_procs: list[CompletedProcess] = rclone.copy_files(
            src=src_prefix, dst=dst_prefix, files=include_files, max_partition_workers=1
        )
        self.assertGreater(len(completed_procs), 0)
        for proc in completed_procs:
            print(proc.stdout)
            print(proc.stderr)
            self.assertTrue(proc.returncode == 0)
        self.assertTrue(rclone.exists(dst_prefix))
        rclone.purge(dst_prefix)
        print(f"Checking that {dst_prefix} was deleted")
        still_exists = rclone.exists(dst_prefix)
        self.assertFalse(still_exists)
        print("done")


if __name__ == "__main__":
    unittest.main()
