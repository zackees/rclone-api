"""
Unit test file.
"""

import os
import tempfile
import unittest
from pathlib import Path

from dotenv import load_dotenv

from rclone_api import Config, Rclone, SizeSuffix

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")  # Default if not in .env


def _generate_rclone_config() -> Config:

    # BUCKET_NAME = os.getenv("BUCKET_NAME", "TorrentBooks")  # Default if not in .env

    # Load additional environment variables
    BUCKET_KEY_SECRET = os.getenv("BUCKET_KEY_SECRET")
    BUCKET_KEY_PUBLIC = os.getenv("BUCKET_KEY_PUBLIC")
    SRC_SFTP_HOST = os.getenv("SRC_SFTP_HOST")
    SRC_SFTP_USER = os.getenv("SRC_SFTP_USER")
    SRC_SFTP_PORT = os.getenv("SRC_SFTP_PORT")
    SRC_SFTP_PASS = os.getenv("SRC_SFTP_PASS")
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

[src]
type = sftp
host = {SRC_SFTP_HOST}
user = {SRC_SFTP_USER}
port = {SRC_SFTP_PORT}
pass = {SRC_SFTP_PASS}

"""
    # _CONFIG_PATH.write_text(config_text, encoding="utf-8")
    # print(f"Config file written to: {_CONFIG_PATH}")
    return Config(config_text)


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
        bytes_or_err: bytes | Exception = rclone.copy_bytes_multimount(
            src="dst:rclone-api-unit-test/zachs_video/breaking_ai_mind.mp4",
            offset=0,
            length=1024 * 1024,
            chunk_size=SizeSuffix(1024 * 1024),
            max_threads=1,
        )
        if isinstance(bytes_or_err, Exception):
            print(bytes_or_err)
            self.fail(f"Error: {bytes_or_err}")
        assert isinstance(bytes_or_err, bytes)
        self.assertEqual(
            len(bytes_or_err), 1024 * 1024
        )  # , f"Length: {len(bytes_or_err)}"

    def test_copy_bytes_to_temp_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir) / "tmp.mp4"
            log = Path(tmpdir) / "log.txt"
            rclone = Rclone(_generate_rclone_config())
            bytes_or_err: bytes | Exception = rclone.copy_bytes_multimount(
                src="dst:rclone-api-unit-test/zachs_video/breaking_ai_mind.mp4",
                offset=0,
                length=1024 * 1024,
                chunk_size=SizeSuffix(1024 * 1024),
                outfile=tmp,
                mount_log=log,
            )
            if isinstance(bytes_or_err, Exception):
                print(bytes_or_err)
                self.fail(f"Error: {bytes_or_err}")
            assert isinstance(bytes_or_err, bytes)
            self.assertEqual(len(bytes_or_err), 0)
            self.assertTrue(tmp.exists())
            tmp_size = tmp.stat().st_size
            self.assertEqual(tmp_size, 1024 * 1024)
            print(f"Log file: {log}:")
            print(log.read_text())
            log_text = log.read_text(encoding="utf-8")
            self.assertTrue("Getattr" in log_text)
            print("done")

        print("done")


if __name__ == "__main__":
    unittest.main()
