"""
Unit test file.
"""

import os
import unittest

from dotenv import load_dotenv

from rclone_api import Config, Rclone
from rclone_api.diff import DiffItem, DiffOption, DiffType

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


class RcloneDiffTests(unittest.TestCase):
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

    def test_diff(self) -> None:
        """Test copying a single file to remote storage."""
        rclone = Rclone(_generate_rclone_config())
        item: DiffItem
        all: list[DiffItem] = []
        for item in rclone.diff("dst:rclone-api-unit-test", "dst:rclone-api-unit-test"):
            self.assertEqual(
                item.type, DiffType.EQUAL
            )  # should be equal because same repo
            all.append(item)
        self.assertGreater(len(all), 10)
        msg = "\n".join([str(item) for item in all])
        print(msg)

    def test_min_max_size(self) -> None:
        rclone = Rclone(_generate_rclone_config())
        item: DiffItem
        all: list[DiffItem] = list(
            rclone.diff(
                "dst:rclone-api-unit-test", "dst:rclone-api-unit-test", min_size="70M"
            )
        )
        for item in all:
            if "internaly_ai_alignment.mp4" in item.path:
                break
        else:
            self.fail("internaly_ai_alignment.mp4 not found")
        all.clear()
        all = list(
            rclone.diff(
                "dst:rclone-api-unit-test", "dst:rclone-api-unit-test", max_size="70M"
            )
        )
        for item in all:
            if "internaly_ai_alignment.mp4" in item.path:
                self.fail("internaly_ai_alignment.mp4 not filtered")

    def test_diff_missing_on_dst(self) -> None:
        rclone = Rclone(_generate_rclone_config())
        item: DiffItem
        all: list[DiffItem] = []
        for item in rclone.diff(
            "dst:rclone-api-unit-test",
            "dst:rclone-api-unit-test/does-not-exist",
            diff_option=DiffOption.MISSING_ON_DST,
        ):
            self.assertEqual(
                item.type, DiffType.MISSING_ON_DST
            )  # should be equal because same repo
            all.append(item)
        self.assertGreaterEqual(len(all), 47)
        msg = "\n".join([str(item) for item in all])
        print(msg)

    def test_diff_missing_on_src(self) -> None:
        rclone = Rclone(_generate_rclone_config())
        item: DiffItem
        all: list[DiffItem] = []
        for item in rclone.diff(
            "dst:rclone-api-unit-test/does-not-exist",
            "dst:rclone-api-unit-test",
            diff_option=DiffOption.MISSING_ON_SRC,
        ):
            self.assertEqual(item.type, DiffType.MISSING_ON_SRC)
            all.append(item)
        self.assertGreaterEqual(len(all), 47)
        msg = "\n".join([str(item) for item in all])
        print(msg)


if __name__ == "__main__":
    unittest.main()
