"""
Unit test file.
"""

import os
import tempfile
import unittest
from pathlib import Path

import psutil
from dotenv import load_dotenv

from rclone_api import Config, Rclone, SizeSuffix

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

    @unittest.skip("Tested")
    def test_copy_bytes_to_temp_file(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir) / "tmp.mp4"
            log = Path(tmpdir) / "log.txt"
            rclone = Rclone(_generate_rclone_config())
            bytes_or_err: bytes | Exception = rclone.copy_bytes(
                src="dst:rclone-api-unit-test/zachs_video/breaking_ai_mind.mp4",
                offset=0,
                length=1024 * 1024,
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

    @unittest.skip("Tested")
    def test_profile_copy_bytes(self) -> None:
        rclone = Rclone(_generate_rclone_config())
        sizes = [
            1024 * 1024 * 1,
            1024 * 1024 * 16,
            1024 * 1024 * 64,
        ]
        transfer_list = [1, 16]
        import time

        for size in sizes:
            for transfers in transfer_list:
                print("\n\n")
                print("#" * 80)
                print(
                    f"# Started test download of {SizeSuffix(size)} with {transfers} transfers"
                )
                print("#" * 80)
                net_io_start = psutil.net_io_counters()
                start = time.time()
                bytes_or_err: bytes | Exception = rclone.copy_bytes(
                    src="dst:rclone-api-unit-test/zachs_video/internaly_ai_alignment.mp4",
                    offset=0,
                    length=size,
                    direct_io=True,
                    transfers=transfers,
                )
                diff = time.time() - start
                net_io_end = psutil.net_io_counters()
                if isinstance(bytes_or_err, Exception):
                    print(bytes_or_err)
                    self.fail(f"Error: {bytes_or_err}")
                assert isinstance(bytes_or_err, bytes)
                self.assertEqual(len(bytes_or_err), size)

                # print io stats
                bytes_sent = net_io_end.bytes_sent - net_io_start.bytes_sent
                bytes_recv = net_io_end.bytes_recv - net_io_start.bytes_recv
                packets_sent = net_io_end.packets_sent - net_io_start.packets_sent
                efficiency = size / (bytes_recv)
                efficiency_100 = efficiency * 100
                efficiency_str = f"{efficiency_100:.2f}"

                bytes_send_suffix = SizeSuffix(bytes_sent)
                bytes_recv_suffix = SizeSuffix(bytes_recv)
                range_size = SizeSuffix(size)

                print(f"\nFinished downloading {range_size} with {transfers} transfers")
                print("Net IO stats:")
                print(f"Bytes sent: {bytes_send_suffix}")
                print(f"Bytes received: {bytes_recv_suffix}")
                print(f"Packets sent: {packets_sent}")
                print(f"Efficiency: {efficiency_str}%")
                print(f"Time: {diff:.1f} seconds")

        print("done")


if __name__ == "__main__":
    unittest.main()
