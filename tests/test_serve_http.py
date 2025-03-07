"""
Unit test file for testing rclone mount functionality.
"""

import os
import subprocess
import unittest
from pathlib import Path

from dotenv import load_dotenv

from rclone_api import Config, Rclone
from rclone_api.http_server import HttpServer

load_dotenv()


def _generate_rclone_config() -> Config:
    # Load environment variables
    BUCKET_KEY_SECRET = os.getenv("BUCKET_KEY_SECRET")
    BUCKET_KEY_PUBLIC = os.getenv("BUCKET_KEY_PUBLIC")
    BUCKET_URL = "sfo3.digitaloceanspaces.com"

    config_text = f"""
[dst]
type = s3
provider = DigitalOcean
access_key_id = {BUCKET_KEY_PUBLIC}
secret_access_key = {BUCKET_KEY_SECRET}
endpoint = {BUCKET_URL}
"""
    return Config(config_text)


class RcloneServeHttpTester(unittest.TestCase):
    """Test rclone mount functionality."""

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

        self.bucket_name = os.getenv("BUCKET_NAME")
        self.mount_point = Path("test_tmp_serve_http")
        # Create mount point directory if it doesn't exist
        # self.mount_point.mkdir(exist_ok=True)
        # make parents
        parent = self.mount_point.parent
        if not parent.exists():
            parent.mkdir(parents=True)

        os.environ["RCLONE_API_VERBOSE"] = "1"
        self.rclone = Rclone(_generate_rclone_config())

    # @unittest.skip("Skip for now")
    def test_server_http(self) -> None:
        """Test mounting a remote bucket."""
        remote_path = f"dst:{self.bucket_name}"
        http_server: HttpServer | None = None
        try:
            with self.rclone.serve_http(remote_path, threads=16) as http_server:
                resource_url = "zachs_video/internaly_ai_alignment.mp4"
                expected_size = 73936110

                actual_size = http_server.size(resource_url)
                print(f"Actual size: {actual_size}")

                self.assertEqual(actual_size, expected_size)
                dst1 = self.mount_point / Path(
                    "zachs_video/internaly_ai_alignment.mp4.1"
                )
                dst2 = self.mount_point / Path(
                    "zachs_video/internaly_ai_alignment.mp4.2"
                )

                # out1 = http_server.copy_chunked(resource_url, dst1).result()
                out1 = http_server.download(resource_url, dst1)
                out2 = http_server.download_multi_threaded(resource_url, dst2)

                assert not isinstance(out1, Exception)
                assert not isinstance(out2, Exception)

                s1 = dst1.stat().st_size
                s2 = dst2.stat().st_size

                print(f"Size of {dst1}: {dst1.stat().st_size}")
                print(f"Size of {dst2}: {dst2.stat().st_size}")

                if s1 != s2:
                    # find the first index where there is a difference
                    with open(dst1, "rb") as f1, open(dst2, "rb") as f2:
                        bad_index = 0
                        while (chunk1 := f1.read(1)) and (chunk2 := f2.read(1)):
                            if chunk1 != chunk2:
                                break
                            bad_index += 1
                        print("bad index: ", bad_index)

                # out1 = f1.result()
                # out2 = f2.result()

                self.assertIsInstance(out1, Path)
                self.assertIsInstance(out2, Path)
                assert isinstance(out2, Path)

                # print(f"Bytes written: {out1.stat().st_size}")
                print(f"Bytes written: {out2.stat().st_size}")

                def hash_bytes(fp: Path) -> str:
                    import hashlib

                    sha256 = hashlib.sha256()
                    with open(fp, "rb") as f:
                        while chunk := f.read(4096):
                            sha256.update(chunk)
                    return sha256.hexdigest()

                hash1 = hash_bytes(dst1)
                hash2 = hash_bytes(dst2)

                print(dst1.absolute())
                print(dst2.absolute())

                self.assertEqual(hash1, hash2)
                print("Done")

        except subprocess.CalledProcessError as e:
            self.fail(f"Mount operation failed: {str(e)}")
        finally:
            # Cleanup will happen in tearDown
            pass

    @unittest.skip("Skip for now")
    def test_download_chunked(self) -> None:
        """Test mounting a remote bucket."""
        remote_path = f"dst:{self.bucket_name}"
        http_server: HttpServer | None = None

        try:
            # ls_result = self.rclone.ls(f"dst:{self.bucket_name}", max_depth=3)
            # print(f"Remotes: {ls_result}")
            http_server = self.rclone.serve_http(remote_path, threads=1)
            content: bytes | Exception = http_server.get("first.txt")
            print(f"Content: {str(content)}")
            self.assertIsInstance(content, bytes)

            content = http_server.get("first.txt")
            print(content)
            print("done")

            resource_url = "zachs_video/internaly_ai_alignment.mp4"

            dst1 = self.mount_point / Path("zachs_video/internaly_ai_alignment.mp4.1")
            #  dst2 = self.mount_point / Path("zachs_video/internaly_ai_alignment.mp4.2")

            # out1 = http_server.copy_chunked(
            #     resource_url, dst1
            # ).result()

            # out2 = http_server.copy(
            #     resource_url, dst2
            # ).result()

            out1 = http_server.download_multi_threaded(resource_url, dst1)
            # f2 = http_server.copy(resource_url, dst2)

            # out1 = f1.result()
            # out2 = f2.result()

            self.assertIsInstance(out1, Path)
            # self.assertIsInstance(out2, Path)

            # def hash_bytes(fp: Path) -> str:
            #     import hashlib
            #     sha256 = hashlib.sha256()
            #     with open(fp, "rb") as f:
            #         while (chunk := f.read(4096)):
            #             sha256.update(chunk)
            #     return sha256.hexdigest()

            # hash1 = hash_bytes(dst1)
            # hash2 = hash_bytes(dst2)

            # print(dst1.absolute())
            # print(dst2.absolute())

            # self.assertEqual(hash1, hash2)
            print("Done")

        except subprocess.CalledProcessError as e:
            self.fail(f"Mount operation failed: {str(e)}")
        finally:
            # Cleanup will happen in tearDown
            pass


if __name__ == "__main__":
    unittest.main()
