"""
Unit test file for testing rclone mount functionality.
"""

import os
import subprocess
import unittest
import warnings
from dataclasses import dataclass
from pathlib import Path

import httpx
from dotenv import load_dotenv

from rclone_api import Config, Process, Rclone

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


@dataclass
class HttpServer:
    """HTTP server configuration."""

    url: str
    process: Process | None = None

    def get(self, path: str) -> bytes | Exception:
        """Get bytes from the server."""
        try:
            assert self.process is not None
            response = httpx.get(f"{self.url}/{path}")
            response.raise_for_status()
            content = response.content
            assert isinstance(content, bytes)
            return response.content
        except Exception as e:
            warnings.warn(f"Failed to get bytes from {self.url}/{path}: {e}")
            return e

    def get_range(self, path: str, start: int, end: int) -> bytes | Exception:
        """Get bytes from the server."""
        try:
            assert self.process is not None
            headers = {"Range": f"bytes={start}-{end}"}
            response = httpx.get(f"{self.url}/{path}", headers=headers)
            response.raise_for_status()
            content = response.content
            assert isinstance(content, bytes)
            return response.content
        except Exception as e:
            warnings.warn(f"Failed to get bytes from {self.url}/{path}: {e}")
            return e

    def copy(self, src_path: str, dst_path: Path) -> Path | Exception:
        """Copy file from src to dst."""
        try:
            assert self.process is not None
            # response = httpx.get(f"{self.url}/{src_path}")
            # esponse.raise_for_status()
            # stream response to file
            response = httpx.get(f"{self.url}/{src_path}")
            response.raise_for_status()
            with open(dst_path, "wb") as f:
                f.write(response.content)
            return dst_path
        except Exception as e:
            warnings.warn(f"Failed to copy {src_path} to {dst_path}: {e}")
            return e

    def copy_chunked(
        self,
        src_path: str,
        dst_path: Path,
        chunk_size: int,
        file_size: int,
        max_workers: int = 1,
    ) -> Path | Exception:
        """Copy file from src to dst."""
        try:
            assert self.process is not None
            # response = httpx.get(f"{self.url}/{src_path}")
            # esponse.raise_for_status()
            # stream response to file
            from concurrent.futures import Future, ThreadPoolExecutor

            def _download_parts(
                start: int, end: int, chunk_size: int
            ) -> Exception | None:
                with ThreadPoolExecutor(max_workers) as executor:
                    try:
                        with open(dst_path, "wb") as f:
                            futures: list[Future[Exception | None]] = []
                            for start in range(0, file_size, chunk_size):
                                end = min(start + chunk_size, file_size)

                                def task(
                                    start: int = start, end: int = end
                                ) -> Exception | None:
                                    bytes = self.get_range(src_path, start, end)
                                    if isinstance(bytes, Exception):
                                        return bytes
                                    f.seek(start)
                                    f.write(bytes)
                                    return None

                                futures.append(executor.submit(task))
                            for future in futures:
                                err: Exception | None = future.result()
                                if err:
                                    executor.shutdown(wait=False, cancel_futures=True)
                                    return err
                            return None
                    except Exception as e:
                        warnings.warn(
                            f"Unexpected failure to download parts from {src_path} to {dst_path}: {e}"
                        )
                        return e

            def download_parts(
                start: int, end: int, chunk_size: int, dst_path: Path
            ) -> Exception | None:
                if dst_path.exists():
                    dst_path.unlink()
                # make file the full size
                dst_path.touch()
                dst_path.write_bytes(b"\0" * file_size)
                err = _download_parts(start, end, chunk_size)
                if err is None:
                    return None
                try:
                    dst_path.unlink()
                except Exception as e:
                    warnings.warn(f"Failed to delete {dst_path}: {e}")
                return err

            err: Exception | None = download_parts(
                start=0, end=file_size, chunk_size=chunk_size, dst_path=dst_path
            )
            if err is not None:
                return err
            return dst_path
        except Exception as e:
            warnings.warn(f"Failed to copy {src_path} to {dst_path}: {e}")
            return e

    def close(self) -> None:
        """Close the server."""
        if self.process:
            if self.process.poll() is None:
                self.process.kill()
            self.process = None


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
        self.mount_point = Path("test_mount")
        # Create mount point directory if it doesn't exist
        # self.mount_point.mkdir(exist_ok=True)
        # make parents
        parent = self.mount_point.parent
        if not parent.exists():
            parent.mkdir(parents=True)

        os.environ["RCLONE_API_VERBOSE"] = "1"
        self.rclone = Rclone(_generate_rclone_config())

    def test_server_http(self) -> None:
        """Test mounting a remote bucket."""
        remote_path = f"dst:{self.bucket_name}"
        process: Process | None = None
        http_server: HttpServer | None = None

        try:
            # Start the mount process
            process = self.rclone.serve_http(remote_path)
            http_server = HttpServer(process=process, url="http://localhost:8080")

            # url = "http://localhost:8080"
            # response = httpx.get(url)
            # print(f"Response: {response}")
            # print("done")

            content: bytes | Exception = http_server.get("first.txt")
            print(f"Content: {str(content)}")
            self.assertIsInstance(content, bytes)

            content = http_server.get("first.txt")
            print(content)
            print("done")

        except subprocess.CalledProcessError as e:
            self.fail(f"Mount operation failed: {str(e)}")
        finally:
            # Cleanup will happen in tearDown
            if process:
                if process.poll() is None:
                    process.kill()
                stdout = process.stdout
                if stdout:
                    # stdout is a buffered reader
                    for line in stdout:
                        print(line)
                stderr = process.stderr
                if stderr:
                    for line in stderr:
                        print(line)
                process.kill()


if __name__ == "__main__":
    unittest.main()
