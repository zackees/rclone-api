"""
Unit test file for testing rclone mount functionality.
"""

import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import httpx

from rclone_api import Process


@dataclass
class HttpServer:
    """HTTP server configuration."""

    url: str
    process: Process | None = None
    max_workers: int = 1
    _thread_pool: ThreadPoolExecutor | None = None

    def __post_init__(self):
        self.max_workers = max(1, self.max_workers)
        self._thread_pool = ThreadPoolExecutor(self.max_workers)

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
        self, src_path: str, dst_path: Path, chunk_size: int, file_size: int
    ) -> Path | Exception:
        """Copy file from src to dst."""
        try:
            assert self.process is not None
            # response = httpx.get(f"{self.url}/{src_path}")
            # esponse.raise_for_status()
            # stream response to file

            def _download_parts(
                start: int, end: int, chunk_size: int, dst_path: Path = dst_path
            ) -> Exception | None:
                executor = self._thread_pool
                assert executor is not None
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
                                # executor.shutdown(wait=False, cancel_futures=True)
                                for future in futures:
                                    future.cancel()
                                for future in futures:
                                    future.result()
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
        if pool := self._thread_pool:
            pool.shutdown(wait=True, cancel_futures=True)
