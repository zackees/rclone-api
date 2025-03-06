"""
Unit test file for testing rclone mount functionality.
"""

import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

import httpx

from rclone_api import Process


class HttpServer:
    """HTTP server configuration."""

    def __init__(self, url: str, process: Process, max_workers: int = 1) -> None:
        self.url = url
        self.max_workers = max(1, max_workers)
        self.process: Process | None = process
        self._thread_pool = ThreadPoolExecutor(self.max_workers)

    def get(self, path: str) -> Future[bytes | Exception]:
        """Get bytes from the server."""

        def task() -> bytes | Exception:
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

        fut = self._thread_pool.submit(task)
        return fut

    def get_range(self, path: str, start: int, end: int) -> Future[bytes | Exception]:
        """Get bytes from the server."""

        def task() -> bytes | Exception:
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

        fut = self._thread_pool.submit(task)
        return fut

    def copy(self, src_path: str, dst_path: Path) -> Future[Path | Exception]:
        """Copy file from src to dst."""

        def task() -> Path | Exception:
            bytes_or_err = self.get(src_path).result()
            if isinstance(bytes_or_err, Exception):
                return bytes_or_err
            try:
                with open(dst_path, "wb") as f:
                    f.write(bytes_or_err)
                return dst_path
            except Exception as e:
                warnings.warn(f"Failed to copy {src_path} to {dst_path}: {e}")
                return e

        fut = self._thread_pool.submit(task)
        return fut

    def _copy_into_existing(
        self, src_path: str, dst_path: Path, start: int, end: int
    ) -> Future[Exception | None]:
        """Copy file from src to dst."""

        def task() -> Exception | None:
            bytes_or_err = self.get_range(src_path, start, end).result()
            if isinstance(bytes_or_err, Exception):
                return bytes_or_err
            try:
                chunk: bytes = bytes_or_err
                with open(dst_path, "wb") as f:
                    f.seek(start)
                    f.write(chunk)
                return None
            except Exception as e:
                warnings.warn(f"Failed to copy {src_path} to {dst_path}: {e}")
                return e

        fut = self._thread_pool.submit(task)
        return fut

    def copy_chunked(
        self, src_path: str, dst_path: Path, chunk_size: int, file_size: int
    ) -> Future[Path | Exception]:
        """Copy file from src to dst."""

        def task() -> Path | Exception:
            # resize the file to the full size
            if dst_path.exists():
                dst_path.unlink()
            dst_path.touch()
            dst_path.write_bytes(b"\0" * file_size)
            try:
                assert self.process is not None
                futures: list[Future[Exception | None]] = []
                for start in range(0, file_size, chunk_size):
                    end = min(start + chunk_size, file_size)
                    futures.append(
                        self._copy_into_existing(src_path, dst_path, start, end)
                    )
                for future in futures:
                    err: Exception | None = future.result()
                    if err:
                        for future in futures:
                            future.cancel()
                        for future in futures:
                            future.result()
                        raise err
                return dst_path
            except Exception as e:
                try:
                    dst_path.unlink()
                except Exception as e2:
                    warnings.warn(f"Failed to delete {dst_path}: {e2}")
                warnings.warn(f"Failed to copy {src_path} to {dst_path}: {e}")
                return e

        fut = self._thread_pool.submit(task)
        return fut

    def close(self) -> None:
        """Close the server."""

        if self.process:
            if self.process.poll() is None:
                self.process.kill()
            self.process = None
        if pool := self._thread_pool:
            pool.shutdown(wait=True, cancel_futures=True)
