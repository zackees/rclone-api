"""
Unit test file for testing rclone mount functionality.
"""

import tempfile
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import httpx

from rclone_api.process import Process

_TIMEOUT = 10 * 60  # 10 minutes


@dataclass
class Range:
    start: int
    end: int

    def to_header(self) -> dict[str, str]:
        val = f"bytes={self.start}-{self.end-1}"
        return {
            "Range": val,
        }


class HttpServer:
    """HTTP server configuration."""

    def __init__(
        self, url: str, subpath: str, process: Process, max_workers: int = 1
    ) -> None:
        self.url = url
        self.subpath = subpath
        self.process: Process | None = process

    def _get_file_url(self, path: str | Path) -> str:
        # if self.subpath == "":
        path = Path(path).as_posix()
        return f"{self.url}/{path}"
        # return f"{self.url}/{self.subpath}/{path}"

    def get(self, path: str) -> bytes | Exception:
        """Get bytes from the server."""
        with tempfile.TemporaryFile() as file:
            self.download(path, Path(file.name), None)
            file.seek(0)
            return file.read()

    def size(self, path: str) -> int | Exception:
        """Get size of the file from the server."""
        try:
            assert self.process is not None
            # response = httpx.head(f"{self.url}/{path}")
            url = self._get_file_url(path)
            response = httpx.head(url)
            response.raise_for_status()
            size = int(response.headers["Content-Length"])
            return size
        except Exception as e:
            warnings.warn(f"Failed to get size of {self.url}/{path}: {e}")
            return e

    def download(
        self, path: str, dst: Path, range: Range | None = None
    ) -> Path | Exception:
        """Get bytes from the server."""
        if not dst.parent.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
        headers: dict[str, str] = {}
        if range:
            headers.update(range.to_header())
        url = self._get_file_url(path)
        try:
            with httpx.stream(
                "GET", url, headers=headers, timeout=_TIMEOUT
            ) as response:
                response.raise_for_status()
                with open(dst, "wb") as file:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
                        else:
                            assert response.is_closed
                # print(f"Downloaded bytes {start}-{end} to {dst}")
                if range:
                    print(f"Downloaded bytes {range.start}-{range.end} to {dst}")
                else:
                    size = dst.stat().st_size
                    print(f"Downloaded {size} bytes to {dst}")
                return dst
        except Exception as e:
            warnings.warn(f"Failed to download {url} to {dst}: {e}")
            return e

    def download_multi_threaded(
        self,
        src_path: str,
        dst_path: Path,
        chunk_size: int = 8 * 1024 * 1024,
        n_threads: int = 16,
    ) -> Path | Exception:
        """Copy file from src to dst."""

        finished: list[Path] = []
        errors: list[Exception] = []

        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            file_size = self.size(src_path)
            if isinstance(file_size, Exception):
                return file_size
            try:
                futures: list[Future[Path | Exception]] = []
                for start in range(0, file_size, chunk_size):
                    end = min(start + chunk_size, file_size)
                    r = Range(start=start, end=end)

                    def task(r: Range = r) -> Path | Exception:
                        dst = dst_path.with_suffix(f".{r.start}")
                        # print(f"Downloading {r.start}-{r.end} to {dst}")
                        out = self.download(src_path, dst, r)
                        if isinstance(out, Exception):
                            warnings.warn(f"Failed to download chunked: {out}")
                        return out

                    fut = executor.submit(task, r)
                    futures.append(fut)

                for fut in futures:
                    result = fut.result()
                    if isinstance(result, Exception):
                        errors.append(result)
                    else:
                        finished.append(result)
                if errors:
                    for finished_file in finished:
                        try:
                            finished_file.unlink()
                        except Exception as e:
                            warnings.warn(f"Failed to delete file {finished_file}: {e}")
                    return Exception(f"Failed to download chunked: {errors}")

                if not dst_path.parent.exists():
                    dst_path.parent.mkdir(parents=True, exist_ok=True)

                count = 0
                with open(dst_path, "wb") as file:
                    for f in finished:
                        print(f"Appending {f} to {dst_path}")
                        with open(f, "rb") as part:
                            # chunk = part.read(8192 * 4)
                            while chunk := part.read(8192 * 4):
                                if not chunk:
                                    break
                                count += len(chunk)
                                file.write(chunk)
                        print(f"Removing {f}")
                        f.unlink()
                # print(f"Downloaded {count} bytes to {dst_path}")
                return dst_path
            except Exception as e:
                warnings.warn(f"Failed to copy chunked: {e}")
                for f in finished:
                    try:
                        if f.exists():
                            f.unlink()
                    except Exception as ee:
                        warnings.warn(f"Failed to delete file {f}: {ee}")
                return e

    def close(self) -> None:
        """Close the server."""

        if self.process:
            if self.process.poll() is None:
                self.process.kill()
            self.process = None

    def __enter__(self) -> "HttpServer":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
        if self.process:
            self.process.terminate()
