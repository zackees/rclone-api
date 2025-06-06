"""
Unit test file for testing rclone mount functionality.
"""

import logging
import tempfile
import time
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Semaphore
from typing import Any

import httpx
from bs4 import BeautifulSoup

from rclone_api.file_part import FilePart
from rclone_api.process import Process
from rclone_api.types import Range, SizeSuffix, get_chunk_tmpdir

_TIMEOUT = 10 * 60  # 10 minutes
_PUT_WARNED = False

logger = logging.getLogger(__name__)

_range = range


@dataclass
class FileList:
    dirs: list[str]
    files: list[str]


def _parse_files_and_dirs(html: str) -> FileList:
    soup = BeautifulSoup(html, "html.parser")
    files = []
    dirs = []
    # Find each table row with class "file"
    for tr in soup.find_all("tr", class_="file"):
        name_span = tr.find("span", class_="name")  # type: ignore
        if not name_span:
            continue
        a_tag = name_span.find("a")  # type: ignore
        if not a_tag:
            continue
        # Get the text from the <a> tag
        file_name = a_tag.get_text(strip=True)  # type: ignore
        # Skip directories (they end with a slash)
        # if not file_name.endswith("/"):
        #    files.append(file_name)
        # files.append(file_name)
        if file_name.endswith("/"):
            dirs.append(file_name)
        else:
            files.append(file_name)
    return FileList(dirs=dirs, files=files)


class HttpServer:
    """HTTP server configuration."""

    def __init__(self, url: str, subpath: str, process: Process) -> None:
        self.url = url
        self.subpath = subpath
        self.process: Process | None = process

    def _get_file_url(self, path: str | Path) -> str:
        # if self.subpath == "":
        path = Path(path).as_posix()
        return f"{self.url}/{path}"
        # return f"{self.url}/{self.subpath}/{path}"

    def get_fetcher(self, path: str, n_threads: int = 16) -> "HttpFetcher":
        return HttpFetcher(self, path, n_threads=n_threads)

    def get(self, path: str, range: Range | None = None) -> bytes | Exception:
        """Get bytes from the server."""
        with tempfile.TemporaryFile() as file:
            self.download(path, Path(file.name), range)
            file.seek(0)
            return file.read()

    def exists(self, path: str) -> bool:
        """Check if the file exists on the server."""
        try:
            assert self.process is not None
            url = self._get_file_url(path)
            response = httpx.head(url)
            return response.status_code == 200
        except Exception as e:
            warnings.warn(f"Failed to check if {self.url}/{path} exists: {e}")
            return False

    def size(self, path: str) -> int | Exception:
        """Get size of the file from the server."""
        try:
            assert self.process is not None
            url = self._get_file_url(path)
            response = httpx.head(url)
            response.raise_for_status()
            size = int(response.headers["Content-Length"])
            return size
        except Exception as e:
            warnings.warn(f"Failed to get size of {self.url}/{path}: {e}")
            return e

    def put(self, path: str, data: bytes) -> Exception | None:
        """Put bytes to the server."""
        global _PUT_WARNED
        if not _PUT_WARNED:
            _PUT_WARNED = True
            warnings.warn("PUT method not implemented on the rclone binary as of 1.69")
        try:
            assert self.process is not None
            url = self._get_file_url(path)
            headers = {"Content-Type": "application/octet-stream"}
            response = httpx.post(url, content=data, timeout=_TIMEOUT, headers=headers)
            logger.info(f"Allowed methods: {response.headers.get('Allow')}")
            response.raise_for_status()
            return None
        except Exception as e:
            warnings.warn(f"Failed to put {path} to {self.url}: {e}")
            return e

    def delete(self, path: str) -> Exception | None:
        """Remove file from the server."""
        try:
            assert self.process is not None
            url = self._get_file_url(path)
            response = httpx.delete(url)
            response.raise_for_status()
            return None
        except Exception as e:
            warnings.warn(f"Failed to remove {path} from {self.url}: {e}")
            return e

        # curl "http://localhost:5572/?list"

    def list(self, path: str) -> tuple[list[str], list[str]] | Exception:
        """List files on the server."""

        try:
            assert self.process is not None
            url = self.url
            if path:
                url += f"/{path}"
            url += "/?list"
            response = httpx.get(url, timeout=_TIMEOUT)
            response.raise_for_status()
            files_and_dirs = _parse_files_and_dirs(response.content.decode())
            return files_and_dirs.files, files_and_dirs.dirs
        except Exception as e:
            warnings.warn(f"Failed to list files on {self.url}: {e}")
            return e

    def download(
        self, path: str, dst: Path, range: Range | None = None
    ) -> Path | Exception:
        """Get bytes from the server."""

        def task() -> Path | Exception:

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
                    if range:
                        length = range.end - range.start
                        logger.info(
                            f"Downloaded bytes starting at {range.start} with size {length} to {dst}"
                        )
                    else:
                        size = dst.stat().st_size
                        logger.info(f"Downloaded {size} bytes to {dst}")
                    return dst
            except Exception as e:
                warnings.warn(f"Failed to download {url} to {dst}: {e}")
                return e

        retries = 3
        for i in _range(retries):
            out = task()
            if not isinstance(out, Exception):
                return out
            warnings.warn(f"Failed to download {path} to {dst}: {out}, retrying ({i})")
            time.sleep(10)
        else:
            return Exception(f"Failed to download {path} to {dst}")

    def download_multi_threaded(
        self,
        src_path: str,
        dst_path: Path,
        chunk_size: int = 32 * 1024 * 1024,
        n_threads: int = 16,
        range: Range | None = None,
    ) -> Path | Exception:
        """Copy file from src to dst."""

        finished: list[Path] = []
        errors: list[Exception] = []

        if range is None:
            sz = self.size(src_path)
            if isinstance(sz, Exception):
                return sz
            range = Range(0, sz)

        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            try:
                futures: list[Future[Path | Exception]] = []
                start: int
                for start in _range(
                    range.start.as_int(), range.end.as_int(), chunk_size
                ):
                    end = min(
                        SizeSuffix(start + chunk_size).as_int(), range.end.as_int()
                    )
                    r = Range(start=start, end=end)

                    def task(r: Range = r) -> Path | Exception:
                        dst = dst_path.with_suffix(f".{r.start}")
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
                        logger.info(f"Appending {f} to {dst_path}")
                        with open(f, "rb") as part:
                            # chunk = part.read(8192 * 4)
                            while chunk := part.read(8192 * 4):
                                if not chunk:
                                    break
                                count += len(chunk)
                                file.write(chunk)
                        logger.info(f"Removing {f}")
                        f.unlink()
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

    def __enter__(self) -> "HttpServer":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.shutdown()

    def shutdown(self) -> None:
        """Shutdown the server."""
        if self.process:
            self.process.dispose()
            self.process = None


class HttpFetcher:
    def __init__(self, server: "HttpServer", path: str, n_threads: int) -> None:
        self.server = server
        self.path = path
        self.executor = ThreadPoolExecutor(max_workers=n_threads)
        # Semaphore throttles the number of concurrent fetches
        # TODO this is kind of a hack.
        self.semaphore = Semaphore(n_threads)

    def bytes_fetcher(
        self, offset: int | SizeSuffix, size: int | SizeSuffix, extra: Any
    ) -> Future[FilePart]:
        if isinstance(offset, SizeSuffix):
            offset = offset.as_int()
        if isinstance(size, SizeSuffix):
            size = size.as_int()

        def task() -> FilePart:
            from rclone_api.util import random_str

            try:
                range = Range(offset, offset + size)
                dst = get_chunk_tmpdir() / f"{random_str(12)}.chunk"
                out = self.server.download(self.path, dst, range)
                if isinstance(out, Exception):
                    raise out
                return FilePart(payload=dst, extra=extra)
            finally:
                self.semaphore.release()

        self.semaphore.acquire()
        fut = self.executor.submit(task)
        return fut

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True)
