import logging
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from threading import Lock, Semaphore
from typing import Any

from rclone_api.mount import Mount
from rclone_api.types import FilePart

# Create a logger for this module
logger = logging.getLogger(__name__)


def _read_from_mount_task(
    offset: int, size: int, path: Path, verbose: bool
) -> bytes | Exception:
    if verbose:
        logger.debug(f"Fetching chunk: offset={offset}, size={size}, path={path}")
    try:
        with path.open("rb") as f:
            f.seek(offset)
            payload = f.read(size)
            assert len(payload) == size, f"Invalid read size: {len(payload)}"
            return payload

    except KeyboardInterrupt as e:
        import _thread

        logger.error("KeyboardInterrupt received during chunk read")
        _thread.interrupt_main()
        return Exception(e)
    except Exception as e:
        stack_trace = traceback.format_exc()
        logger.error(
            f"Error fetching file chunk at offset {offset} + {size}: {e}\n{stack_trace}"
        )
        return e


class MultiMountFileChunker:
    def __init__(
        self,
        filename: str,
        filesize: int,
        mounts: list[Mount],
        executor: ThreadPoolExecutor,
        verbose: bool | None,
    ) -> None:
        from rclone_api.util import get_verbose

        self.filename = filename
        self.filesize = filesize
        self.executor = executor
        self.mounts_processing: list[Mount] = []
        self.mounts_availabe: list[Mount] = mounts
        self.semaphore = Semaphore(len(mounts))
        self.lock = Lock()
        self.verbose = get_verbose(verbose)
        logger.info(
            f"Initialized MultiMountFileChunker for {filename} ({filesize} bytes)"
        )

    def shutdown(self) -> None:
        logger.info("Shutting down MultiMountFileChunker")
        self.executor.shutdown(wait=True, cancel_futures=True)
        with ThreadPoolExecutor() as executor:
            for mount in self.mounts_processing:
                executor.submit(lambda: mount.close())
        logger.debug("MultiMountFileChunker shutdown complete")

    def _acquire_mount(self) -> Mount:
        logger.debug("Acquiring mount")
        self.semaphore.acquire()
        with self.lock:
            mount = self.mounts_availabe.pop()
            self.mounts_processing.append(mount)
        logger.debug(f"Mount acquired: {mount}")
        return mount

    def _release_mount(self, mount: Mount) -> None:
        logger.debug(f"Releasing mount: {mount}")
        with self.lock:
            self.mounts_processing.remove(mount)
            self.mounts_availabe.append(mount)
            self.semaphore.release()
        logger.debug("Mount released")

    def fetch(self, offset: int, size: int, extra: Any) -> Future[FilePart]:
        if self.verbose:
            logger.debug(f"Fetching data range: offset={offset}, size={size}")

        assert size > 0, f"Invalid size: {size}"
        assert offset >= 0, f"Invalid offset: {offset}"
        assert (
            offset + size <= self.filesize
        ), f"Invalid offset + size: {offset} + {size} ({offset+size}) <= {self.filesize}"

        try:
            mount = self._acquire_mount()
            path = mount.mount_path / self.filename

            def task_fetch_file_range(
                size=size, path=path, mount=mount, verbose=self.verbose
            ) -> FilePart:
                bytes_or_err = _read_from_mount_task(
                    offset=offset, size=size, path=path, verbose=verbose
                )
                self._release_mount(mount)

                if isinstance(bytes_or_err, Exception):
                    logger.warning(f"Fetch task returned exception: {bytes_or_err}")
                    return FilePart(payload=bytes_or_err, extra=extra)
                logger.debug(f"Successfully fetched {size} bytes from offset {offset}")
                out = FilePart(payload=bytes_or_err, extra=extra)
                return out

            fut = self.executor.submit(task_fetch_file_range)
            return fut
        except Exception as e:
            logger.error(f"Error setting up file chunk fetch: {e}", exc_info=True)
            fp = FilePart(payload=e, extra=extra)
            return self.executor.submit(lambda: fp)
