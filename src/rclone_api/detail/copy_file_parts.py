import _thread
import os
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from rclone_api.http_server import HttpServer
from rclone_api.rclone_impl import RcloneImpl
from rclone_api.types import (
    PartInfo,
    Range,
    SizeSuffix,
)


@dataclass
class UploadPart:
    chunk: Path
    dst_part: str
    exception: Exception | None = None
    finished: bool = False

    def dispose(self):
        try:
            if self.chunk.exists():
                self.chunk.unlink()
            self.finished = True
        except Exception as e:
            warnings.warn(f"Failed to delete file {self.chunk}: {e}")

    def __del__(self):
        self.dispose()


def upload_task(self: RcloneImpl, upload_part: UploadPart) -> UploadPart:
    try:
        if upload_part.exception is not None:
            return upload_part
        self.copy_to(upload_part.chunk.as_posix(), upload_part.dst_part)
        return upload_part
    except Exception as e:
        upload_part.exception = e
        return upload_part
    finally:
        upload_part.dispose()


def copy_file_parts(
    self: RcloneImpl,
    src: str,  # src:/Bucket/path/myfile.large.zst
    dst_dir: str,  # dst:/Bucket/path/myfile.large.zst-parts/
    part_infos: list[PartInfo] | None = None,
    threads: int = 1,
) -> Exception | None:
    """Copy parts of a file from source to destination."""
    if dst_dir.endswith("/"):
        dst_dir = dst_dir[:-1]

    part_info: PartInfo
    src_dir = os.path.dirname(src)
    src_name = os.path.basename(src)
    http_server: HttpServer

    if part_infos is None:
        src_size = self.size_file(src)
        if isinstance(src_size, Exception):
            return src_size
        part_infos = PartInfo.split_parts(src_size, SizeSuffix("96MB"))

    def read_task(
        http_server: HttpServer,
        tmpdir: Path,
        offset: SizeSuffix,
        length: SizeSuffix,
        part_dst: str,
    ) -> UploadPart:
        outchunk: Path = (
            tmpdir / f"{offset.as_int()}-{(offset + length).as_int()}.chunk"
        )
        range = Range(offset.as_int(), (offset + length).as_int())

        try:
            err = http_server.download(
                path=src_name,
                range=range,
                dst=outchunk,
            )
            if isinstance(err, Exception):
                out = UploadPart(chunk=outchunk, dst_part="", exception=err)
                out.dispose()
                return out
            return UploadPart(chunk=outchunk, dst_part=part_dst)
        except KeyboardInterrupt as ke:
            _thread.interrupt_main()
            raise ke
        except SystemExit as se:
            _thread.interrupt_main()
            raise se
        except Exception as e:
            return UploadPart(chunk=outchunk, dst_part=part_dst, exception=e)

    finished_tasks: list[UploadPart] = []

    with self.serve_http(src_dir) as http_server:
        with TemporaryDirectory() as tmp_dir:
            tmpdir: Path = Path(tmp_dir)
            import threading

            semaphore = threading.Semaphore(threads)

            with ThreadPoolExecutor(max_workers=threads) as upload_executor:
                with ThreadPoolExecutor(max_workers=threads) as read_executor:
                    for part_info in part_infos:
                        part_number: int = part_info.part_number
                        range: Range = part_info.range
                        offset: SizeSuffix = SizeSuffix(range.start)
                        length: SizeSuffix = SizeSuffix(range.end - range.start)
                        end = offset + length
                        part_dst = f"{dst_dir}/part.{part_number:05d}.{offset.as_int()}-{end.as_int()}"

                        def task(
                            http_server=http_server,
                            tmpdir=tmpdir,
                            offset=offset,
                            length=length,
                            part_dst=part_dst,
                        ) -> UploadPart:
                            return read_task(
                                http_server=http_server,
                                tmpdir=tmpdir,
                                offset=offset,
                                length=length,
                                part_dst=part_dst,
                            )

                        read_fut: Future[UploadPart] = read_executor.submit(task)

                        def queue_upload_task(
                            read_fut=read_fut,
                        ) -> None:
                            upload_part = read_fut.result()
                            upload_fut: Future[UploadPart] = upload_executor.submit(
                                upload_task, self, upload_part
                            )
                            upload_fut.add_done_callback(lambda _: semaphore.release())
                            upload_fut.add_done_callback(
                                lambda fut: finished_tasks.append(fut.result())
                            )

                        read_fut.add_done_callback(queue_upload_task)
                        semaphore.acquire()  # If we are back filled, we will wait here

    exceptions: list[Exception] = [
        t.exception for t in finished_tasks if t.exception is not None
    ]
    if len(exceptions) > 0:
        return Exception(f"Failed to copy parts: {exceptions}", exceptions)
    return None
