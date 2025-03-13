import _thread
import atexit
import os
import shutil
import threading
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from rclone_api.http_server import HttpServer
from rclone_api.rclone_impl import RcloneImpl
from rclone_api.s3.multipart.info_json import InfoJson
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


def _gen_name(part_number: int, offset: SizeSuffix, end: SizeSuffix) -> str:
    return f"part.{part_number:05d}_{offset.as_int()}-{end.as_int()}"


def upload_task(self: RcloneImpl, upload_part: UploadPart) -> UploadPart:
    try:
        if upload_part.exception is not None:
            return upload_part
        # print(f"Uploading {upload_part.chunk} to {upload_part.dst_part}")
        msg = "\n#########################################\n"
        msg += f"# Uploading {upload_part.chunk} to {upload_part.dst_part}\n"
        msg += "#########################################\n"
        print(msg)
        self.copy_to(upload_part.chunk.as_posix(), upload_part.dst_part)
        return upload_part
    except Exception as e:
        upload_part.exception = e
        return upload_part
    finally:
        upload_part.dispose()


def read_task(
    http_server: HttpServer,
    src_name: str,
    tmpdir: Path,
    offset: SizeSuffix,
    length: SizeSuffix,
    part_dst: str,
) -> UploadPart:
    outchunk: Path = tmpdir / f"{offset.as_int()}-{(offset + length).as_int()}.chunk"
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


def collapse_runs(numbers: list[int]) -> list[str]:
    if not numbers:
        return []

    runs = []
    start = numbers[0]
    prev = numbers[0]

    for num in numbers[1:]:
        if num == prev + 1:
            # Continue current run
            prev = num
        else:
            # End current run
            if start == prev:
                runs.append(str(start))
            else:
                runs.append(f"{start}-{prev}")
            start = num
            prev = num

    # Append the final run
    if start == prev:
        runs.append(str(start))
    else:
        runs.append(f"{start}-{prev}")

    return runs


_MIN_PART_UPLOAD_SIZE = SizeSuffix("5MB")


def _check_part_size(parts: list[PartInfo]) -> Exception | None:
    if len(parts) == 0:
        return Exception("No parts to upload")
    part = parts[0]
    chunk = part.range.end - part.range.start
    if chunk < _MIN_PART_UPLOAD_SIZE:
        return Exception(
            f"Part size {chunk} is too small to upload. Minimum size for server side merge is {_MIN_PART_UPLOAD_SIZE}"
        )
    return None


def upload_parts_resumable(
    self: RcloneImpl,
    src: str,  # src:/Bucket/path/myfile.large.zst
    dst_dir: str,  # dst:/Bucket/path/myfile.large.zst-parts/
    part_infos: list[PartInfo] | None = None,
    threads: int = 1,
    verbose: bool | None = None,
) -> Exception | None:
    """Copy parts of a file from source to destination."""
    from rclone_api.util import random_str

    def verbose_print(*args, **kwargs):
        if verbose:
            print(*args, **kwargs)

    if dst_dir.endswith("/"):
        dst_dir = dst_dir[:-1]
    src_size = self.size_file(src)

    if isinstance(src_size, Exception):
        return src_size

    part_info: PartInfo
    src_dir = os.path.dirname(src)
    src_name = os.path.basename(src)
    http_server: HttpServer

    full_part_infos: list[PartInfo] | Exception = PartInfo.split_parts(
        src_size, SizeSuffix("96MB")
    )
    if isinstance(full_part_infos, Exception):
        return full_part_infos
    assert isinstance(full_part_infos, list)

    if part_infos is None:
        src_size = self.size_file(src)
        if isinstance(src_size, Exception):
            return src_size
        part_infos = full_part_infos.copy()

    err = _check_part_size(part_infos)
    if err:
        return err

    all_part_numbers: list[int] = [p.part_number for p in part_infos]
    src_info_json = f"{dst_dir}/info.json"
    info_json = InfoJson(self, src, src_info_json)

    if not info_json.load():
        verbose_print(f"New: {src_info_json}")
        # info_json.save()

    all_numbers_already_done: set[int] = set(
        info_json.fetch_all_finished_part_numbers()
    )

    first_part_number = part_infos[0].part_number
    last_part_number = part_infos[-1].part_number

    verbose_print(
        f"all_numbers_already_done: {collapse_runs(sorted(list(all_numbers_already_done)))}"
    )

    filtered_part_infos: list[PartInfo] = []
    for part_info in part_infos:
        if part_info.part_number not in all_numbers_already_done:
            filtered_part_infos.append(part_info)
    part_infos = filtered_part_infos
    remaining_part_numbers: list[int] = [p.part_number for p in part_infos]
    verbose_print(f"remaining_part_numbers: {collapse_runs(remaining_part_numbers)}")
    num_remaining_to_upload = len(part_infos)
    verbose_print(
        f"num_remaining_to_upload: {num_remaining_to_upload} / {len(full_part_infos)}"
    )

    if num_remaining_to_upload == 0:
        return None
    chunk_size = SizeSuffix(part_infos[0].range.end - part_infos[0].range.start)

    info_json.chunksize = chunk_size

    info_json.first_part = first_part_number
    info_json.last_part = last_part_number
    info_json.save()

    # We are now validated
    info_json.load()
    info_json.print()

    print(info_json)

    finished_tasks: list[UploadPart] = []
    tmp_dir = str(Path("chunks") / random_str(12))

    atexit.register(lambda: shutil.rmtree(tmp_dir, ignore_errors=True))

    with self.serve_http(src_dir) as http_server:
        tmpdir: Path = Path(tmp_dir)
        write_semaphore = threading.Semaphore(threads)
        with ThreadPoolExecutor(max_workers=threads) as upload_executor:
            with ThreadPoolExecutor(max_workers=threads) as read_executor:
                for part_info in part_infos:
                    part_number: int = part_info.part_number
                    range: Range = part_info.range
                    offset: SizeSuffix = SizeSuffix(range.start)
                    length: SizeSuffix = SizeSuffix(range.end - range.start)
                    end = offset + length
                    suffix = _gen_name(part_number, offset, end)
                    part_dst = f"{dst_dir}/{suffix}"

                    def _read_task(
                        src_name=src_name,
                        http_server=http_server,
                        tmpdir=tmpdir,
                        offset=offset,
                        length=length,
                        part_dst=part_dst,
                    ) -> UploadPart:
                        return read_task(
                            src_name=src_name,
                            http_server=http_server,
                            tmpdir=tmpdir,
                            offset=offset,
                            length=length,
                            part_dst=part_dst,
                        )

                    read_fut: Future[UploadPart] = read_executor.submit(_read_task)

                    # Releases the semaphore when the write task is done
                    def queue_upload_task(
                        read_fut=read_fut,
                    ) -> None:
                        upload_part = read_fut.result()
                        upload_fut: Future[UploadPart] = upload_executor.submit(
                            upload_task, self, upload_part
                        )
                        # SEMAPHORE RELEASE!!!
                        upload_fut.add_done_callback(
                            lambda _: write_semaphore.release()
                        )
                        upload_fut.add_done_callback(
                            lambda fut: finished_tasks.append(fut.result())
                        )

                    read_fut.add_done_callback(queue_upload_task)
                    # SEMAPHORE ACQUIRE!!!
                    # If we are back filled on the writers, then we stall.
                    write_semaphore.acquire()

    exceptions: list[Exception] = [
        t.exception for t in finished_tasks if t.exception is not None
    ]

    shutil.rmtree(tmp_dir, ignore_errors=True)

    if len(exceptions) > 0:
        return Exception(f"Failed to copy parts: {exceptions}", exceptions)

    finished_parts: list[int] = info_json.fetch_all_finished_part_numbers()
    print(f"finished_names: {finished_parts}")

    diff_set = set(all_part_numbers).symmetric_difference(set(finished_parts))
    all_part_numbers_done = len(diff_set) == 0
    print(f"all_part_numbers_done: {all_part_numbers_done}")
    return None
