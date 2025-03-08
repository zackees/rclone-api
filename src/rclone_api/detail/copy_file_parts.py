import _thread
import hashlib
import json
import os
import threading
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from rclone_api.dir_listing import DirListing
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


def _gen_name(part_number: int, offset: SizeSuffix, end: SizeSuffix) -> str:
    return f"part.{part_number:05d}_{offset.as_int()}-{end.as_int()}"


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


def _fetch_all_names(
    self: RcloneImpl,
    src: str,
) -> list[str]:
    dl: DirListing = self.ls(src)
    files = dl.files
    filenames: list[str] = [f.name for f in files]
    filtered: list[str] = [f for f in filenames if f.startswith("part.")]
    return filtered


def _get_info_json(self: RcloneImpl, src: str, src_info: str) -> dict:
    from rclone_api.file import File

    src_stat: File | Exception = self.stat(src)
    if isinstance(src_stat, Exception):
        raise FileNotFoundError(f"Failed to stat {src}: {src_stat}")

    now: datetime = datetime.now()
    new_data = {
        "new": True,
        "created": now.isoformat(),
        "src": src,
        "src_modtime": src_stat.mod_time(),
        "size": src_stat.size,
        "chunksize": None,
        "chunksize_int": None,
        "first_part": None,
        "last_part": None,
        "hash": None,
    }

    text_or_err = self.read_text(src_info)
    err: Exception | None = text_or_err if isinstance(text_or_err, Exception) else None
    if isinstance(text_or_err, Exception):
        warnings.warn(f"Failed to read {src_info}: {text_or_err}")
        return new_data
    assert isinstance(text_or_err, str)
    text: str = text_or_err

    if err is not None:
        return new_data

    data: dict = {}
    try:
        data = json.loads(text)
        return data
    except Exception as e:
        warnings.warn(f"Failed to parse JSON: {e} at {src_info}")
        return new_data


def _save_info_json(self: RcloneImpl, src: str, data: dict) -> None:
    data = data.copy()
    data["new"] = False
    # hash

    h = hashlib.md5()
    tmp = [
        data.get("src"),
        data.get("src_modtime"),
        data.get("size"),
        data.get("chunksize_int"),
    ]
    data_vals: list[str] = [str(v) for v in tmp]
    str_data = "".join(data_vals)
    h.update(str_data.encode("utf-8"))
    data["hash"] = h.hexdigest()
    json_str = json.dumps(data, indent=0)
    self.write_text(dst=src, text=json_str)


class InfoJson:
    def __init__(self, rclone: RcloneImpl, src: str, src_info: str) -> None:
        self.rclone = rclone
        self.src = src
        self.src_info = src_info
        self.data: dict = {}

    def load(self) -> bool:
        self.data = _get_info_json(self.rclone, self.src, self.src_info)
        return not self.data.get("new", False)

    def save(self) -> None:
        _save_info_json(self.rclone, self.src_info, self.data)

    def print(self) -> None:
        self.rclone.print(self.src_info)

    def fetch_all_finished(self) -> list[str]:
        parent_path = os.path.dirname(self.src_info)
        out = _fetch_all_names(self.rclone, parent_path)
        return out

    def fetch_all_finished_part_numbers(self) -> list[int]:
        names = self.fetch_all_finished()
        part_numbers = [int(name.split("_")[0].split(".")[1]) for name in names]
        return part_numbers

    @property
    def new(self) -> bool:
        return self.data.get("new", False)

    @property
    def chunksize(self) -> SizeSuffix | None:
        chunksize: str | None = self.data.get("chunksize")
        if chunksize is None:
            return None
        return SizeSuffix(chunksize)

    @chunksize.setter
    def chunksize(self, value: SizeSuffix) -> None:
        self.data["chunksize"] = str(value)
        self.data["chunksize_int"] = value.as_int()

    @property
    def src_modtime(self) -> datetime:
        return datetime.fromisoformat(self.data["src_modtime"])

    @src_modtime.setter
    def src_modtime(self, value: datetime) -> None:
        self.data["src_modtime"] = value.isoformat()

    @property
    def first_part(self) -> int | None:
        return self.data.get("first_part")

    @first_part.setter
    def first_part(self, value: int) -> None:
        self.data["first_part"] = value

    @property
    def last_part(self) -> int | None:
        return self.data.get("last_part")

    @last_part.setter
    def last_part(self, value: int) -> None:
        self.data["last_part"] = value

    @property
    def hash(self) -> str | None:
        return self.data.get("hash")

    def to_json_str(self) -> str:
        return json.dumps(self.data)

    def __repr__(self):
        return f"InfoJson({self.src}, {self.src_info}, {self.data})"

    def __str__(self):
        return self.to_json_str()


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

    all_part_numbers: list[int] = [p.part_number for p in part_infos]
    src_info_json = f"{dst_dir}/info.json"
    info_json = InfoJson(self, src, src_info_json)

    if not info_json.load():
        print(f"New: {src_info_json}")
        # info_json.save()

    all_numbers_already_done: set[int] = set(
        info_json.fetch_all_finished_part_numbers()
    )
    print(f"all_numbers_already_done: {sorted(list(all_numbers_already_done))}")

    filtered_part_infos: list[PartInfo] = []
    for part_info in part_infos:
        if part_info.part_number not in all_numbers_already_done:
            filtered_part_infos.append(part_info)
    part_infos = filtered_part_infos

    remaining_part_numbers: list[int] = [p.part_number for p in part_infos]
    print(f"remaining_part_numbers: {remaining_part_numbers}")

    if len(part_infos) == 0:
        return Exception(f"No parts to copy for {src}")
    chunk_size = SizeSuffix(part_infos[0].range.end - part_infos[0].range.start)

    info_json.chunksize = chunk_size
    info_json.first_part = part_infos[0].part_number
    info_json.last_part = part_infos[-1].part_number
    info_json.save()

    # We are now validated
    info_json.load()
    info_json.print()

    print(info_json)

    finished_tasks: list[UploadPart] = []

    with self.serve_http(src_dir) as http_server:
        with TemporaryDirectory() as tmp_dir:
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
    if len(exceptions) > 0:
        return Exception(f"Failed to copy parts: {exceptions}", exceptions)

    finished_parts: list[int] = info_json.fetch_all_finished_part_numbers()
    print(f"finished_names: {finished_parts}")

    diff_set = set(all_part_numbers).symmetric_difference(set(finished_parts))
    all_part_numbers_done = len(diff_set) == 0
    print(f"all_part_numbers_done: {all_part_numbers_done}")
    return None
