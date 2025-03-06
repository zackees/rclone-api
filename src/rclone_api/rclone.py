"""
Unit test file.
"""

import os
import random
import shutil
import subprocess
import time
import traceback
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from fnmatch import fnmatch
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator

from rclone_api import Dir
from rclone_api.completed_process import CompletedProcess
from rclone_api.config import Config, Parsed, Section
from rclone_api.convert import convert_to_filestr_list, convert_to_str
from rclone_api.deprecated import deprecated
from rclone_api.diff import DiffItem, DiffOption, diff_stream_from_running_process
from rclone_api.dir_listing import DirListing
from rclone_api.exec import RcloneExec
from rclone_api.file import File, FileItem
from rclone_api.group_files import group_files
from rclone_api.mount import Mount, clean_mount, prepare_mount
from rclone_api.mount_read_chunker import MultiMountFileChunker
from rclone_api.process import Process
from rclone_api.remote import Remote
from rclone_api.rpath import RPath
from rclone_api.s3.types import (
    MultiUploadResult,
    S3MutliPartUploadConfig,
    S3Provider,
    S3UploadTarget,
)
from rclone_api.types import (
    ListingOption,
    ModTimeStrategy,
    Order,
    SizeResult,
    SizeSuffix,
)
from rclone_api.util import (
    get_check,
    get_rclone_exe,
    get_verbose,
    to_path,
)
from rclone_api.walk import walk


def rclone_verbose(verbose: bool | None) -> bool:
    if verbose is not None:
        os.environ["RCLONE_API_VERBOSE"] = "1" if verbose else "0"
    return bool(int(os.getenv("RCLONE_API_VERBOSE", "0")))


def _to_rclone_conf(config: Config | Path) -> Config:
    if isinstance(config, Path):
        content = config.read_text(encoding="utf-8")
        return Config(content)
    else:
        return config


class FilesStream:

    def __init__(self, path: str, process: Process) -> None:
        self.path = path
        self.process = process

    def __enter__(self) -> "FilesStream":
        self.process.__enter__()
        return self

    def __exit__(self, *exc_info):
        self.process.__exit__(*exc_info)

    def files(self) -> Generator[FileItem, None, None]:
        line: bytes
        for line in self.process.stdout:
            linestr: str = line.decode("utf-8").strip()
            if linestr.startswith("["):
                continue
            if linestr.endswith(","):
                linestr = linestr[:-1]
            if linestr.endswith("]"):
                continue
            fileitem: FileItem | None = FileItem.from_json_str(self.path, linestr)
            if fileitem is None:
                continue
            yield fileitem

    def files_paged(
        self, page_size: int = 1000
    ) -> Generator[list[FileItem], None, None]:
        page: list[FileItem] = []
        for fileitem in self.files():
            page.append(fileitem)
            if len(page) >= page_size:
                yield page
                page = []
        if len(page) > 0:
            yield page

    def __iter__(self) -> Generator[FileItem, None, None]:
        return self.files()


class Rclone:
    def __init__(
        self, rclone_conf: Path | Config, rclone_exe: Path | None = None
    ) -> None:
        if isinstance(rclone_conf, Path):
            if not rclone_conf.exists():
                raise ValueError(f"Rclone config file not found: {rclone_conf}")
        self._exec = RcloneExec(rclone_conf, get_rclone_exe(rclone_exe))
        self.config: Config = _to_rclone_conf(rclone_conf)

    def _run(
        self, cmd: list[str], check: bool = False, capture: bool | Path | None = None
    ) -> subprocess.CompletedProcess:
        return self._exec.execute(cmd, check=check, capture=capture)

    def _launch_process(
        self, cmd: list[str], capture: bool | None = None, log: Path | None = None
    ) -> Process:
        return self._exec.launch_process(cmd, capture=capture, log=log)

    def _get_tmp_mount_dir(self) -> Path:
        return Path("tmp_mnts")

    def _get_cache_dir(self) -> Path:
        return Path("cache")

    def webgui(self, other_args: list[str] | None = None) -> Process:
        """Launch the Rclone web GUI."""
        cmd = ["rcd", "--rc-web-gui"]
        if other_args:
            cmd += other_args
        return self._launch_process(cmd, capture=False)

    def launch_server(
        self,
        addr: str,
        user: str | None = None,
        password: str | None = None,
        other_args: list[str] | None = None,
    ) -> Process:
        """Launch the Rclone server so it can receive commands"""
        cmd = ["rcd"]
        if addr is not None:
            cmd += ["--rc-addr", addr]
        if user is not None:
            cmd += ["--rc-user", user]
        if password is not None:
            cmd += ["--rc-pass", password]
        if other_args:
            cmd += other_args
        out = self._launch_process(cmd, capture=False)
        time.sleep(1)  # Give it some time to launch
        return out

    def remote_control(
        self,
        addr: str,
        user: str | None = None,
        password: str | None = None,
        capture: bool | None = None,
        other_args: list[str] | None = None,
    ) -> CompletedProcess:
        cmd = ["rc"]
        if addr:
            cmd += ["--rc-addr", addr]
        if user is not None:
            cmd += ["--rc-user", user]
        if password is not None:
            cmd += ["--rc-pass", password]
        if other_args:
            cmd += other_args
        cp = self._run(cmd, capture=capture)
        return CompletedProcess.from_subprocess(cp)

    def obscure(self, password: str) -> str:
        """Obscure a password for use in rclone config files."""
        cmd_list: list[str] = ["obscure", password]
        cp = self._run(cmd_list)
        return cp.stdout.strip()

    def ls_stream(
        self,
        path: str,
        max_depth: int = -1,
        fast_list: bool = False,
    ) -> FilesStream:
        """
        List files in the given path

        Args:
            src: Remote path to list
            max_depth: Maximum recursion depth (-1 for unlimited)
            fast_list: Use fast list (only use when getting THE entire data repository from the root/bucket, or it's small)
        """
        cmd = ["lsjson", path, "--files-only"]
        recurse = max_depth < 0 or max_depth > 1
        if recurse:
            cmd.append("-R")
            if max_depth > 1:
                cmd += ["--max-depth", str(max_depth)]
        if fast_list:
            cmd.append("--fast-list")
        streamer = FilesStream(path, self._launch_process(cmd, capture=True))
        return streamer

    def save_to_db(
        self,
        src: str,
        db_url: str,
        max_depth: int = -1,
        fast_list: bool = False,
    ) -> None:
        """
        Save files to a database (sqlite, mysql, postgres)

        Args:
            src: Remote path to list, this will be used to populate an entire table, so always use the root-most path.
            db_url: Database URL, like sqlite:///data.db or mysql://user:pass@localhost/db or postgres://user:pass@localhost/db
            max_depth: Maximum depth to traverse (-1 for unlimited)
            fast_list: Use fast list (only use when getting THE entire data repository from the root/bucket)

        """
        from rclone_api.db import DB

        db = DB(db_url)
        with self.ls_stream(src, max_depth, fast_list) as stream:
            for page in stream.files_paged(page_size=10000):
                db.add_files(page)

    def ls(
        self,
        path: Dir | Remote | str,
        max_depth: int | None = None,
        glob: str | None = None,
        order: Order = Order.NORMAL,
        listing_option: ListingOption = ListingOption.ALL,
    ) -> DirListing:
        """List files in the given path.

        Args:
            path: Remote path or Remote object to list
            max_depth: Maximum recursion depth (0 means no recursion)

        Returns:
            List of File objects found at the path
        """

        if isinstance(path, str):
            path = Dir(
                to_path(path, self)
            )  # assume it's a directory if ls is being called.

        cmd = ["lsjson"]
        if max_depth is not None:
            if max_depth < 0:
                cmd.append("--recursive")
            if max_depth > 0:
                cmd.append("--max-depth")
                cmd.append(str(max_depth))
        if listing_option != ListingOption.ALL:
            cmd.append(f"--{listing_option.value}")

        cmd.append(str(path))
        remote = path.remote if isinstance(path, Dir) else path
        assert isinstance(remote, Remote)

        cp = self._run(cmd, check=True)
        text = cp.stdout
        parent_path: str | None = None
        if isinstance(path, Dir):
            parent_path = path.path.path
        paths: list[RPath] = RPath.from_json_str(text, remote, parent_path=parent_path)
        # print(parent_path)
        for o in paths:
            o.set_rclone(self)

        # do we have a glob pattern?
        if glob is not None:
            paths = [p for p in paths if fnmatch(p.path, glob)]

        if order == Order.REVERSE:
            paths.reverse()
        elif order == Order.RANDOM:
            random.shuffle(paths)
        return DirListing(paths)

    def listremotes(self) -> list[Remote]:
        cmd = ["listremotes"]
        cp = self._run(cmd)
        text: str = cp.stdout
        tmp = text.splitlines()
        tmp = [t.strip() for t in tmp]
        # strip out ":" from the end
        tmp = [t.replace(":", "") for t in tmp]
        out = [Remote(name=t, rclone=self) for t in tmp]
        return out

    def diff(
        self,
        src: str,
        dst: str,
        min_size: (
            str | None
        ) = None,  # e. g. "1MB" - see rclone documentation: https://rclone.org/commands/rclone_check/
        max_size: (
            str | None
        ) = None,  # e. g. "1GB" - see rclone documentation: https://rclone.org/commands/rclone_check/
        diff_option: DiffOption = DiffOption.COMBINED,
        fast_list: bool = True,
        size_only: bool | None = None,
        checkers: int | None = None,
        other_args: list[str] | None = None,
    ) -> Generator[DiffItem, None, None]:
        """Be extra careful with the src and dst values. If you are off by one
        parent directory, you will get a huge amount of false diffs."""
        other_args = other_args or []
        if checkers is None or checkers < 1:
            checkers = 1000
        cmd = [
            "check",
            src,
            dst,
            "--checkers",
            str(checkers),
            "--log-level",
            "INFO",
            f"--{diff_option.value}",
            "-",
        ]
        if size_only is None:
            size_only = diff_option in [
                DiffOption.MISSING_ON_DST,
                DiffOption.MISSING_ON_SRC,
            ]
        if size_only:
            cmd += ["--size-only"]
        if fast_list:
            cmd += ["--fast-list"]
        if min_size:
            cmd += ["--min-size", min_size]
        if max_size:
            cmd += ["--max-size", max_size]
        if diff_option == DiffOption.MISSING_ON_DST:
            cmd += ["--one-way"]
        if other_args:
            cmd += other_args
        proc = self._launch_process(cmd, capture=True)
        item: DiffItem
        for item in diff_stream_from_running_process(
            running_process=proc, src_slug=src, dst_slug=dst, diff_option=diff_option
        ):
            if item is None:
                break
            yield item

    def walk(
        self,
        path: Dir | Remote | str,
        max_depth: int = -1,
        breadth_first: bool = True,
        order: Order = Order.NORMAL,
    ) -> Generator[DirListing, None, None]:
        """Walk through the given path recursively.

        Args:
            path: Remote path or Remote object to walk through
            max_depth: Maximum depth to traverse (-1 for unlimited)

        Yields:
            DirListing: Directory listing for each directory encountered
        """
        dir_obj: Dir
        if isinstance(path, Dir):
            # Create a Remote object for the path
            remote = path.remote
            rpath = RPath(
                remote=remote,
                path=path.path.path,
                name=path.path.name,
                size=0,
                mime_type="inode/directory",
                mod_time="",
                is_dir=True,
            )
            rpath.set_rclone(self)
            dir_obj = Dir(rpath)
        elif isinstance(path, str):
            dir_obj = Dir(to_path(path, self))
        elif isinstance(path, Remote):
            dir_obj = Dir(path)
        else:
            dir_obj = Dir(path)  # shut up pyright
            assert f"Invalid type for path: {type(path)}"

        yield from walk(
            dir_obj, max_depth=max_depth, breadth_first=breadth_first, order=order
        )

    def scan_missing_folders(
        self,
        src: Dir | Remote | str,
        dst: Dir | Remote | str,
        max_depth: int = -1,
        order: Order = Order.NORMAL,
    ) -> Generator[Dir, None, None]:
        """Walk through the given path recursively.

        WORK IN PROGRESS!!

        Args:
            src: Source directory or Remote to walk through
            dst: Destination directory or Remote to walk through
            max_depth: Maximum depth to traverse (-1 for unlimited)

        Yields:
            DirListing: Directory listing for each directory encountered
        """
        from rclone_api.scan_missing_folders import scan_missing_folders

        src_dir = Dir(to_path(src, self))
        dst_dir = Dir(to_path(dst, self))
        yield from scan_missing_folders(
            src=src_dir, dst=dst_dir, max_depth=max_depth, order=order
        )

    def cleanup(
        self, path: str, other_args: list[str] | None = None
    ) -> CompletedProcess:
        """Cleanup any resources used by the Rclone instance."""
        # rclone cleanup remote:path [flags]
        cmd = ["cleanup", path]
        if other_args:
            cmd += other_args
        out = self._run(cmd)
        return CompletedProcess.from_subprocess(out)

    def copy_to(
        self,
        src: File | str,
        dst: File | str,
        check: bool | None = None,
        verbose: bool | None = None,
        other_args: list[str] | None = None,
    ) -> CompletedProcess:
        """Copy one file from source to destination.

        Warning - slow.

        """
        check = get_check(check)
        verbose = get_verbose(verbose)
        src = src if isinstance(src, str) else str(src.path)
        dst = dst if isinstance(dst, str) else str(dst.path)
        cmd_list: list[str] = ["copyto", src, dst]
        if other_args is not None:
            cmd_list += other_args
        cp = self._run(cmd_list, check=check)
        return CompletedProcess.from_subprocess(cp)

    def copy_files(
        self,
        src: str,
        dst: str,
        files: list[str] | Path,
        check: bool | None = None,
        max_backlog: int | None = None,
        verbose: bool | None = None,
        checkers: int | None = None,
        transfers: int | None = None,
        low_level_retries: int | None = None,
        retries: int | None = None,
        retries_sleep: str | None = None,
        metadata: bool | None = None,
        timeout: str | None = None,
        max_partition_workers: int | None = None,
        multi_thread_streams: int | None = None,
        other_args: list[str] | None = None,
    ) -> list[CompletedProcess]:
        """Copy multiple files from source to destination.

        Args:
            payload: Dictionary of source and destination file paths
        """
        check = get_check(check)
        max_partition_workers = max_partition_workers or 1
        low_level_retries = low_level_retries or 10
        retries = retries or 3
        other_args = other_args or []
        checkers = checkers or 1000
        transfers = transfers or 32
        verbose = get_verbose(verbose)
        payload: list[str] = (
            files
            if isinstance(files, list)
            else [f.strip() for f in files.read_text().splitlines() if f.strip()]
        )
        if len(payload) == 0:
            return []

        for p in payload:
            if ":" in p:
                raise ValueError(
                    f"Invalid file path, contains a remote, which is not allowed for copy_files: {p}"
                )

        using_fast_list = "--fast-list" in other_args
        if using_fast_list:
            warnings.warn(
                "It's not recommended to use --fast-list with copy_files as this will perform poorly on large repositories since the entire repository has to be scanned."
            )

        if max_partition_workers > 1:
            datalists: dict[str, list[str]] = group_files(
                payload, fully_qualified=False
            )
        else:
            datalists = {"": payload}
        # out: subprocess.CompletedProcess | None = None
        out: list[CompletedProcess] = []

        futures: list[Future] = []

        with ThreadPoolExecutor(max_workers=max_partition_workers) as executor:
            for common_prefix, files in datalists.items():

                def _task(
                    files: list[str] | Path = files,
                ) -> subprocess.CompletedProcess:
                    with TemporaryDirectory() as tmpdir:
                        filelist: list[str] = []
                        filepath: Path
                        if isinstance(files, list):
                            include_files_txt = Path(tmpdir) / "include_files.txt"
                            include_files_txt.write_text(
                                "\n".join(files), encoding="utf-8"
                            )
                            filelist = list(files)
                            filepath = Path(include_files_txt)
                        elif isinstance(files, Path):
                            filelist = [
                                f.strip()
                                for f in files.read_text().splitlines()
                                if f.strip()
                            ]
                            filepath = files
                        if common_prefix:
                            src_path = f"{src}/{common_prefix}"
                            dst_path = f"{dst}/{common_prefix}"
                        else:
                            src_path = src
                            dst_path = dst

                        if verbose:
                            nfiles = len(filelist)
                            files_fqdn = [f"  {src_path}/{f}" for f in filelist]
                            print(f"Copying {nfiles} files:")
                            chunk_size = 100
                            for i in range(0, nfiles, chunk_size):
                                chunk = files_fqdn[i : i + chunk_size]
                                files_str = "\n".join(chunk)
                                print(f"{files_str}")
                        cmd_list: list[str] = [
                            "copy",
                            src_path,
                            dst_path,
                            "--files-from",
                            str(filepath),
                            "--checkers",
                            str(checkers),
                            "--transfers",
                            str(transfers),
                            "--low-level-retries",
                            str(low_level_retries),
                            "--retries",
                            str(retries),
                        ]
                        if metadata:
                            cmd_list.append("--metadata")
                        if retries_sleep is not None:
                            cmd_list += ["--retries-sleep", retries_sleep]
                        if timeout is not None:
                            cmd_list += ["--timeout", timeout]
                        if max_backlog is not None:
                            cmd_list += ["--max-backlog", str(max_backlog)]
                        if multi_thread_streams is not None:
                            cmd_list += [
                                "--multi-thread-streams",
                                str(multi_thread_streams),
                            ]
                        if verbose:
                            if not any(["-v" in x for x in other_args]):
                                cmd_list.append("-vvvv")
                            if not any(["--progress" in x for x in other_args]):
                                cmd_list.append("--progress")
                        if other_args:
                            cmd_list += other_args
                        out = self._run(cmd_list, capture=not verbose)
                        return out

                fut: Future = executor.submit(_task)
                futures.append(fut)
            for fut in futures:
                cp: subprocess.CompletedProcess = fut.result()
                assert cp is not None
                out.append(CompletedProcess.from_subprocess(cp))
                if cp.returncode != 0:
                    if check:
                        raise ValueError(f"Error deleting files: {cp.stderr}")
                    else:
                        warnings.warn(f"Error deleting files: {cp.stderr}")
        return out

    def copy(
        self,
        src: Dir | str,
        dst: Dir | str,
        check: bool | None = None,
        transfers: int | None = None,
        checkers: int | None = None,
        multi_thread_streams: int | None = None,
        low_level_retries: int | None = None,
        retries: int | None = None,
        other_args: list[str] | None = None,
    ) -> CompletedProcess:
        """Copy files from source to destination.

        Args:
            src: Source directory
            dst: Destination directory
        """
        # src_dir = src.path.path
        # dst_dir = dst.path.path
        src_dir = convert_to_str(src)
        dst_dir = convert_to_str(dst)
        check = get_check(check)
        checkers = checkers or 1000
        transfers = transfers or 32
        low_level_retries = low_level_retries or 10
        retries = retries or 3
        cmd_list: list[str] = ["copy", src_dir, dst_dir]
        cmd_list += ["--checkers", str(checkers)]
        cmd_list += ["--transfers", str(transfers)]
        cmd_list += ["--low-level-retries", str(low_level_retries)]
        if multi_thread_streams is not None:
            cmd_list += ["--multi-thread-streams", str(multi_thread_streams)]
        if other_args:
            cmd_list += other_args
        cp = self._run(cmd_list, check=check, capture=False)
        return CompletedProcess.from_subprocess(cp)

    def purge(self, path: Dir | str) -> CompletedProcess:
        """Purge a directory"""
        # path should always be a string
        path = path if isinstance(path, str) else str(path.path)
        cmd_list: list[str] = ["purge", str(path)]
        cp = self._run(cmd_list)
        return CompletedProcess.from_subprocess(cp)

    def delete_files(
        self,
        files: str | File | list[str] | list[File],
        check: bool | None = None,
        rmdirs=False,
        verbose: bool | None = None,
        max_partition_workers: int | None = None,
        other_args: list[str] | None = None,
    ) -> CompletedProcess:
        """Delete a directory"""
        check = get_check(check)
        verbose = get_verbose(verbose)
        payload: list[str] = convert_to_filestr_list(files)
        if len(payload) == 0:
            if verbose:
                print("No files to delete")
            cp = subprocess.CompletedProcess(
                args=["rclone", "delete", "--files-from", "[]"],
                returncode=0,
                stdout="",
                stderr="",
            )
            return CompletedProcess.from_subprocess(cp)

        datalists: dict[str, list[str]] = group_files(payload)
        completed_processes: list[subprocess.CompletedProcess] = []

        futures: list[Future] = []

        with ThreadPoolExecutor(max_workers=max_partition_workers) as executor:

            for remote, files in datalists.items():

                def _task(
                    files=files, check=check, remote=remote
                ) -> subprocess.CompletedProcess:
                    with TemporaryDirectory() as tmpdir:
                        include_files_txt = Path(tmpdir) / "include_files.txt"
                        include_files_txt.write_text("\n".join(files), encoding="utf-8")

                        # print(include_files_txt)
                        cmd_list: list[str] = [
                            "delete",
                            remote,
                            "--files-from",
                            str(include_files_txt),
                            "--checkers",
                            "1000",
                            "--transfers",
                            "1000",
                        ]
                        if verbose:
                            cmd_list.append("-vvvv")
                        if rmdirs:
                            cmd_list.append("--rmdirs")
                        if other_args:
                            cmd_list += other_args
                        out = self._run(cmd_list, check=check)
                    if out.returncode != 0:
                        if check:
                            completed_processes.append(out)
                            raise ValueError(f"Error deleting files: {out}")
                        else:
                            warnings.warn(f"Error deleting files: {out}")
                    return out

                fut: Future = executor.submit(_task)
                futures.append(fut)

            for fut in futures:
                out = fut.result()
                assert out is not None
                completed_processes.append(out)

        return CompletedProcess(completed_processes)

    @deprecated("delete_files")
    def deletefiles(
        self, files: str | File | list[str] | list[File]
    ) -> CompletedProcess:
        out = self.delete_files(files)
        return out

    def exists(self, path: Dir | Remote | str | File) -> bool:
        """Check if a file or directory exists."""
        arg: str = convert_to_str(path)
        assert isinstance(arg, str)
        try:
            dir_listing = self.ls(arg)
            # print(dir_listing)
            return len(dir_listing.dirs) > 0 or len(dir_listing.files) > 0
        except subprocess.CalledProcessError:
            return False

    def is_synced(self, src: str | Dir, dst: str | Dir) -> bool:
        """Check if two directories are in sync."""
        src = convert_to_str(src)
        dst = convert_to_str(dst)
        cmd_list: list[str] = ["check", str(src), str(dst)]
        try:
            self._run(cmd_list, check=True)
            return True
        except subprocess.CalledProcessError:
            return False

    def copy_file_resumable_s3(
        self,
        src: str,
        dst: str,
        save_state_json: Path,
        chunk_size: SizeSuffix | None = None,
        read_threads: int = 8,
        write_threads: int = 8,
        retries: int = 3,
        verbose: bool | None = None,
        max_chunks_before_suspension: int | None = None,
        mount_log: Path | None = None,
    ) -> MultiUploadResult:
        """For massive files that rclone can't handle in one go, this function will copy the file in chunks to an S3 store"""
        from rclone_api.s3.api import S3Client
        from rclone_api.s3.create import S3Credentials
        from rclone_api.util import S3PathInfo, split_s3_path

        other_args: list[str] = ["--no-modtime", "--vfs-read-wait", "1s"]
        chunk_size = chunk_size or SizeSuffix("64M")
        unit_chunk_size = chunk_size / read_threads
        tmp_mount_dir = self._get_tmp_mount_dir()
        vfs_read_chunk_size = unit_chunk_size
        vfs_read_chunk_size_limit = chunk_size
        vfs_read_chunk_streams = read_threads
        vfs_disk_space_total_size = chunk_size
        assert (
            chunk_size.as_int() % vfs_read_chunk_size.as_int() == 0
        ), f"chunk_size {chunk_size} must be a multiple of vfs_read_chunk_size {vfs_read_chunk_size}"
        other_args += ["--vfs-read-chunk-size", vfs_read_chunk_size.as_str()]
        other_args += [
            "--vfs-read-chunk-size-limit",
            vfs_read_chunk_size_limit.as_str(),
        ]
        other_args += ["--vfs-read-chunk-streams", str(vfs_read_chunk_streams)]
        other_args += [
            "--vfs-disk-space-total-size",
            vfs_disk_space_total_size.as_str(),
        ]
        other_args += ["--read-only"]
        other_args += ["--direct-io"]
        # --vfs-cache-max-size
        other_args += ["--vfs-cache-max-size", vfs_disk_space_total_size.as_str()]
        mount_path = tmp_mount_dir / "RCLONE_API_DYNAMIC_MOUNT"
        src_path = Path(src)
        name = src_path.name

        src_parent_path = Path(src).parent.as_posix()
        size_result: SizeResult = self.size_files(src_parent_path, [name])

        target_size = SizeSuffix(size_result.total_size)
        if target_size < SizeSuffix("5M"):
            # fallback to normal copy
            completed_proc = self.copy_to(src, dst, check=True)
            if completed_proc.ok:
                return MultiUploadResult.UPLOADED_FRESH
        if size_result.total_size <= 0:
            raise ValueError(
                f"File {src} has size {size_result.total_size}, is this a directory?"
            )

        path_info: S3PathInfo = split_s3_path(dst)
        remote = path_info.remote
        bucket_name = path_info.bucket
        s3_key = path_info.key
        parsed: Parsed = self.config.parse()
        sections: dict[str, Section] = parsed.sections
        if remote not in sections:
            raise ValueError(
                f"Remote {remote} not found in rclone config, remotes are: {sections.keys()}"
            )

        section: Section = sections[remote]
        dst_type = section.type()
        if dst_type != "s3" and dst_type != "b2":
            raise ValueError(
                f"Remote {remote} is not an S3 remote, it is of type {dst_type}"
            )

        def get_provider_str(section=section) -> str | None:
            type: str = section.type()
            provider: str | None = section.provider()
            if provider is not None:
                return provider
            if type == "b2":
                return S3Provider.BACKBLAZE.value
            if type != "s3":
                raise ValueError(f"Remote {remote} is not an S3 remote")
            return S3Provider.S3.value

        provider: str
        if provided_provider_str := get_provider_str():
            if verbose:
                print(f"Using provided provider: {provided_provider_str}")
            provider = provided_provider_str
        else:
            if verbose:
                print(f"Using default provider: {S3Provider.S3.value}")
            provider = S3Provider.S3.value
        provider_enum = S3Provider.from_str(provider)

        s3_creds: S3Credentials = S3Credentials(
            provider=provider_enum,
            access_key_id=section.access_key_id(),
            secret_access_key=section.secret_access_key(),
            endpoint_url=section.endpoint(),
        )

        chunk_fetcher: MultiMountFileChunker = self.get_multi_mount_file_chunker(
            src=src_path.as_posix(),
            chunk_size=chunk_size,
            threads=read_threads,
            mount_log=mount_log,
            direct_io=True,
        )

        client = S3Client(s3_creds)
        upload_config: S3MutliPartUploadConfig = S3MutliPartUploadConfig(
            chunk_size=chunk_size.as_int(),
            chunk_fetcher=chunk_fetcher.fetch,
            max_write_threads=write_threads,
            retries=retries,
            resume_path_json=save_state_json,
            max_chunks_before_suspension=max_chunks_before_suspension,
        )

        src_file = mount_path / name

        print(f"Uploading {name} to {s3_key} in bucket {bucket_name}")
        print(f"Source: {src_path}")
        print(f"bucket_name: {bucket_name}")
        print(f"upload_config: {upload_config}")

        # get the file size

        upload_target = S3UploadTarget(
            src_file=src_file,
            src_file_size=size_result.total_size,
            bucket_name=bucket_name,
            s3_key=s3_key,
        )

        try:
            out: MultiUploadResult = client.upload_file_multipart(
                upload_target=upload_target,
                upload_config=upload_config,
            )
            return out
        except Exception as e:
            print(f"Error uploading file: {e}")
            traceback.print_exc()
            raise
        finally:
            chunk_fetcher.shutdown()

    def get_multi_mount_file_chunker(
        self,
        src: str,
        chunk_size: SizeSuffix,
        threads: int,
        mount_log: Path | None,
        direct_io: bool,
    ) -> MultiMountFileChunker:
        from rclone_api.util import random_str

        mounts: list[Mount] = []
        vfs_read_chunk_size = chunk_size
        vfs_read_chunk_size_limit = chunk_size
        vfs_read_chunk_streams = 0
        vfs_disk_space_total_size = chunk_size
        other_args: list[str] = []
        other_args += ["--no-modtime"]
        other_args += ["--vfs-read-chunk-size", vfs_read_chunk_size.as_str()]
        other_args += [
            "--vfs-read-chunk-size-limit",
            vfs_read_chunk_size_limit.as_str(),
        ]
        other_args += ["--vfs-read-chunk-streams", str(vfs_read_chunk_streams)]
        other_args += [
            "--vfs-disk-space-total-size",
            vfs_disk_space_total_size.as_str(),
        ]
        other_args += ["--read-only"]
        if direct_io:
            other_args += ["--direct-io"]

        base_mount_dir = self._get_tmp_mount_dir()
        base_cache_dir = self._get_cache_dir()

        filename = Path(src).name
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures: list[Future] = []
            try:
                for i in range(threads):
                    tmp_mnts = base_mount_dir / random_str(12)
                    verbose = mount_log is not None

                    src_parent_path = Path(src).parent.as_posix()
                    cache_dir = base_cache_dir / random_str(12)

                    def task(
                        src_parent_path=src_parent_path,
                        tmp_mnts=tmp_mnts,
                        cache_dir=cache_dir,
                    ):
                        clean_mount(tmp_mnts, verbose=verbose)
                        prepare_mount(tmp_mnts, verbose=verbose)
                        return self.mount(
                            src=src_parent_path,
                            outdir=tmp_mnts,
                            allow_writes=False,
                            use_links=True,
                            vfs_cache_mode="minimal",
                            verbose=False,
                            cache_dir=cache_dir,
                            cache_dir_delete_on_exit=True,
                            log=mount_log,
                            other_args=other_args,
                        )

                    futures.append(executor.submit(task))
                mount_errors: list[Exception] = []
                for fut in futures:
                    try:
                        mount = fut.result()
                        mounts.append(mount)
                    except Exception as er:
                        warnings.warn(f"Error mounting: {er}")
                        mount_errors.append(er)
                if mount_errors:
                    warnings.warn(f"Error mounting: {mount_errors}")
                    raise Exception(mount_errors)
            except Exception:
                for mount in mounts:
                    mount.close()
                raise

        src_path: Path = Path(src)
        src_parent_path = src_path.parent.as_posix()
        name = src_path.name
        size_result: SizeResult = self.size_files(src_parent_path, [name])
        filesize = size_result.total_size

        executor = ThreadPoolExecutor(max_workers=threads)
        filechunker: MultiMountFileChunker = MultiMountFileChunker(
            filename=filename,
            filesize=filesize,
            mounts=mounts,
            executor=executor,
            verbose=mount_log is not None,
        )
        return filechunker

    def copy_bytes(
        self,
        src: str,
        offset: int | SizeSuffix,
        length: int | SizeSuffix,
        outfile: Path,
        other_args: list[str] | None = None,
    ) -> Exception | None:
        """Copy a slice of bytes from the src file to dst."""
        offset = SizeSuffix(offset).as_int()
        length = SizeSuffix(length).as_int()
        cmd_list: list[str] = [
            "cat",
            "--offset",
            str(offset),
            "--count",
            str(length),
            src,
        ]
        if other_args:
            cmd_list.extend(other_args)
        try:
            cp = self._run(cmd_list, capture=outfile)
            if cp.returncode == 0:
                return None
            return Exception(cp.stderr)
        except subprocess.CalledProcessError as e:
            return e

    def copy_bytes_mount(
        self,
        src: str,
        offset: int | SizeSuffix,
        length: int | SizeSuffix,
        chunk_size: SizeSuffix,
        max_threads: int = 1,
        # If outfile is supplied then bytes are written to this file and success returns bytes(0)
        outfile: Path | None = None,
        mount_log: Path | None = None,
        direct_io: bool = True,
    ) -> bytes | Exception:
        """Copy a slice of bytes from the src file to dst. Parallelism is achieved through multiple mounted files."""
        from rclone_api.types import FilePart

        offset = SizeSuffix(offset).as_int()
        length = SizeSuffix(length).as_int()
        # determine number of threads from chunk size
        threads = max(1, min(max_threads, length // chunk_size.as_int()))
        # todo - implement max threads.
        filechunker = self.get_multi_mount_file_chunker(
            src=src,
            chunk_size=chunk_size,
            threads=threads,
            mount_log=mount_log,
            direct_io=direct_io,
        )
        try:
            fut = filechunker.fetch(offset, length, extra=None)
            fp: FilePart = fut.result()
            payload = fp.payload
            if isinstance(payload, Exception):
                return payload
            try:
                if outfile is None:
                    return payload.read_bytes()
                shutil.move(payload, outfile)
                return bytes(0)
            finally:
                fp.close()

        except Exception as e:
            warnings.warn(f"Error copying bytes: {e}")
            return e
        finally:
            try:
                filechunker.shutdown()
            except Exception as e:
                warnings.warn(f"Error closing filechunker: {e}")

    def copy_dir(
        self, src: str | Dir, dst: str | Dir, args: list[str] | None = None
    ) -> CompletedProcess:
        """Copy a directory from source to destination."""
        # convert src to str, also dst
        src = convert_to_str(src)
        dst = convert_to_str(dst)
        cmd_list: list[str] = ["copy", src, dst]
        if args is not None:
            cmd_list += args
        cp = self._run(cmd_list)
        return CompletedProcess.from_subprocess(cp)

    def copy_remote(
        self, src: Remote, dst: Remote, args: list[str] | None = None
    ) -> CompletedProcess:
        """Copy a remote to another remote."""
        cmd_list: list[str] = ["copy", str(src), str(dst)]
        if args is not None:
            cmd_list += args
        # return self._run(cmd_list)
        cp = self._run(cmd_list)
        return CompletedProcess.from_subprocess(cp)

    def mount(
        self,
        src: Remote | Dir | str,
        outdir: Path,
        allow_writes: bool | None = False,
        use_links: bool | None = None,
        vfs_cache_mode: str | None = None,
        verbose: bool | None = None,
        cache_dir: Path | None = None,
        cache_dir_delete_on_exit: bool | None = None,
        log: Path | None = None,
        other_args: list[str] | None = None,
    ) -> Mount:
        """Mount a remote or directory to a local path.

        Args:
            src: Remote or directory to mount
            outdir: Local path to mount to

        Returns:
            CompletedProcess from the mount command execution

        Raises:
            subprocess.CalledProcessError: If the mount operation fails
        """

        allow_writes = allow_writes or False
        use_links = use_links or True
        verbose = get_verbose(verbose) or (log is not None)
        vfs_cache_mode = vfs_cache_mode or "full"
        clean_mount(outdir, verbose=verbose)
        prepare_mount(outdir, verbose=verbose)
        debug_fuse = log is not None
        src_str = convert_to_str(src)
        cmd_list: list[str] = ["mount", src_str, str(outdir)]
        if not allow_writes:
            cmd_list.append("--read-only")
        if use_links:
            cmd_list.append("--links")
        if vfs_cache_mode:
            cmd_list.append("--vfs-cache-mode")
            cmd_list.append(vfs_cache_mode)
        if cache_dir:
            cmd_list.append("--cache-dir")
            cmd_list.append(str(cache_dir.absolute()))
        if debug_fuse:
            cmd_list.append("--debug-fuse")
        if verbose:
            cmd_list.append("-vvvv")
        if other_args:
            cmd_list += other_args
        proc = self._launch_process(cmd_list, log=log)
        mount_read_only = not allow_writes
        mount: Mount = Mount(
            src=src_str,
            mount_path=outdir,
            process=proc,
            read_only=mount_read_only,
            cache_dir=cache_dir,
            cache_dir_delete_on_exit=cache_dir_delete_on_exit,
        )
        return mount

    @contextmanager
    def scoped_mount(
        self,
        src: Remote | Dir | str,
        outdir: Path,
        allow_writes: bool | None = None,
        use_links: bool | None = None,
        vfs_cache_mode: str | None = None,
        verbose: bool | None = None,
        log: Path | None = None,
        cache_dir: Path | None = None,
        cache_dir_delete_on_exit: bool | None = None,
        other_args: list[str] | None = None,
    ) -> Generator[Mount, None, None]:
        """Like mount, but can be used in a context manager."""
        error_happened = False
        mount: Mount = self.mount(
            src,
            outdir,
            allow_writes=allow_writes,
            use_links=use_links,
            vfs_cache_mode=vfs_cache_mode,
            verbose=verbose,
            cache_dir=cache_dir,
            cache_dir_delete_on_exit=cache_dir_delete_on_exit,
            log=log,
            other_args=other_args,
        )
        try:
            yield mount
        except Exception as e:
            error_happened = True
            stack_trace = traceback.format_exc()
            warnings.warn(f"Error in scoped_mount: {e}\n\nStack Trace:\n{stack_trace}")
            raise
        finally:
            if not error_happened or (not allow_writes):
                mount.close()

    # Settings optimized for s3.
    def mount_s3(
        self,
        url: str,
        outdir: Path,
        allow_writes=False,
        vfs_cache_mode="full",
        dir_cache_time: str | None = "1h",
        attribute_timeout: str | None = "1h",
        vfs_disk_space_total_size: str | None = "100M",
        transfers: int | None = 128,
        modtime_strategy: (
            ModTimeStrategy | None
        ) = ModTimeStrategy.USE_SERVER_MODTIME,  # speeds up S3 operations
        vfs_read_chunk_streams: int | None = 16,
        vfs_read_chunk_size: str | None = "4M",
        vfs_fast_fingerprint: bool = True,
        # vfs-refresh
        vfs_refresh: bool = True,
        other_args: list[str] | None = None,
    ) -> Mount:
        """Mount a remote or directory to a local path.

        Args:
            src: Remote or directory to mount
            outdir: Local path to mount to
        """
        other_args = other_args or []
        if modtime_strategy is not None:
            other_args.append(f"--{modtime_strategy.value}")
        if (vfs_cache_mode == "full" or vfs_cache_mode == "writes") and (
            transfers is not None and "--transfers" not in other_args
        ):
            other_args.append("--transfers")
            other_args.append(str(transfers))
        if dir_cache_time is not None and "--dir-cache-time" not in other_args:
            other_args.append("--dir-cache-time")
            other_args.append(dir_cache_time)
        if (
            vfs_disk_space_total_size is not None
            and "--vfs-cache-max-size" not in other_args
        ):
            other_args.append("--vfs-cache-max-size")
            other_args.append(vfs_disk_space_total_size)
        if vfs_refresh and "--vfs-refresh" not in other_args:
            other_args.append("--vfs-refresh")
        if attribute_timeout is not None and "--attr-timeout" not in other_args:
            other_args.append("--attr-timeout")
            other_args.append(attribute_timeout)
        if vfs_read_chunk_streams:
            other_args.append("--vfs-read-chunk-streams")
            other_args.append(str(vfs_read_chunk_streams))
        if vfs_read_chunk_size:
            other_args.append("--vfs-read-chunk-size")
            other_args.append(vfs_read_chunk_size)
        if vfs_fast_fingerprint:
            other_args.append("--vfs-fast-fingerprint")

        other_args = other_args if other_args else None
        return self.mount(
            url,
            outdir,
            allow_writes=allow_writes,
            vfs_cache_mode=vfs_cache_mode,
            other_args=other_args,
        )

    def serve_webdav(
        self,
        src: Remote | Dir | str,
        user: str,
        password: str,
        addr: str = "localhost:2049",
        allow_other: bool = False,
        other_args: list[str] | None = None,
    ) -> Process:
        """Serve a remote or directory via NFS.

        Args:
            src: Remote or directory to serve
            addr: Network address and port to serve on (default: localhost:2049)
            allow_other: Allow other users to access the share

        Returns:
            Process: The running webdev server process

        Raises:
            ValueError: If the NFS server fails to start
        """
        src_str = convert_to_str(src)
        cmd_list: list[str] = ["serve", "webdav", "--addr", addr, src_str]
        cmd_list.extend(["--user", user, "--pass", password])
        if allow_other:
            cmd_list.append("--allow-other")
        if other_args:
            cmd_list += other_args
        proc = self._launch_process(cmd_list)
        time.sleep(2)  # give it a moment to start
        if proc.poll() is not None:
            raise ValueError("NFS serve process failed to start")
        return proc

    def size_files(
        self,
        src: str,
        files: list[str],
        fast_list: bool = False,  # Recommend that this is False
        other_args: list[str] | None = None,
        check: bool | None = False,
        verbose: bool | None = None,
    ) -> SizeResult:
        """Get the size of a list of files. Example of files items: "remote:bucket/to/file"."""
        verbose = get_verbose(verbose)
        check = get_check(check)
        if fast_list or (other_args and "--fast-list" in other_args):
            warnings.warn(
                "It's not recommended to use --fast-list with size_files as this will perform poorly on large repositories since the entire repository has to be scanned."
            )
        files = list(files)
        all_files: list[File] = []
        # prefix, files = group_under_one_prefix(src, files)
        cmd = ["lsjson", src, "--files-only", "-R"]
        with TemporaryDirectory() as tmpdir:
            # print("files: " + ",".join(files))
            include_files_txt = Path(tmpdir) / "include_files.txt"
            include_files_txt.write_text("\n".join(files), encoding="utf-8")
            cmd += ["--files-from", str(include_files_txt)]
            if fast_list:
                cmd.append("--fast-list")
            if other_args:
                cmd += other_args
            cp = self._run(cmd, check=check)

            if cp.returncode != 0:
                if check:
                    raise ValueError(f"Error getting file sizes: {cp.stderr}")
                else:
                    warnings.warn(f"Error getting file sizes: {cp.stderr}")
            stdout = cp.stdout
            pieces = src.split(":", 1)
            remote_name = pieces[0]
            parent_path: str | None
            if len(pieces) > 1:
                parent_path = pieces[1]
            else:
                parent_path = None
            remote = Remote(name=remote_name, rclone=self)
            paths: list[RPath] = RPath.from_json_str(
                stdout, remote, parent_path=parent_path
            )
            # print(paths)
            all_files += [File(p) for p in paths]
        file_sizes: dict[str, int] = {}
        f: File
        for f in all_files:
            p = f.to_string(include_remote=True)
            if p in file_sizes:
                warnings.warn(f"Duplicate file found: {p}")
                continue
            size = f.size
            if size == 0:
                warnings.warn(f"File size is 0: {p}")
            file_sizes[p] = f.size
        total_size = sum(file_sizes.values())
        file_sizes_path_corrected: dict[str, int] = {}
        for path, size in file_sizes.items():
            # remove the prefix
            path_path = Path(path)
            path_str = path_path.relative_to(src).as_posix()
            file_sizes_path_corrected[path_str] = size
        out: SizeResult = SizeResult(
            prefix=src, total_size=total_size, file_sizes=file_sizes_path_corrected
        )
        return out
