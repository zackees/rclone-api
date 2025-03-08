"""
Unit test file.
"""

import os
import random
import subprocess
import time
import traceback
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from fnmatch import fnmatch
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator

from rclone_api import Dir
from rclone_api.completed_process import CompletedProcess
from rclone_api.config import Config, Parsed, Section
from rclone_api.convert import convert_to_filestr_list, convert_to_str
from rclone_api.deprecated import deprecated
from rclone_api.detail.walk import walk
from rclone_api.diff import DiffItem, DiffOption, diff_stream_from_running_process
from rclone_api.dir_listing import DirListing
from rclone_api.exec import RcloneExec
from rclone_api.file import File
from rclone_api.file_stream import FilesStream
from rclone_api.group_files import group_files
from rclone_api.http_server import HttpServer
from rclone_api.mount import Mount, clean_mount, prepare_mount
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
    PartInfo,
    SizeResult,
    SizeSuffix,
)
from rclone_api.util import (
    find_free_port,
    get_check,
    get_rclone_exe,
    get_verbose,
    to_path,
)


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


class RcloneImpl:
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
        path: Dir | Remote | str | None = None,
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

        if path is None:
            # list remotes instead
            list_remotes: list[Remote] = self.listremotes()
            dirs: list[Dir] = [Dir(remote) for remote in list_remotes]
            for d in dirs:
                d.path.path = ""
            rpaths = [d.path for d in dirs]
            return DirListing(rpaths)

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

    def print(self, path: str) -> Exception | None:
        """Print the contents of a file."""
        try:
            text_or_err = self.read_text(path)
            if isinstance(text_or_err, Exception):
                return text_or_err
            print(text_or_err)
        except Exception as e:
            return e
        return None

    def stat(self, src: str) -> File | Exception:
        """Get the status of a file or directory."""
        dirlist: DirListing = self.ls(src)
        if len(dirlist.files) == 0:
            # raise FileNotFoundError(f"File not found: {src}")
            return FileNotFoundError(f"File not found: {src}")
        try:
            file: File = dirlist.files[0]
            return file
        except Exception as e:
            return e

    def modtime(self, src: str) -> str | Exception:
        """Get the modification time of a file or directory."""
        try:
            file: File | Exception = self.stat(src)
            if isinstance(file, Exception):
                return file
            return file.mod_time()
        except Exception as e:
            return e

    def modtime_dt(self, src: str) -> datetime | Exception:
        """Get the modification time of a file or directory."""
        modtime: str | Exception = self.modtime(src)
        if isinstance(modtime, Exception):
            return modtime
        return datetime.fromisoformat(modtime)

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
        cmd_list: list[str] = ["copyto", src, dst, "--s3-no-check-bucket"]
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
        other_args.append("--s3-no-check-bucket")
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
        cmd_list.append("--s3-no-check-bucket")
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

    def copy_file_parts(
        self,
        src: str,  # src:/Bucket/path/myfile.large.zst
        dst_dir: str,  # dst:/Bucket/path/myfile.large.zst-parts/
        part_infos: list[PartInfo] | None = None,
        threads: int = 1,
    ) -> Exception | None:
        """Copy parts of a file from source to destination."""
        from rclone_api.detail.copy_file_parts import copy_file_parts

        out = copy_file_parts(
            self=self,
            src=src,
            dst_dir=dst_dir,
            part_infos=part_infos,
            threads=threads,
        )
        return out

    def write_text(
        self,
        dst: str,
        text: str,
    ) -> Exception | None:
        """Write text to a file."""
        data = text.encode("utf-8")
        return self.write_bytes(dst=dst, data=data)

    def write_bytes(
        self,
        dst: str,
        data: bytes,
    ) -> Exception | None:
        """Write bytes to a file."""
        with TemporaryDirectory() as tmpdir:
            tmpfile = Path(tmpdir) / "file.bin"
            tmpfile.write_bytes(data)
            completed_proc = self.copy_to(str(tmpfile), dst, check=True)
            if completed_proc.returncode != 0:
                return Exception(f"Failed to write bytes to {dst}", completed_proc)
        return None

    def read_bytes(self, src: str) -> bytes | Exception:
        """Read bytes from a file."""
        with TemporaryDirectory() as tmpdir:
            tmpfile = Path(tmpdir) / "file.bin"
            completed_proc = self.copy_to(src, str(tmpfile), check=True)
            if completed_proc.returncode != 0:
                return Exception(f"Failed to read bytes from {src}", completed_proc)

            if not tmpfile.exists():
                return Exception(f"Failed to read bytes from {src}, file not found")
            try:
                return tmpfile.read_bytes()
            except Exception as e:
                return Exception(f"Failed to read bytes from {src}", e)

    def read_text(self, src: str) -> str | Exception:
        """Read text from a file."""
        data = self.read_bytes(src)
        if isinstance(data, Exception):
            return data
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as e:
            return Exception(f"Failed to decode text from {src}", e)

    def size_file(self, src: str) -> SizeSuffix | Exception:
        """Get the size of a file or directory."""
        src_parent = os.path.dirname(src)
        src_name = os.path.basename(src)
        out: SizeResult = self.size_files(src_parent, [src_name])
        one_file = len(out.file_sizes) == 1
        if not one_file:
            return Exception(
                f"More than one result returned, is this is a directory? {out}"
            )
        return SizeSuffix(out.total_size)

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
        backend_log: Path | None = None,
    ) -> MultiUploadResult:
        """For massive files that rclone can't handle in one go, this function will copy the file in chunks to an S3 store"""
        from rclone_api.http_server import HttpFetcher, HttpServer
        from rclone_api.s3.api import S3Client
        from rclone_api.s3.create import S3Credentials
        from rclone_api.util import S3PathInfo, split_s3_path

        src_path = Path(src)
        name = src_path.name
        src_parent_path = Path(src).parent.as_posix()

        size_result: SizeResult = self.size_files(src_parent_path, [name])
        target_size = SizeSuffix(size_result.total_size)

        chunk_size = chunk_size or SizeSuffix("64M")
        MAX_CHUNKS = 10000
        min_chunk_size = SizeSuffix(size_result.total_size // (MAX_CHUNKS - 1))
        if min_chunk_size > chunk_size:
            warnings.warn(
                f"Chunk size {chunk_size} is too small for file size {size_result.total_size}, setting to {min_chunk_size}"
            )
            chunk_size = SizeSuffix(min_chunk_size)

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

        port = random.randint(10000, 20000)
        http_server: HttpServer = self.serve_http(
            src=src_path.parent.as_posix(),
            addr=f"localhost:{port}",
            serve_http_log=backend_log,
        )
        chunk_fetcher: HttpFetcher = http_server.get_fetcher(
            path=src_path.name,
            n_threads=read_threads,
        )

        client = S3Client(s3_creds)
        upload_config: S3MutliPartUploadConfig = S3MutliPartUploadConfig(
            chunk_size=chunk_size.as_int(),
            chunk_fetcher=chunk_fetcher.bytes_fetcher,
            max_write_threads=write_threads,
            retries=retries,
            resume_path_json=save_state_json,
            max_chunks_before_suspension=max_chunks_before_suspension,
        )

        print(f"Uploading {name} to {s3_key} in bucket {bucket_name}")
        print(f"Source: {src_path}")
        print(f"bucket_name: {bucket_name}")
        print(f"upload_config: {upload_config}")

        upload_target = S3UploadTarget(
            src_file=src_path,
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

    def copy_dir(
        self, src: str | Dir, dst: str | Dir, args: list[str] | None = None
    ) -> CompletedProcess:
        """Copy a directory from source to destination."""
        # convert src to str, also dst
        src = convert_to_str(src)
        dst = convert_to_str(dst)
        cmd_list: list[str] = ["copy", src, dst, "--s3-no-check-bucket"]
        if args is not None:
            cmd_list += args
        cp = self._run(cmd_list)
        return CompletedProcess.from_subprocess(cp)

    def copy_remote(
        self, src: Remote, dst: Remote, args: list[str] | None = None
    ) -> CompletedProcess:
        """Copy a remote to another remote."""
        cmd_list: list[str] = ["copy", str(src), str(dst), "--s3-no-check-bucket"]
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

    def serve_http(
        self,
        src: str,
        addr: str | None = None,
        serve_http_log: Path | None = None,
        other_args: list[str] | None = None,
    ) -> HttpServer:
        """Serve a remote or directory via HTTP.

        Args:
            src: Remote or directory to serve
            addr: Network address and port to serve on (default: localhost:8080)
        """
        addr = addr or f"localhost:{find_free_port()}"
        _, subpath = src.split(":", 1)  # might not work on local paths.
        cmd_list: list[str] = ["serve", "http", "--addr", addr, src]
        if serve_http_log:
            cmd_list += ["--log-file", str(serve_http_log)]
            cmd_list += ["-vvvv"]
        if other_args:
            cmd_list += other_args
        proc = self._launch_process(cmd_list, log=serve_http_log)
        time.sleep(2)
        if proc.poll() is not None:
            raise ValueError("HTTP serve process failed to start")
        out: HttpServer = HttpServer(
            url=f"http://{addr}", subpath=subpath, process=proc
        )
        return out

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
