"""
Unit test file.
"""

import os
import subprocess
import time
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from enum import Enum
from fnmatch import fnmatch
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator

from rclone_api import Dir
from rclone_api.completed_process import CompletedProcess
from rclone_api.config import Config
from rclone_api.convert import convert_to_filestr_list, convert_to_str
from rclone_api.deprecated import deprecated
from rclone_api.diff import DiffItem, diff_stream_from_running_process
from rclone_api.dir_listing import DirListing
from rclone_api.exec import RcloneExec
from rclone_api.file import File
from rclone_api.group_files import group_files
from rclone_api.process import Process
from rclone_api.remote import Remote
from rclone_api.rpath import RPath
from rclone_api.util import (
    get_rclone_exe,
    get_verbose,
    to_path,
    wait_for_mount,
)
from rclone_api.walk import walk

EXECUTOR = ThreadPoolExecutor(16)


def rclone_verbose(verbose: bool | None) -> bool:
    if verbose is not None:
        os.environ["RCLONE_API_VERBOSE"] = "1" if verbose else "0"
    return bool(int(os.getenv("RCLONE_API_VERBOSE", "0")))


class ModTimeStrategy(Enum):
    USE_SERVER_MODTIME = "use-server-modtime"
    NO_MODTIME = "no-modtime"


class Rclone:
    def __init__(
        self, rclone_conf: Path | Config, rclone_exe: Path | None = None
    ) -> None:
        if isinstance(rclone_conf, Path):
            if not rclone_conf.exists():
                raise ValueError(f"Rclone config file not found: {rclone_conf}")
        self._exec = RcloneExec(rclone_conf, get_rclone_exe(rclone_exe))

    def _run(self, cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
        return self._exec.execute(cmd, check=check)

    def _launch_process(self, cmd: list[str], capture: bool | None = None) -> Process:
        return self._exec.launch_process(cmd, capture=capture)

    def webgui(self, other_args: list[str] | None = None) -> Process:
        """Launch the Rclone web GUI."""
        cmd = ["rcd", "--rc-web-gui"]
        if other_args:
            cmd += other_args
        return self._launch_process(cmd, capture=False)

    def obscure(self, password: str) -> str:
        """Obscure a password for use in rclone config files."""
        cmd_list: list[str] = ["obscure", password]
        cp = self._run(cmd_list)
        return cp.stdout.strip()

    def ls(
        self,
        path: Dir | Remote | str,
        max_depth: int | None = None,
        glob: str | None = None,
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

    def diff(self, src: str, dst: str) -> Generator[DiffItem, None, None]:
        """Be extra careful with the src and dst values. If you are off by one
        parent directory, you will get a huge amount of false diffs."""
        cmd = [
            "check",
            src,
            dst,
            "--checkers",
            "1000",
            "--log-level",
            "INFO",
            "--combined",
            "-",
        ]
        proc = self._launch_process(cmd, capture=True)
        item: DiffItem
        for item in diff_stream_from_running_process(proc, src_slug=src, dst_slug=dst):
            if item is None:
                break
            yield item

    def walk(
        self, path: Dir | Remote | str, max_depth: int = -1, breadth_first: bool = True
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

        yield from walk(dir_obj, max_depth=max_depth, breadth_first=breadth_first)

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

    def copyfile(self, src: File | str, dst: File | str) -> None:
        """Copy a single file from source to destination.

        Args:
            src: Source file path (including remote if applicable)
            dst: Destination file path (including remote if applicable)

        Raises:
            subprocess.CalledProcessError: If the copy operation fails
        """
        src = src if isinstance(src, str) else str(src.path)
        dst = dst if isinstance(dst, str) else str(dst.path)
        cmd_list: list[str] = ["copyto", src, dst]
        self._run(cmd_list)

    def copy_to(self, src: File | str, dst: File | str) -> None:
        """Copy multiple files from source to destination.

        Warning - slow.

        Args:
            payload: Dictionary of source and destination file paths
        """
        src = str(src)
        dst = str(dst)
        cmd_list: list[str] = ["copyto", src, dst]
        self._run(cmd_list)

    def copyfiles(self, files: str | File | list[str] | list[File], check=True) -> None:
        """Copy multiple files from source to destination.

        Warning - slow.

        Args:
            payload: Dictionary of source and destination file paths
        """
        payload: list[str] = convert_to_filestr_list(files)
        if len(payload) == 0:
            return

        datalists: dict[str, list[str]] = group_files(payload)
        out: subprocess.CompletedProcess | None = None

        futures: list[Future] = []

        for remote, files in datalists.items():

            def _task(files=files) -> subprocess.CompletedProcess:
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
                    out = self._run(cmd_list)
                    return out

            fut: Future = EXECUTOR.submit(_task)
            futures.append(fut)
        for fut in futures:
            out = fut.result()
            assert out is not None
            if out.returncode != 0:
                if check:
                    raise ValueError(f"Error deleting files: {out.stderr}")
                else:
                    warnings.warn(f"Error deleting files: {out.stderr}")

    def copy(self, src: Dir | str, dst: Dir | str) -> CompletedProcess:
        """Copy files from source to destination.

        Args:
            src: Source directory
            dst: Destination directory
        """
        # src_dir = src.path.path
        # dst_dir = dst.path.path
        src_dir = convert_to_str(src)
        dst_dir = convert_to_str(dst)
        cmd_list: list[str] = ["copy", src_dir, dst_dir]
        cp = self._run(cmd_list)
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
        check=True,
        rmdirs=False,
        verbose: bool | None = None,
        other_args: list[str] | None = None,
    ) -> CompletedProcess:
        """Delete a directory"""
        payload: list[str] = convert_to_filestr_list(files)
        if len(payload) == 0:
            cp = subprocess.CompletedProcess(
                args=["rclone", "delete", "--files-from", "[]"],
                returncode=0,
                stdout="",
                stderr="",
            )
            return CompletedProcess.from_subprocess(cp)

        datalists: dict[str, list[str]] = group_files(payload)
        completed_processes: list[subprocess.CompletedProcess] = []
        verbose = get_verbose(verbose)

        futures: list[Future] = []

        for remote, files in datalists.items():

            def _task(files=files, check=check) -> subprocess.CompletedProcess:
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

            fut: Future = EXECUTOR.submit(_task)
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
            self._run(cmd_list)
            return True
        except subprocess.CalledProcessError:
            return False

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
        allow_writes=False,
        use_links=True,
        vfs_cache_mode="full",
        other_cmds: list[str] | None = None,
    ) -> Process:
        """Mount a remote or directory to a local path.

        Args:
            src: Remote or directory to mount
            outdir: Local path to mount to

        Returns:
            CompletedProcess from the mount command execution

        Raises:
            subprocess.CalledProcessError: If the mount operation fails
        """
        if outdir.exists():
            is_empty = not list(outdir.iterdir())
            if not is_empty:
                raise ValueError(
                    f"Mount directory already exists and is not empty: {outdir}"
                )
            outdir.rmdir()
        try:
            outdir.parent.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            warnings.warn(
                f"Permission error creating parent directory: {outdir.parent}"
            )
        src_str = convert_to_str(src)
        cmd_list: list[str] = ["mount", src_str, str(outdir)]
        if not allow_writes:
            cmd_list.append("--read-only")
        if use_links:
            cmd_list.append("--links")
        if vfs_cache_mode:
            cmd_list.append("--vfs-cache-mode")
            cmd_list.append(vfs_cache_mode)
        if other_cmds:
            cmd_list += other_cmds
        proc = self._launch_process(cmd_list)
        wait_for_mount(outdir, proc)
        return proc

    def mount_webdav(
        self,
        url: str,
        outdir: Path,
        vfs_cache_mode="full",
        vfs_disk_space_total_size: str | None = "10G",
        other_cmds: list[str] | None = None,
    ) -> Process:
        """Mount a remote or directory to a local path.

        Args:
            src: Remote or directory to mount
            outdir: Local path to mount to

        Returns:
            CompletedProcess from the mount command execution

        Raises:
            subprocess.CalledProcessError: If the mount operation fails
        """
        if outdir.exists():
            is_empty = not list(outdir.iterdir())
            if not is_empty:
                raise ValueError(
                    f"Mount directory already exists and is not empty: {outdir}"
                )
            outdir.rmdir()

        src_str = url
        cmd_list: list[str] = ["mount", src_str, str(outdir)]
        cmd_list.append("--vfs-cache-mode")
        cmd_list.append(vfs_cache_mode)
        if other_cmds:
            cmd_list += other_cmds
        if vfs_disk_space_total_size is not None:
            cmd_list.append("--vfs-cache-max-size")
            cmd_list.append(vfs_disk_space_total_size)
        proc = self._launch_process(cmd_list)
        wait_for_mount(outdir, proc)
        return proc

    def mount_s3(
        self,
        url: str,
        outdir: Path,
        allow_writes=False,
        vfs_cache_mode="full",
        # dir-cache-time
        dir_cache_time: str | None = "1h",
        attribute_timeout: str | None = "1h",
        # --vfs-cache-max-size
        # vfs-cache-max-size
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
        other_cmds: list[str] | None = None,
    ) -> Process:
        """Mount a remote or directory to a local path.

        Args:
            src: Remote or directory to mount
            outdir: Local path to mount to
        """
        other_cmds = other_cmds or []
        if modtime_strategy is not None:
            other_cmds.append(f"--{modtime_strategy.value}")
        if (vfs_cache_mode == "full" or vfs_cache_mode == "writes") and (
            transfers is not None and "--transfers" not in other_cmds
        ):
            other_cmds.append("--transfers")
            other_cmds.append(str(transfers))
        if dir_cache_time is not None and "--dir-cache-time" not in other_cmds:
            other_cmds.append("--dir-cache-time")
            other_cmds.append(dir_cache_time)
        if (
            vfs_disk_space_total_size is not None
            and "--vfs-cache-max-size" not in other_cmds
        ):
            other_cmds.append("--vfs-cache-max-size")
            other_cmds.append(vfs_disk_space_total_size)
        if vfs_refresh and "--vfs-refresh" not in other_cmds:
            other_cmds.append("--vfs-refresh")
        if attribute_timeout is not None and "--attr-timeout" not in other_cmds:
            other_cmds.append("--attr-timeout")
            other_cmds.append(attribute_timeout)
        if vfs_read_chunk_streams:
            other_cmds.append("--vfs-read-chunk-streams")
            other_cmds.append(str(vfs_read_chunk_streams))
        if vfs_read_chunk_size:
            other_cmds.append("--vfs-read-chunk-size")
            other_cmds.append(vfs_read_chunk_size)
        if vfs_fast_fingerprint:
            other_cmds.append("--vfs-fast-fingerprint")

        other_cmds = other_cmds if other_cmds else None
        return self.mount(
            url,
            outdir,
            allow_writes=allow_writes,
            vfs_cache_mode=vfs_cache_mode,
            other_cmds=other_cmds,
        )

    def serve_webdav(
        self,
        src: Remote | Dir | str,
        user: str,
        password: str,
        addr: str = "localhost:2049",
        allow_other: bool = False,
    ) -> Process:
        """Serve a remote or directory via NFS.

        Args:
            src: Remote or directory to serve
            addr: Network address and port to serve on (default: localhost:2049)
            allow_other: Allow other users to access the share

        Returns:
            Process: The running NFS server process

        Raises:
            ValueError: If the NFS server fails to start
        """
        src_str = convert_to_str(src)
        cmd_list: list[str] = ["serve", "webdav", "--addr", addr, src_str]
        cmd_list.extend(["--user", user, "--pass", password])
        if allow_other:
            cmd_list.append("--allow-other")
        proc = self._launch_process(cmd_list)
        time.sleep(2)  # give it a moment to start
        if proc.poll() is not None:
            raise ValueError("NFS serve process failed to start")
        return proc
