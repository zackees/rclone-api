"""
Unit test file.
"""

import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from fnmatch import fnmatch
from pathlib import Path
from typing import Generator

from rclone_api import Dir
from rclone_api.config import Config
from rclone_api.convert import convert_to_filestr_list, convert_to_str
from rclone_api.dir_listing import DirListing
from rclone_api.exec import RcloneExec
from rclone_api.file import File
from rclone_api.filelist import FileList
from rclone_api.process import Process
from rclone_api.remote import Remote
from rclone_api.rpath import RPath
from rclone_api.util import get_rclone_exe, to_path
from rclone_api.walk import walk


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

    def _launch_process(self, cmd: list[str]) -> Process:
        return self._exec.launch_process(cmd)

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
            cmd.append("--recursive")
            if max_depth > -1:
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

    def walk(
        self, path: Dir | Remote | str, max_depth: int = -1
    ) -> Generator[DirListing, None, None]:
        """Walk through the given path recursively.

        Args:
            path: Remote path or Remote object to walk through
            max_depth: Maximum depth to traverse (-1 for unlimited)

        Yields:
            DirListing: Directory listing for each directory encountered
        """
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
            assert f"Invalid type for path: {type(path)}"

        yield from walk(dir_obj, max_depth=max_depth)

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

    def copyfiles(self, filelist: dict[File, File] | dict[str, str]) -> None:
        """Copy multiple files from source to destination.

        Warning - slow.

        Args:
            payload: Dictionary of source and destination file paths
        """
        str_dict: dict[str, str] = {}
        for src, dst in filelist.items():
            src = src if isinstance(src, str) else str(src.path)
            dst = dst if isinstance(dst, str) else str(dst.path)
            str_dict[src] = dst

        with ThreadPoolExecutor(max_workers=64) as executor:
            for src, dst in str_dict.items():  # warning - slow
                cmd_list: list[str] = ["copyto", src, dst]
                # self._run(cmd_list)
                executor.submit(self._run, cmd_list)

    def copy(
        self, src: Dir | str, dst: Dir | str, filelist: FileList | None = None
    ) -> subprocess.CompletedProcess:
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
        return self._run(cmd_list)

    def purge(self, path: Dir | str) -> subprocess.CompletedProcess:
        """Purge a directory"""
        # path should always be a string
        path = path if isinstance(path, str) else str(path.path)
        cmd_list: list[str] = ["purge", str(path)]
        return self._run(cmd_list)

    def deletefiles(
        self, files: str | File | list[str] | list[File]
    ) -> subprocess.CompletedProcess:
        """Delete a directory"""
        payload: list[str] = convert_to_filestr_list(files)
        cmd_list: list[str] = ["delete"] + payload
        return self._run(cmd_list)

    def exists(self, path: Dir | Remote | str | File) -> bool:
        """Check if a file or directory exists."""
        arg: str = convert_to_str(path)
        assert isinstance(arg, str)
        try:
            self.ls(arg)
            return True
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
    ) -> subprocess.CompletedProcess:
        """Copy a directory from source to destination."""
        # convert src to str, also dst
        src = convert_to_str(src)
        dst = convert_to_str(dst)
        cmd_list: list[str] = ["copy", src, dst]
        if args is not None:
            cmd_list += args
        return self._run(cmd_list)

    def copy_remote(
        self, src: Remote, dst: Remote, args: list[str] | None = None
    ) -> subprocess.CompletedProcess:
        """Copy a remote to another remote."""
        cmd_list: list[str] = ["copy", str(src), str(dst)]
        if args is not None:
            cmd_list += args
        return self._run(cmd_list)

    def mount(
        self, src: Remote | Dir | str, outdir: Path, allow_writes=False, use_links=True
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
        src_str = convert_to_str(src)
        cmd_list: list[str] = ["mount", src_str, str(outdir)]
        if not allow_writes:
            cmd_list.append("--read-only")
        if use_links:
            cmd_list.append("--links")
        proc = self._launch_process(cmd_list)
        time.sleep(2)  # give it a moment to mount
        if proc.poll() is not None:
            raise ValueError("Mount process failed to start")
        return proc
