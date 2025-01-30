"""
Unit test file.
"""

import subprocess
from concurrent.futures import ThreadPoolExecutor
from fnmatch import fnmatch
from pathlib import Path
from typing import Generator

from rclone_api import Dir
from rclone_api.config import Config
from rclone_api.convert import convert_to_filestr_list
from rclone_api.dir_listing import DirListing
from rclone_api.exec import RcloneExec
from rclone_api.file import File
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

    def _run(self, cmd: list[str]) -> subprocess.CompletedProcess:
        return self._exec.execute(cmd)

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

        cp = self._run(cmd)
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

    def copy(self, src: Dir, dst: Dir) -> None:
        """Copy files from source to destination.

        Args:
            src: Source directory
            dst: Destination directory
        """
        src_dir = src.path.path
        dst_dir = dst.path.path
        cmd_list: list[str] = ["copy", src_dir, dst_dir]
        self._run(cmd_list)

    def purge(self, path: Dir) -> None:
        """Purge a directory"""
        cmd_list: list[str] = ["purge", str(path)]
        self._run(cmd_list)

    def deletefiles(self, files: str | File | list[str] | list[File]) -> None:
        """Delete a directory"""
        payload: list[str] = convert_to_filestr_list(files)
        cmd_list: list[str] = ["delete"] + payload
        self._run(cmd_list)

    def exists(self, path: Dir | Remote | str | File) -> bool:
        """Check if a file or directory exists."""
        arg: str
        if isinstance(path, File):
            arg = str(path.path)
        elif isinstance(path, Remote):
            arg = str(path)
        elif isinstance(path, str):
            arg = path
        else:
            raise ValueError(f"Invalid type for path: {type(path)}")
        assert isinstance(arg, str)
        try:
            self.ls(arg)
            return True
        except subprocess.CalledProcessError:
            return False
