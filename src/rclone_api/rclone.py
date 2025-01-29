"""
Unit test file.
"""

import subprocess
from pathlib import Path
from typing import Generator

from rclone_api import Dir
from rclone_api.dir_listing import DirListing
from rclone_api.remote import Remote
from rclone_api.rpath import RPath
from rclone_api.types import Config, RcloneExec
from rclone_api.util import get_rclone_exe
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

    def ls(self, path: str | Remote, max_depth: int = 0) -> DirListing:
        """List files in the given path.

        Args:
            path: Remote path or Remote object to list
            max_depth: Maximum recursion depth (0 means no recursion)

        Returns:
            List of File objects found at the path
        """
        cmd = ["lsjson"]
        if max_depth > 0:
            cmd.extend(["--recursive", "--max-depth", str(max_depth)])
        cmd.append(str(path))

        cp = self._run(cmd)
        text = cp.stdout
        paths: list[RPath] = RPath.from_json_str(text)
        for o in paths:
            o.set_rclone(self)
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
        self, path: str | Remote, max_depth: int = -1
    ) -> Generator[DirListing, None, None]:
        """Walk through the given path recursively.

        Args:
            path: Remote path or Remote object to walk through
            max_depth: Maximum depth to traverse (-1 for unlimited)

        Yields:
            DirListing: Directory listing for each directory encountered
        """
        if isinstance(path, str):
            # Create a Remote object for the path
            rpath = RPath(
                path=path,
                name=path,
                size=0,
                mime_type="inode/directory",
                mod_time="",
                is_dir=True,
            )
            rpath.set_rclone(self)
            dir_obj = Dir(rpath)
        else:
            dir_obj = Dir(path)

        yield from walk(dir_obj, max_depth=max_depth)
