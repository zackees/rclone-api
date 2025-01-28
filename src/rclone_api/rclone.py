"""
Unit test file.
"""

import subprocess
from pathlib import Path

from rclone_api.dir import Dir
from rclone_api.dir_listing import DirListing
from rclone_api.file import File
from rclone_api.remote import Remote
from rclone_api.rpath import RPath
from rclone_api.types import Config, RcloneExec
from rclone_api.util import get_rclone_exe


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
        dirs: list[Dir] = [Dir(o) for o in paths if o.is_dir]
        files: list[File] = [File(o) for o in paths if not o.is_dir]
        return DirListing(dirs=dirs, files=files)

    def listremotes(self) -> list[Remote]:
        cmd = ["listremotes"]
        cp = self._run(cmd)
        text: str = cp.stdout
        tmp = text.splitlines()
        tmp = [t.strip() for t in tmp]
        # strip out ":" from the end
        tmp = [t.replace(":", "") for t in tmp]
        out = [Remote(name=t) for t in tmp]
        return out
