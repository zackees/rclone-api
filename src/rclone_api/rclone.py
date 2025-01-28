"""
Unit test file.
"""

import subprocess
from pathlib import Path

from rclone_api.file import File
from rclone_api.types import Config, RcloneExec, Remote
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

    def ls(self, path: str | Remote) -> list[File]:
        cmd = ["lsjson", str(path)]
        cp = self._run(cmd)
        text = cp.stdout
        out: list[File] = File.from_json_str(text)
        for o in out:
            o.set_rclone(self)
        return out

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
