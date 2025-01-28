"""
Unit test file.
"""

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory


@dataclass
class RcloneConfig:
    """Rclone configuration dataclass."""

    text: str


def _rclone_execute(
    cmd: list[str],
    rclone_conf: Path | RcloneConfig,
    rclone_exe: Path,
    verbose: bool = False,
) -> subprocess.CompletedProcess:
    print(subprocess.list2cmdline(cmd))
    tempdir: TemporaryDirectory | None = None

    try:
        if isinstance(rclone_conf, RcloneConfig):
            tempdir = TemporaryDirectory()
            tmpfile = Path(tempdir.name) / "rclone.conf"
            tmpfile.write_text(rclone_conf.text, encoding="utf-8")
            rclone_conf = tmpfile
        cmd = (
            [str(rclone_exe.resolve())] + ["--config", str(rclone_conf.resolve())] + cmd
        )
        if verbose:
            cmd_str = subprocess.list2cmdline(cmd)
            print(f"Running: {cmd_str}")
        cp = subprocess.run(
            cmd, capture_output=True, encoding="utf-8", check=True, shell=False
        )
        return cp
    finally:
        if tempdir:
            try:
                tempdir.cleanup()
            except Exception as e:
                print(f"Error cleaning up tempdir: {e}")


@dataclass
class RcloneExec:
    """Rclone execution dataclass."""

    rclone_config: Path | RcloneConfig
    rclone_exe: Path

    def execute(self, cmd: list[str]) -> subprocess.CompletedProcess:
        """Execute rclone command."""
        return _rclone_execute(cmd, self.rclone_config, self.rclone_exe)


@dataclass
class RemoteFile:
    """Remote file dataclass."""

    path: str
    name: str
    size: int
    mime_type: str
    mod_time: str
    is_dir: bool
    # is_bucket: bool

    @staticmethod
    def from_dict(data: dict) -> "RemoteFile":
        """Create a RemoteFile from a dictionary."""
        return RemoteFile(
            data["Path"],
            data["Name"],
            data["Size"],
            data["MimeType"],
            data["ModTime"],
            data["IsDir"],
            # data["IsBucket"],
        )

    @staticmethod
    def from_array(data: list[dict]) -> list["RemoteFile"]:
        """Create a RemoteFile from a dictionary."""
        out: list[RemoteFile] = []
        for d in data:
            file: RemoteFile = RemoteFile.from_dict(d)
            out.append(file)
        return out

    @staticmethod
    def from_json_str(json_str: str) -> list["RemoteFile"]:
        """Create a RemoteFile from a JSON string."""
        json_obj = json.loads(json_str)
        if isinstance(json_obj, dict):
            return [RemoteFile.from_dict(json_obj)]
        return RemoteFile.from_array(json_obj)

    def to_json(self) -> dict:
        return {
            "Path": self.path,
            "Name": self.name,
            "Size": self.size,
            "MimeType": self.mime_type,
            "ModTime": self.mod_time,
            "IsDir": self.is_dir,
            # "IsBucket": self.is_bucket,
        }

    def __str__(self) -> str:
        out = self.to_json()
        return json.dumps(out)


def _get_rclone_exe(rclone_exe: Path | None) -> Path:
    if rclone_exe is None:

        rclone_which_path = shutil.which("rclone")
        if rclone_which_path is None:
            raise ValueError("rclone executable not found")
        return Path(rclone_which_path)
    return rclone_exe


class Rclone:
    def __init__(
        self, rclone_conf: Path | RcloneConfig, rclone_exe: Path | None = None
    ) -> None:
        if isinstance(rclone_conf, Path):
            if not rclone_conf.exists():
                raise ValueError(f"Rclone config file not found: {rclone_conf}")
        self._exec = RcloneExec(rclone_conf, _get_rclone_exe(rclone_exe))

    def _run(self, cmd: list[str]) -> subprocess.CompletedProcess:
        return self._exec.execute(cmd)

    def ls(self, path: str) -> list[RemoteFile]:
        cmd = ["lsjson", path]
        cp = self._run(cmd)
        text = cp.stdout
        out: list[RemoteFile] = RemoteFile.from_json_str(text)
        return out

    def listremotes(self) -> list[str]:
        cmd = ["listremotes"]
        cp = self._run(cmd)
        text = cp.stdout
        out = text.splitlines()
        out = [o.strip() for o in out]
        # strip out ":" from the end
        out = [o.replace(":", "") for o in out]
        return out
