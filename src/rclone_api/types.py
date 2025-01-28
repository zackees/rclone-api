import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    """Rclone configuration dataclass."""

    text: str


@dataclass
class RcloneExec:
    """Rclone execution dataclass."""

    rclone_config: Path | Config
    rclone_exe: Path

    def execute(self, cmd: list[str]) -> subprocess.CompletedProcess:
        """Execute rclone command."""
        from rclone_api.util import rclone_execute

        return rclone_execute(cmd, self.rclone_config, self.rclone_exe)


class Remote:
    """Remote dataclass."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __str__(self) -> str:
        return f"{self.name}:"
