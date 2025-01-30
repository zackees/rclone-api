import subprocess
from dataclasses import dataclass
from pathlib import Path

from rclone_api.config import Config


@dataclass
class RcloneExec:
    """Rclone execution dataclass."""

    rclone_config: Path | Config
    rclone_exe: Path

    def execute(self, cmd: list[str], check: bool) -> subprocess.CompletedProcess:
        """Execute rclone command."""
        from rclone_api.util import rclone_execute

        return rclone_execute(cmd, self.rclone_config, self.rclone_exe, check=check)
