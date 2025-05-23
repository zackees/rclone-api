import subprocess
from dataclasses import dataclass
from pathlib import Path

from rclone_api.config import Config
from rclone_api.process import Process, ProcessArgs


@dataclass
class RcloneExec:
    """Rclone execution dataclass."""

    rclone_config: Path | Config | None
    rclone_exe: Path

    def execute(
        self, cmd: list[str], check: bool, capture: bool | Path | None = None
    ) -> subprocess.CompletedProcess:
        """Execute rclone command."""
        from rclone_api.util import rclone_execute

        return rclone_execute(
            cmd, self.rclone_config, self.rclone_exe, check=check, capture=capture
        )

    def launch_process(
        self, cmd: list[str], capture: bool | None, log: Path | None
    ) -> Process:
        """Launch rclone process."""

        args: ProcessArgs = ProcessArgs(
            cmd=cmd,
            rclone_conf=self.rclone_config,
            rclone_exe=self.rclone_exe,
            cmd_list=cmd,
            capture_stdout=capture,
            log=log,
        )
        process = Process(args)
        return process
