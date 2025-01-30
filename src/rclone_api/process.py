import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from rclone_api.config import Config
from rclone_api.util import get_verbose

# def rclone_launch_process(
#     cmd: list[str],
#     rclone_conf: Path | Config,
#     rclone_exe: Path,
#     verbose: bool | None = None,
# ) -> subprocess.Popen:
#     tempdir: TemporaryDirectory | None = None
#     verbose = _get_verbose(verbose)
#     assert verbose is not None

#     try:
#         if isinstance(rclone_conf, Config):
#             tempdir = TemporaryDirectory()
#             tmpfile = Path(tempdir.name) / "rclone.conf"
#             tmpfile.write_text(rclone_conf.text, encoding="utf-8")
#             rclone_conf = tmpfile
#         cmd = (
#             [str(rclone_exe.resolve())] + ["--config", str(rclone_conf.resolve())] + cmd
#         )
#         if verbose:
#             cmd_str = subprocess.list2cmdline(cmd)
#             print(f"Running: {cmd_str}")
#         cp = subprocess.Popen(
#             cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False
#         )
#         return cp
#     finally:
#         if tempdir:
#             try:
#                 tempdir.cleanup()
#             except Exception as e:
#                 print(f"Error cleaning up tempdir: {e}")


def _get_verbose(verbose: bool | None) -> bool:
    if verbose is not None:
        return verbose
    # get it from the environment
    return bool(int(os.getenv("RCLONE_API_VERBOSE", "0")))


@dataclass
class ProcessArgs:
    cmd: list[str]
    rclone_conf: Path | Config
    rclone_exe: Path
    cmd_list: list[str]
    verbose: bool | None = None


class Process:
    def __init__(self, args: ProcessArgs) -> None:
        assert args.rclone_exe.exists()
        self.args = args
        self.tempdir: TemporaryDirectory | None = None
        verbose = get_verbose(args.verbose)
        if isinstance(args.rclone_conf, Config):
            self.tempdir = TemporaryDirectory()
            tmpfile = Path(self.tempdir.name) / "rclone.conf"
            tmpfile.write_text(args.rclone_conf.text, encoding="utf-8")
            rclone_conf = tmpfile
            self.needs_cleanup = True
        else:
            rclone_conf = args.rclone_conf
            self.needs_cleanup = False

        assert rclone_conf.exists()

        self.cmd = (
            [str(args.rclone_exe.resolve())]
            + ["--config", str(rclone_conf.resolve())]
            + args.cmd
        )
        if verbose:
            cmd_str = subprocess.list2cmdline(self.cmd)
            print(f"Running: {cmd_str}")
        self.process = subprocess.Popen(self.cmd, shell=False)

    def cleanup(self) -> None:
        if self.tempdir and self.needs_cleanup:
            try:
                self.tempdir.cleanup()
            except Exception as e:
                print(f"Error cleaning up tempdir: {e}")

    def __del__(self) -> None:
        self.cleanup()

    def kill(self) -> None:
        self.cleanup()
        return self.process.kill()

    def terminate(self) -> None:
        self.cleanup()
        return self.process.terminate()

    @property
    def returncode(self) -> int | None:
        return self.process.returncode

    @property
    def stdout(self) -> Any:
        return self.process.stdout

    @property
    def stderr(self) -> Any:
        return self.process.stderr

    def poll(self) -> int | None:
        return self.process.poll()

    def wait(self) -> int:
        return self.process.wait()

    def send_signal(self, signal: int) -> None:
        return self.process.send_signal(signal)
