import shutil
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

from rclone_api.types import Config


def get_rclone_exe(rclone_exe: Path | None) -> Path:
    if rclone_exe is None:

        rclone_which_path = shutil.which("rclone")
        if rclone_which_path is None:
            raise ValueError("rclone executable not found")
        return Path(rclone_which_path)
    return rclone_exe


def rclone_execute(
    cmd: list[str],
    rclone_conf: Path | Config,
    rclone_exe: Path,
    verbose: bool = False,
) -> subprocess.CompletedProcess:
    print(subprocess.list2cmdline(cmd))
    tempdir: TemporaryDirectory | None = None

    try:
        if isinstance(rclone_conf, Config):
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
