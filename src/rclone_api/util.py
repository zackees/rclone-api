import os
import shutil
import subprocess
import warnings
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from typing import Any

from rclone_api.config import Config
from rclone_api.dir import Dir
from rclone_api.remote import Remote
from rclone_api.rpath import RPath
from rclone_api.types import S3PathInfo

# from .rclone import Rclone

_PRINT_LOCK = Lock()


def locked_print(*args, **kwargs):
    with _PRINT_LOCK:
        print(*args, **kwargs)


def to_path(item: Dir | Remote | str, rclone: Any) -> RPath:
    from rclone_api.rclone import Rclone

    assert isinstance(rclone, Rclone)
    # if str then it will be remote:path
    if isinstance(item, str):
        # return RPath(item)
        # remote_name: str = item.split(":")[0]
        parts = item.split(":")
        remote_name = parts[0]
        path = ":".join(parts[1:])
        remote = Remote(name=remote_name, rclone=rclone)
        out = RPath(
            remote=remote,
            path=path,
            name="",
            size=0,
            mime_type="",
            mod_time="",
            is_dir=True,
        )
        out.set_rclone(rclone)
        return out
    elif isinstance(item, Dir):
        return item.path
    elif isinstance(item, Remote):
        out = RPath(
            remote=item,
            path=str(item),
            name=str(item),
            size=0,
            mime_type="inode/directory",
            mod_time="",
            is_dir=True,
        )
        out.set_rclone(rclone)
        return out
    else:
        raise ValueError(f"Invalid type for item: {type(item)}")


def get_verbose(verbose: bool | None) -> bool:
    if verbose is not None:
        return verbose
    # get it from the environment
    return bool(int(os.getenv("RCLONE_API_VERBOSE", "0")))


def get_check(check: bool | None) -> bool:
    if check is not None:
        return check
    # get it from the environment
    return bool(int(os.getenv("RCLONE_API_CHECK", "1")))


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
    check: bool,
    capture: bool | None = None,
    verbose: bool | None = None,
) -> subprocess.CompletedProcess:
    tempdir: TemporaryDirectory | None = None
    verbose = get_verbose(verbose)
    capture = capture if isinstance(capture, bool) else True
    assert verbose is not None

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
            print(f"\nRunning: {cmd_str}")
        cp = subprocess.run(
            cmd, capture_output=capture, encoding="utf-8", check=False, shell=False
        )
        if cp.returncode != 0:
            cmd_str = subprocess.list2cmdline(cmd)
            warnings.warn(
                f"Error running: {cmd_str}, returncode: {cp.returncode}\n{cp.stdout}\n{cp.stderr}"
            )
            if check:
                raise subprocess.CalledProcessError(
                    cp.returncode, cmd, cp.stdout, cp.stderr
                )
        return cp
    finally:
        if tempdir:
            try:
                tempdir.cleanup()
            except Exception as e:
                print(f"Error cleaning up tempdir: {e}")


def split_s3_path(path: str) -> S3PathInfo:
    if ":" not in path:
        raise ValueError(f"Invalid S3 path: {path}")

    prts = path.split(":", 1)
    remote = prts[0]
    path = prts[1]
    parts: list[str] = []
    for part in path.split("/"):
        part = part.strip()
        if part:
            parts.append(part)
    if len(parts) < 2:
        raise ValueError(f"Invalid S3 path: {path}")
    bucket = parts[0]
    key = "/".join(parts[1:])
    assert bucket
    assert key
    return S3PathInfo(remote=remote, bucket=bucket, key=key)


def random_str(length: int) -> str:
    import random
    import string

    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
