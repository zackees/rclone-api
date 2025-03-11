import atexit
import os
import random
import shutil
import signal
import subprocess
import warnings
from pathlib import Path
from threading import Lock
from typing import Any

from rclone_api.config import Config
from rclone_api.dir import Dir
from rclone_api.remote import Remote
from rclone_api.rpath import RPath
from rclone_api.types import S3PathInfo

# from .rclone import Rclone

_PRINT_LOCK = Lock()

_TMP_CONFIG_DIR = Path(".") / ".rclone" / "tmp_config"
_RCLONE_CONFIGS_LIST: list[Path] = []
_DO_CLEANUP = os.getenv("RCLONE_API_CLEANUP", "1") == "1"


def _clean_configs(signum=None, frame=None) -> None:
    if not _DO_CLEANUP:
        return
    for config in _RCLONE_CONFIGS_LIST:
        try:
            config.unlink()
        except Exception as e:
            print(f"Error deleting config file: {config}, {e}")
    _RCLONE_CONFIGS_LIST.clear()
    if signum is not None:
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)


def _init_cleanup() -> None:
    atexit.register(_clean_configs)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _clean_configs)


_init_cleanup()


def _make_temp_config_file() -> Path:
    from rclone_api.util import random_str

    tmpdir = _TMP_CONFIG_DIR / random_str(32)
    tmpdir.mkdir(parents=True, exist_ok=True)
    tmpfile = tmpdir / "rclone.conf"
    _RCLONE_CONFIGS_LIST.append(tmpfile)
    return tmpfile


def locked_print(*args, **kwargs):
    with _PRINT_LOCK:
        print(*args, **kwargs)


def port_is_free(port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) != 0


def find_free_port() -> int:
    tries = 20
    port = random.randint(10000, 20000)
    while tries > 0:
        if port_is_free(port):
            return port
        tries -= 1
        port = random.randint(10000, 20000)
    warnings.warn(f"Failed to find a free port, so using {port}")
    return port


def to_path(item: Dir | Remote | str, rclone: Any) -> RPath:
    from rclone_api.rclone_impl import RcloneImpl

    assert isinstance(rclone, RcloneImpl)
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
    capture: bool | Path | None = None,
    verbose: bool | None = None,
) -> subprocess.CompletedProcess:
    tmpfile: Path | None = None
    verbose = get_verbose(verbose)

    # Handle the Path case for capture
    output_file = None
    if isinstance(capture, Path):
        output_file = capture
        capture = False  # Don't capture to memory when redirecting to file
    else:
        capture = capture if isinstance(capture, bool) else True

    assert verbose is not None

    try:
        if isinstance(rclone_conf, Config):
            tmpfile = _make_temp_config_file()
            tmpfile.write_text(rclone_conf.text, encoding="utf-8")
            rclone_conf = tmpfile
        cmd = (
            [str(rclone_exe.resolve())] + ["--config", str(rclone_conf.resolve())] + cmd
        )
        if verbose:
            cmd_str = subprocess.list2cmdline(cmd)
            print(f"\nRunning: {cmd_str}")

        # If output_file is set, redirect output to that file
        if output_file:
            with open(output_file, "w", encoding="utf-8") as f:
                cp = subprocess.run(
                    cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    encoding="utf-8",
                    check=False,
                    shell=False,
                )
        else:
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
        if tmpfile and _DO_CLEANUP:
            try:
                tmpfile.unlink()
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
