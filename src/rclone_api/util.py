import atexit
import os
import platform
import random
import shutil
import signal
import subprocess
import warnings
import weakref
from pathlib import Path
from threading import Lock
from typing import Any

import psutil
from appdirs import user_cache_dir

from rclone_api.config import Config
from rclone_api.dir import Dir
from rclone_api.install import rclone_download
from rclone_api.remote import Remote
from rclone_api.rpath import RPath
from rclone_api.types import S3PathInfo

# from .rclone import Rclone

_PRINT_LOCK = Lock()

_TMP_CONFIG_DIR = Path(".") / ".rclone" / "tmp_config"
_RCLONE_CONFIGS_LIST: list[Path] = []
_DO_CLEANUP = os.getenv("RCLONE_API_CLEANUP", "1") == "1"
_CACHE_DIR = Path(user_cache_dir("rclone_api"))

_RCLONE_EXE = _CACHE_DIR / "rclone"
if platform.system() == "Windows":
    _RCLONE_EXE = _RCLONE_EXE.with_suffix(".exe")


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


def make_temp_config_file() -> Path:
    from rclone_api.util import random_str

    tmpdir = _TMP_CONFIG_DIR / random_str(32)
    tmpdir.mkdir(parents=True, exist_ok=True)
    tmpfile = tmpdir / "rclone.conf"
    _RCLONE_CONFIGS_LIST.append(tmpfile)
    return tmpfile


def clear_temp_config_file(path: Path | None) -> None:
    if (path is None) or (not path.exists()) or (not _DO_CLEANUP):
        return
    try:
        path.unlink()
    except Exception as e:
        print(f"Error deleting config file: {path}, {e}")


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
        if rclone_which_path is not None:
            return Path(rclone_which_path)
        rclone_download(out=_RCLONE_EXE, replace=False)
        return _RCLONE_EXE
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

    # Handle the Path case for capture.
    output_file: Path | None = None
    if isinstance(capture, Path):
        output_file = capture
        capture = False  # When redirecting to file, don't capture to memory.
    else:
        capture = capture if isinstance(capture, bool) else True

    try:
        # Create a temporary config file if needed.
        if isinstance(rclone_conf, Config):
            tmpfile = make_temp_config_file()
            tmpfile.write_text(rclone_conf.text, encoding="utf-8")
            rclone_conf = tmpfile

        # Build the command line.
        full_cmd = (
            [str(rclone_exe.resolve())] + ["--config", str(rclone_conf.resolve())] + cmd
        )
        if verbose:
            cmd_str = subprocess.list2cmdline(full_cmd)
            print(f"\nRunning: {cmd_str}")

        # Prepare subprocess parameters.
        proc_kwargs: dict[str, Any] = {
            "encoding": "utf-8",
            "shell": False,
            "stderr": subprocess.PIPE,
        }
        file_handle = None
        if output_file:
            # Open the file for writing.
            file_handle = open(output_file, "w", encoding="utf-8")
            proc_kwargs["stdout"] = file_handle
        else:
            proc_kwargs["stdout"] = subprocess.PIPE if capture else None

        # Start the process.
        process = subprocess.Popen(full_cmd, **proc_kwargs)

        # Register an atexit callback that uses psutil to kill the process tree.
        proc_ref = weakref.ref(process)

        def cleanup():
            proc = proc_ref()
            if proc is None:
                return
            try:
                parent = psutil.Process(proc.pid)
            except psutil.NoSuchProcess:
                return
            # Terminate all child processes first.
            children = parent.children(recursive=True)
            if children:
                print(f"Terminating {len(children)} child process(es)...")
                for child in children:
                    try:
                        child.terminate()
                    except Exception as e:
                        print(f"Error terminating child {child.pid}: {e}")
                psutil.wait_procs(children, timeout=2)
                for child in children:
                    if child.is_running():
                        try:
                            child.kill()
                        except Exception as e:
                            print(f"Error killing child {child.pid}: {e}")
            # Now terminate the parent process.
            if parent.is_running():
                try:
                    parent.terminate()
                    parent.wait(timeout=3)
                except (psutil.TimeoutExpired, Exception):
                    try:
                        parent.kill()
                    except Exception as e:
                        print(f"Error killing process {parent.pid}: {e}")

        atexit.register(cleanup)

        # Wait for the process to complete.
        out, err = process.communicate()
        # Close the file handle if used.
        if file_handle:
            file_handle.close()

        cp: subprocess.CompletedProcess = subprocess.CompletedProcess(
            args=full_cmd,
            returncode=process.returncode,
            stdout=out,
            stderr=err,
        )

        # Warn or raise if return code is non-zero.
        if cp.returncode != 0:
            cmd_str = subprocess.list2cmdline(full_cmd)
            warnings.warn(
                f"Error running: {cmd_str}, returncode: {cp.returncode}\n"
                f"{cp.stdout}\n{cp.stderr}"
            )
            if check:
                raise subprocess.CalledProcessError(
                    cp.returncode, full_cmd, cp.stdout, cp.stderr
                )
        return cp
    finally:
        clear_temp_config_file(tmpfile)


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
