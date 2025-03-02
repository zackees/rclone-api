import atexit
import os
import platform
import shutil
import subprocess
import time
import warnings
import weakref
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rclone_api.process import Process

_SYSTEM = platform.system()  # "Linux", "Darwin", "Windows", etc.

_MOUNTS_FOR_GC: weakref.WeakSet = weakref.WeakSet()


def _add_mount_for_gc(mount: "Mount") -> None:
    # weak reference to avoid circular references
    _MOUNTS_FOR_GC.add(mount)


def _remove_mount_for_gc(mount: "Mount") -> None:
    _MOUNTS_FOR_GC.discard(mount)


def _cleanup_mounts() -> None:
    with ThreadPoolExecutor() as executor:
        mount: Mount
        for mount in _MOUNTS_FOR_GC:
            executor.submit(mount.close)


def _cache_dir_delete_on_exit(cache_dir: Path) -> None:
    if cache_dir.exists():
        try:
            shutil.rmtree(cache_dir)
        except Exception as e:
            warnings.warn(f"Error removing cache directory {cache_dir}: {e}")


atexit.register(_cleanup_mounts)


@dataclass
class Mount:
    """Mount information."""

    src: str
    mount_path: Path
    process: Process
    read_only: bool
    cache_dir: Path | None = None
    cache_dir_delete_on_exit: bool | None = None
    _closed: bool = False

    def __post_init__(self):
        assert isinstance(self.mount_path, Path)
        assert self.process is not None
        wait_for_mount(self.mount_path, self.process)
        _add_mount_for_gc(self)

    def close(self, wait=True) -> None:
        """Clean up the mount."""
        if self._closed:
            return
        self._closed = True
        self.process.terminate()
        clean_mount(self, verbose=False, wait=wait)
        if self.cache_dir and self.cache_dir_delete_on_exit:
            _cache_dir_delete_on_exit(self.cache_dir)
        _remove_mount_for_gc(self)

    def __enter__(self) -> "Mount":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close(wait=True)

    def __del__(self):
        self.close(wait=False)

    # make this a hashable object
    def __hash__(self):
        return hash(self.mount_path)


def run_command(cmd: str, verbose: bool) -> int:
    """Run a shell command and print its output if verbose is True."""
    if verbose:
        print(f"Executing: {cmd}")
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, check=False
        )
        if result.returncode != 0 and verbose:
            print(f"Command failed: {cmd}\nStdErr: {result.stderr.strip()}")
        return result.returncode
    except Exception as e:
        warnings.warn(f"Error running command '{cmd}': {e}")
        return -1


def prepare_mount(outdir: Path, verbose: bool) -> None:
    if _SYSTEM == "Windows":
        # Windows -> Must create parent directories only if they don't exist
        if verbose:
            print(f"Creating parent directories for {outdir}")
        outdir.parent.mkdir(parents=True, exist_ok=True)
    else:
        # Linux -> Must create parent directories and the directory itself
        if verbose:
            print(f"Creating directories for {outdir}")
        outdir.mkdir(parents=True, exist_ok=True)


def wait_for_mount(
    path: Path,
    mount_process: Any,
    timeout: int = 20,
    post_mount_delay: int = 5,
    poll_interval: float = 1.0,
    check_mount_flag: bool = False,
) -> None:
    """
    Wait for a mount point to become available by checking if the directory exists,
    optionally verifying that it is a mount point, and confirming that it contains files.
    This function periodically polls for the mount status, ensures the mount process
    is still running, and applies an extra delay after detecting content for stabilization.

    Args:
        path (Path): The mount point directory to check.
        mount_process (Any): A Process instance handling the mount (must be an instance of Process).
        timeout (int): Maximum time in seconds to wait for the mount to become available.
        post_mount_delay (int): Additional seconds to wait after detecting files.
        poll_interval (float): Seconds between each poll iteration.
        check_mount_flag (bool): If True, verifies that the path is recognized as a mount point.

    Raises:
        subprocess.CalledProcessError: If the mount_process exits unexpectedly.
        TimeoutError: If the mount is not available within the timeout period.
        TypeError: If mount_process is not an instance of Process.
    """

    if not isinstance(mount_process, Process):
        raise TypeError("mount_process must be an instance of Process")

    expire_time = time.time() + timeout
    last_error = None

    while time.time() < expire_time:
        # Check if the mount process has terminated unexpectedly.
        rtn = mount_process.poll()
        if rtn is not None:
            cmd_str = subprocess.list2cmdline(mount_process.cmd)
            print(f"Mount process terminated unexpectedly: {cmd_str}")
            raise subprocess.CalledProcessError(rtn, cmd_str)

        # Check if the mount path exists.
        if path.exists():
            # Optionally check if path is a mount point.
            if check_mount_flag:
                try:
                    if not os.path.ismount(str(path)):
                        print(
                            f"{path} exists but is not recognized as a mount point yet."
                        )
                        time.sleep(poll_interval)
                        continue
                except Exception as e:
                    print(f"Could not verify mount point status for {path}: {e}")

            try:
                # Check for at least one entry in the directory.
                if any(path.iterdir()):
                    print(
                        f"Mount point {path} appears available with files. Waiting {post_mount_delay} seconds for stabilization."
                    )
                    time.sleep(post_mount_delay)
                    return
                else:
                    print(f"Mount point {path} is empty. Waiting for files to appear.")
            except Exception as e:
                last_error = e
                print(f"Error accessing {path}: {e}")
        else:
            print(f"Mount point {path} does not exist yet.")

        time.sleep(poll_interval)

    raise TimeoutError(
        f"Mount point {path} did not become available within {timeout} seconds. Last error: {last_error}"
    )


def clean_mount(mount: Mount | Path, verbose: bool = False, wait=True) -> None:
    """
    Clean up a mount path across Linux, macOS, and Windows.

    The function attempts to unmount the mount at mount_path, then, if the
    directory is empty, removes it. On Linux it uses 'fusermount -u' (for FUSE mounts)
    and 'umount'. On macOS it uses 'umount' (and optionally 'diskutil unmount'),
    while on Windows it attempts to remove the mount point via 'mountvol /D'.
    """
    proc = mount.process if isinstance(mount, Mount) else None
    if proc is not None and proc.poll() is None:
        if verbose:
            print(f"Terminating mount process {proc.pid}")
        proc.kill()

    # Check if the mount path exists; if an OSError occurs, assume it exists.
    mount_path = mount.mount_path if isinstance(mount, Mount) else mount
    try:
        mount_exists = mount_path.exists()
    except OSError:
        # warnings.warn(f"Error checking {mount_path}: {e}")
        mount_exists = True

    # Give the system a moment (if unmount is in progress, etc.)
    if wait:
        time.sleep(2)

    if not mount_exists:
        if verbose:
            print(f"{mount_path} does not exist; nothing to clean up.")
        return

    if verbose:
        print(f"{mount_path} still exists, attempting to unmount and remove.")

    # Platform-specific unmount procedures
    if _SYSTEM == "Linux":
        # Try FUSE unmount first (if applicable), then the regular umount.
        run_command(f"fusermount -u {mount_path}", verbose)
        run_command(f"umount {mount_path}", verbose)
    elif _SYSTEM == "Darwin":
        # On macOS, use umount; optionally try diskutil for stubborn mounts.
        run_command(f"umount {mount_path}", verbose)
        # Optionally: uncomment the next line if diskutil unmount is preferred.
        # run_command(f"diskutil unmount {mount_path}", verbose)
    elif _SYSTEM == "Windows":
        # On Windows, remove the mount point using mountvol.
        run_command(f"mountvol {mount_path} /D", verbose)
        # If that does not work, try to remove the directory directly.
        try:
            mount_path.rmdir()
            if verbose:
                print(f"Successfully removed mount directory {mount_path}")
        except Exception:
            # warnings.warn(f"Failed to remove mount {mount_path}: {e}")
            pass
    else:
        warnings.warn(f"Unsupported platform: {_SYSTEM}")

    # Allow some time for the unmount commands to take effect.
    if wait:
        time.sleep(2)

    # Re-check if the mount path still exists.
    try:
        still_exists = mount_path.exists()
    except OSError as e:
        warnings.warn(f"Error re-checking {mount_path}: {e}")
        still_exists = True

    if still_exists:
        if verbose:
            print(f"{mount_path} still exists after unmount attempt.")
        # Attempt to remove the directory if it is empty.
        try:
            # Only remove if the directory is empty.
            if not any(mount_path.iterdir()):
                mount_path.rmdir()
                if verbose:
                    print(f"Removed empty mount directory {mount_path}")
            else:
                warnings.warn(f"{mount_path} is not empty; cannot remove.")
        except Exception as e:
            warnings.warn(f"Failed during cleanup of {mount_path}: {e}")
    else:
        if verbose:
            print(f"{mount_path} successfully cleaned up.")
