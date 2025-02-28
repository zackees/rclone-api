import os
import platform
import subprocess
import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rclone_api.process import Process

_SYSTEM = platform.system()  # "Linux", "Darwin", "Windows", etc.


@dataclass
class Mount:
    """Mount information."""

    mount_path: Path
    process: Process
    read_only: bool
    _closed: bool = False

    def __post_init__(self):
        assert isinstance(self.mount_path, Path)
        assert self.process is not None
        wait_for_mount(self.mount_path, self.process)

    def close(self, wait=True) -> None:
        """Clean up the mount."""
        if self._closed:
            return
        self._closed = True
        clean_mount(self, verbose=False)

    def __del__(self):
        self.close(wait=False)


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


def wait_for_mount(path: Path, mount_process: Any, timeout: int = 10) -> None:
    from rclone_api.process import Process

    assert isinstance(mount_process, Process)
    expire_time = time.time() + timeout
    while time.time() < expire_time:
        rtn = mount_process.poll()
        if rtn is not None:
            cmd_str = subprocess.list2cmdline(mount_process.cmd)
            raise subprocess.CalledProcessError(rtn, cmd_str)
        if path.exists():
            # how many files?
            dircontents = os.listdir(str(path))
            if len(dircontents) > 0:
                print(f"Mount point {path}, waiting 5 seconds for files to appear.")
                time.sleep(5)
                return
        time.sleep(1)


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
    except OSError as e:
        warnings.warn(f"Error checking {mount_path}: {e}")
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
        except Exception as e:
            warnings.warn(f"Failed to remove mount {mount_path}: {e}")
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
