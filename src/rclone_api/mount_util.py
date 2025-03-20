import atexit
import os
import platform
import shutil
import subprocess
import time
import warnings
import weakref
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from rclone_api.mount import Mount
from rclone_api.process import Process

_SYSTEM = platform.system()  # "Linux", "Darwin", "Windows", etc.


_MOUNTS_FOR_GC: weakref.WeakSet = weakref.WeakSet()


def _cleanup_mounts() -> None:
    with ThreadPoolExecutor() as executor:
        mount: Mount
        for mount in _MOUNTS_FOR_GC:
            executor.submit(mount.close)


def _run_command(cmd: str, verbose: bool) -> int:
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


atexit.register(_cleanup_mounts)


def cache_dir_delete_on_exit(cache_dir: Path) -> None:
    if cache_dir.exists():
        try:
            shutil.rmtree(cache_dir, ignore_errors=True)
        except Exception as e:
            warnings.warn(f"Error removing cache directory {cache_dir}: {e}")


def add_mount_for_gc(mount: Mount) -> None:
    # weak reference to avoid circular references
    _MOUNTS_FOR_GC.add(mount)


def remove_mount_for_gc(mount: Mount) -> None:
    _MOUNTS_FOR_GC.discard(mount)


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
    mount: Mount,
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
        src (Path): The mount point directory to check.
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

    mount_process = mount.process
    src = mount.mount_path

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
        if src.exists():
            # Optionally check if path is a mount point.
            if check_mount_flag:
                try:
                    if not os.path.ismount(str(src)):
                        print(
                            f"{src} exists but is not recognized as a mount point yet."
                        )
                        time.sleep(poll_interval)
                        continue
                except Exception as e:
                    print(f"Could not verify mount point status for {src}: {e}")

            try:
                # Check for at least one entry in the directory.
                if any(src.iterdir()):
                    print(
                        f"Mount point {src} appears available with files. Waiting {post_mount_delay} seconds for stabilization."
                    )
                    time.sleep(post_mount_delay)
                    return
                else:
                    print(f"Mount point {src} is empty. Waiting for files to appear.")
            except Exception as e:
                last_error = e
                print(f"Error accessing {src}: {e}")
        else:
            print(f"Mount point {src} does not exist yet.")

        time.sleep(poll_interval)

    # raise TimeoutError(
    #     f"Mount point {src} did not become available within {timeout} seconds. Last error: {last_error}"
    # )
    if last_error is not None:
        raise last_error


def _rmtree_ignore_mounts(path):
    """
    Recursively remove a directory tree while ignoring mount points.

    Directories that are mount points (where os.path.ismount returns True)
    are skipped.
    """
    # Iterate over directory entries without following symlinks
    with os.scandir(path) as it:
        for entry in it:
            full_path = entry.path
            if entry.is_dir(follow_symlinks=False):
                # If it's a mount point, skip recursing into it
                if os.path.ismount(full_path):
                    print(f"Skipping mount point: {full_path}")
                    continue
                # Recursively remove subdirectories
                _rmtree_ignore_mounts(full_path)
            else:
                # Remove files or symlinks
                os.unlink(full_path)
    # Remove the now-empty directory
    os.rmdir(path)


# Example usage:
# rmtree_ignore_mounts("/path/to/directory")


def clean_mount(mount: Mount | Path, verbose: bool = False, wait=True) -> None:
    """
    Clean up a mount path across Linux, macOS, and Windows.

    The function attempts to unmount the mount at mount_path, then, if the
    directory is empty, removes it. On Linux it uses 'fusermount -u' (for FUSE mounts)
    and 'umount'. On macOS it uses 'umount' (and optionally 'diskutil unmount'),
    while on Windows it attempts to remove the mount point via 'mountvol /D'.
    """

    def verbose_print(msg: str):
        if verbose:
            print(msg)

    proc = mount.process if isinstance(mount, Mount) else None
    if proc is not None and proc.poll() is None:
        verbose_print(f"Terminating mount process {proc.pid}")
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
        verbose_print(f"{mount_path} does not exist; nothing to clean up.")
        return

    verbose_print(f"{mount_path} still exists, attempting to unmount and remove.")

    # Platform-specific unmount procedures
    if _SYSTEM == "Linux":
        # Try FUSE unmount first (if applicable), then the regular umount.
        _run_command(f"fusermount -u {mount_path}", verbose)
        _run_command(f"umount {mount_path}", verbose)
    elif _SYSTEM == "Darwin":
        # On macOS, use umount; optionally try diskutil for stubborn mounts.
        _run_command(f"umount {mount_path}", verbose)
        # Optionally: uncomment the next line if diskutil unmount is preferred.
        # _run_command(f"diskutil unmount {mount_path}", verbose)
    elif _SYSTEM == "Windows":
        # On Windows, remove the mount point using mountvol.
        _run_command(f"mountvol {mount_path} /D", verbose)
        # If that does not work, try to remove the directory directly.
        try:
            _rmtree_ignore_mounts(mount_path)
            if mount_path.exists():
                raise OSError(f"Failed to remove mount directory {mount_path}")
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
        verbose_print(f"{mount_path} still exists after unmount attempt.")
        # Attempt to remove the directory if it is empty.

        # Only remove if the directory is empty.
        if not any(mount_path.iterdir()):
            try:
                mount_path.rmdir()
            except Exception as e:
                warnings.warn(f"Error removing mount {mount_path}: {e}")
                raise
            if verbose:
                verbose_print(f"Removed empty mount directory {mount_path}")
        else:
            warnings.warn(f"{mount_path} is not empty; cannot remove.")
            raise OSError(f"{mount_path} is not empty")

    else:
        verbose_print(f"{mount_path} successfully cleaned up.")
