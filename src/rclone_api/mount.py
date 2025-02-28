import platform
import subprocess
import time
import warnings
from pathlib import Path

_SYSTEM = platform.system()  # "Linux", "Darwin", "Windows", etc.


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


def clean_mount(mount_path: Path, verbose: bool = False) -> None:
    """
    Clean up a mount path across Linux, macOS, and Windows.

    The function attempts to unmount the mount at mount_path, then, if the
    directory is empty, removes it. On Linux it uses 'fusermount -u' (for FUSE mounts)
    and 'umount'. On macOS it uses 'umount' (and optionally 'diskutil unmount'),
    while on Windows it attempts to remove the mount point via 'mountvol /D'.
    """
    # Check if the mount path exists; if an OSError occurs, assume it exists.
    try:
        mount_exists = mount_path.exists()
    except OSError as e:
        warnings.warn(f"Error checking {mount_path}: {e}")
        mount_exists = True

    # Give the system a moment (if unmount is in progress, etc.)
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
