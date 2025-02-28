import os
import time
import warnings
from pathlib import Path

_IS_WINDOWS = os.name == "nt"


def clean_mount(mount_path: Path, verbose: bool) -> None:
    """Clean up a mount path."""
    mount_path_exists: bool = False
    try:
        mount_path_exists = mount_path.exists()
    except OSError as e:
        warnings.warn(f"Error in scoped_mount: {e}")
        mount_path_exists = True
    time.sleep(2)
    if mount_path_exists:
        print(f"{mount_path} mount still exists, attempting to remove")
        if not _IS_WINDOWS:

            def exec(cmd: str) -> int:
                if verbose:
                    print(f"Executing: {cmd}")
                rtn = os.system(cmd)
                if rtn != 0 and verbose:
                    print(f"Failed to execute: {cmd}")
                return rtn

            exec(f"fusermount -u {mount_path}")
            exec(f"umount {mount_path}")
            time.sleep(2)

            try:
                mount_path_exists = mount_path.exists()
            except OSError as e:
                warnings.warn(f"Error in scoped_mount: {e}")
                mount_path_exists = True

            if mount_path_exists:
                is_empty = True
                try:
                    is_empty = not list(mount_path.iterdir())
                    if not is_empty:
                        warnings.warn(f"Failed to unmount {mount_path}")
                    else:
                        try:
                            mount_path.rmdir()
                        except Exception as e:
                            warnings.warn(f"Failed to remove {mount_path}: {e}")
                except Exception as e:
                    warnings.warn(
                        f"Failed during mount cleanup of {mount_path}: because {e}"
                    )
