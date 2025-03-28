import logging
import os
import platform
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from warnings import warn

from download import download

URL_WINDOWS = "https://downloads.rclone.org/rclone-current-windows-amd64.zip"
URL_LINUX = "https://downloads.rclone.org/rclone-current-linux-amd64.zip"
URL_MACOS_ARM = "https://downloads.rclone.org/rclone-current-osx-arm64.zip"
URL_MACOS_X86 = "https://downloads.rclone.org/rclone-current-osx-amd64.zip"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def rclone_download_url() -> str:
    system = platform.system()
    arch = platform.machine()
    if system == "Windows":
        assert "arm" not in arch, f"Unsupported arch: {arch}"
        return URL_WINDOWS
    elif system == "Linux":
        assert "arm" not in arch, f"Unsupported arch: {arch}"
        return URL_LINUX
    elif system == "Darwin":
        if "x86" in arch:
            return URL_MACOS_X86
        elif "arm" in arch:
            return URL_MACOS_ARM
        else:
            raise Exception(f"Unsupported arch: {arch}")
    else:
        raise Exception("Unsupported system")


def _remove_signed_binary_requirements(out: Path) -> None:
    if platform.system() == "Windows":
        return
    # mac os
    if platform.system() == "Darwin":
        # remove signed binary requirements
        #
        # xattr -d com.apple.quarantine rclone
        import subprocess

        subprocess.run(
            ["xattr", "-d", "com.apple.quarantine", str(out)],
            capture_output=True,
            check=False,
        )
        return


def _make_executable(out: Path) -> None:
    if platform.system() == "Windows":
        return
    # linux and mac os
    os.chmod(out, 0o755)


def _find_rclone_exe(start: Path) -> Path | None:
    for root, dirs, files in os.walk(start):
        if platform.system() == "Windows":
            if "rclone.exe" in files:
                return Path(root) / "rclone.exe"
        else:
            if "rclone" in files:
                return Path(root) / "rclone"
    return None


def _move_to_standard_linux_paths(exe_path: Path) -> None:
    """Move rclone to standard paths on Linux systems and create a symlink in the original location."""
    if platform.system() != "Linux":
        return

    try:
        # Try system-wide installation first
        system_bin_path = Path("/usr/local/bin/rclone")

        if os.access("/usr/local/bin", os.W_OK):
            # Remove existing binary if it exists
            if system_bin_path.exists():
                if system_bin_path.is_symlink():
                    system_bin_path.unlink()
                else:
                    os.remove(system_bin_path)

            # Move the binary to standard path
            shutil.move(str(exe_path), str(system_bin_path))

            # Make it executable
            os.chmod(system_bin_path, 0o755)

            # Create a symlink in the original location
            exe_path.symlink_to(system_bin_path)

            logger.info(
                f"Moved rclone to {system_bin_path} and created symlink at {exe_path}"
            )
        else:
            # Fall back to user's home directory if no root access
            home_bin_dir = Path.home() / ".local" / "bin"
            home_bin_dir.mkdir(parents=True, exist_ok=True)
            home_bin_path = home_bin_dir / "rclone"

            # Remove existing binary if it exists
            if home_bin_path.exists():
                if home_bin_path.is_symlink():
                    home_bin_path.unlink()
                else:
                    os.remove(home_bin_path)

            # Move the binary to user's bin directory
            shutil.move(str(exe_path), str(home_bin_path))

            # Make it executable
            os.chmod(home_bin_path, 0o755)

            # Create a symlink in the original location
            exe_path.symlink_to(home_bin_path)

            logger.info(
                f"Moved rclone to {home_bin_path} and created symlink at {exe_path}"
            )

            # Check if ~/.local/bin is in PATH, if not suggest adding it
            if str(home_bin_dir) not in os.environ.get("PATH", ""):
                logger.warning(
                    f"{home_bin_dir} is not in PATH. Consider adding it to your shell profile."
                )
    except Exception as e:
        logger.warning(f"Failed to move rclone to standard paths: {e}")


def rclone_download(out: Path, replace=False) -> Exception | None:
    if out.exists() and not replace:
        return None
    try:
        url = rclone_download_url()
        with TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            logging.info(f"Downloading rclone from {url} to {tmp.absolute()}")
            download(url, tmp, kind="zip", replace=True)
            exe = _find_rclone_exe(tmp)
            if exe is None:
                raise FileNotFoundError("rclone executable not found")
            if os.path.exists(out):
                os.remove(out)
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(exe, out)
        _remove_signed_binary_requirements(out)
        _make_executable(out)

        # Move to standard paths on Linux
        _move_to_standard_linux_paths(out)

        return None
    except Exception as e:
        import traceback

        stacktrace = traceback.format_exc()
        warn(f"Failed to download rclone: {e}\n{stacktrace}")
        return e
