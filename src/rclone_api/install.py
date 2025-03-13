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


def rclone_download_url() -> str:
    system = platform.system()
    if system == "Windows":
        return URL_WINDOWS
    elif system == "Linux":
        return URL_LINUX
    elif system == "Darwin":
        arch = platform.machine()
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


def rclone_download(out: Path, replace=False) -> Exception | None:
    try:
        url = rclone_download_url()
        with TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            download(url, tmp, kind="zip", replace=replace)
            exe = _find_rclone_exe(tmp)
            if exe is None:
                raise FileNotFoundError("rclone executable not found")
            if os.path.exists(out):
                os.remove(out)
            shutil.move(exe, out)
        _remove_signed_binary_requirements(out)
        _make_executable(out)
        return None
    except Exception as e:
        import traceback

        stacktrace = traceback.format_exc()
        warn(f"Failed to download rclone: {e}\n{stacktrace}")
        return e
