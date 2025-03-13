import platform
from pathlib import Path
from warnings import warn

from download import download

URL_WINDOWS = "https://downloads.rclone.org/rclone-current-windows-amd64.zip"
URL_LINUX = "https://downloads.rclone.org/rclone-current-linux-amd64.zip"
URL_MACOS = "https://downloads.rclone.org/rclone-current-osx-amd64.zip"


def rclone_download_url() -> str:
    system = platform.system()
    if system == "Windows":
        return URL_WINDOWS
    elif system == "Linux":
        return URL_LINUX
    elif system == "Darwin":
        return URL_MACOS
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
    out.chmod(0o755)


def rclone_download(out: Path) -> Exception | None:
    try:
        url = rclone_download_url()
        download(url, out)
        _remove_signed_binary_requirements(out)
        _make_executable(out)
        return None
    except Exception as e:
        import traceback

        stacktrace = traceback.format_exc()
        warn(f"Failed to download rclone: {e}\n{stacktrace}")
        return e
