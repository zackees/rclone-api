"""
Unit test file.
"""

import platform
from pathlib import Path

from rclone_api.install import rclone_download


def main() -> None:
    rclone_exe = "rclone"
    if platform.system() == "Windows":
        rclone_exe += ".exe"
    rclone_download(out=Path(rclone_exe), replace=True)


if __name__ == "__main__":
    main()
