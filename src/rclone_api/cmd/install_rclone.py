"""
Unit test file.
"""

import logging
import platform
from pathlib import Path

from rclone_api.install import rclone_download


def main() -> None:
    # set the root logger level to DEBUG
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    rclone_exe = "rclone"
    if platform.system() == "Windows":
        rclone_exe += ".exe"
    rclone_download(out=Path(rclone_exe), replace=True)


if __name__ == "__main__":
    main()
