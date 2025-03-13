"""
Unit test file.
"""

import os
import platform
import unittest
from pathlib import Path

from rclone_api.install import rclone_download


class RcloneInstallTester(unittest.TestCase):
    """Test rclone functionality."""

    def test_list_remotes(self) -> None:
        rclone_exe = "rclone"
        if platform.system() == "Windows":
            rclone_exe += ".exe"
        rclone_download(out=Path(rclone_exe), replace=True)
        self.assertTrue(os.path.exists(rclone_exe))
        cmd = f"./{rclone_exe}" if platform.system() != "Windows" else f"{rclone_exe}"
        rtn = os.system(f"{cmd} --version")
        os.remove(f"{rclone_exe}")
        self.assertEqual(rtn, 0)


if __name__ == "__main__":
    unittest.main()
