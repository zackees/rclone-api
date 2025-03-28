"""
UUnit test file for the DB class.
"""

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from rclone_api.fs.filesystem import FSPath, RealFS

HERE = Path(__file__).parent
DB_PATH = HERE / "test.db"

os.environ["DB_PATH"] = str(DB_PATH)


class RcloneFSTester(unittest.TestCase):
    """Test DB functionality."""

    def test_os_walk(self) -> None:
        """Test table section functionality."""
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir)

            # create sub directories
            (path / "sub1").mkdir()
            (path / "sub2").mkdir()

            # create files
            (path / "file1.txt").touch()
            (path / "file2.txt").touch()

            # create sub files in sub1
            (path / "sub1" / "subfile1.txt").touch()

            cwd = RealFS.from_path(path)

            all_dirs: list[FSPath] = []
            all_files: list[FSPath] = []

            for current_dir, dir_paths, file_paths in cwd.walk():
                for dir_path in dir_paths:
                    full_path = current_dir / dir_path
                    all_dirs.append(full_path)
                for file_path in file_paths:
                    full_path = current_dir / file_path
                    all_files.append(full_path)

            self.assertEqual(all_dirs[0].relative_to(cwd).path, "sub1")
            self.assertEqual(all_dirs[1].relative_to(cwd).path, "sub2")
            self.assertEqual(all_files[0].relative_to(cwd).path, "file1.txt")
            self.assertEqual(all_files[1].relative_to(cwd).path, "file2.txt")
            self.assertEqual(all_files[2].relative_to(cwd).path, "sub1/subfile1.txt")


#
if __name__ == "__main__":
    unittest.main()
