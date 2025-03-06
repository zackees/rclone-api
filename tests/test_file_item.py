"""
Unit test file.
"""

import unittest

from rclone_api import FileItem


class RcloneTestFileItem(unittest.TestCase):
    """Test rclone functionality."""

    def test_file_item_suffix(self) -> None:
        file_item: FileItem = FileItem(
            remote="remote",
            parent="parent",
            name="name.sql.gz",
            size=1,
            mime_type="mime_type",
            mod_time="mod_time",
        )
        self.assertEqual(file_item.real_suffix, "sql")

    def test_file_item_suffix_weird(self) -> None:
        file_item: FileItem = FileItem(
            remote="remote",
            parent="parent",
            name="name.sql.gz  -- annas archive",
            size=1,
            mime_type="mime_type",
            mod_time="mod_time",
        )
        self.assertEqual(file_item.real_suffix, "sql")


if __name__ == "__main__":
    unittest.main()
