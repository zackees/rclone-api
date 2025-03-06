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

    def test_weird_suffix(self) -> None:
        name = r"%28sici%291096-911x%28199809%2931%3A3%3C170%3A%3Aaid-mpo8%3E3.0.co%3B2-8.pdf"
        file_item: FileItem = FileItem(
            remote="remote",
            parent="parent",
            name=name,
            size=1,
            mime_type="mime_type",
            mod_time="mod_time",
        )
        self.assertEqual(file_item.real_suffix, "pdf")

    def test_weird_suffix2(self) -> None:
        name = "acb86a1f632adb2be7cac60d76c3c85b.cbz"
        file_item = FileItem(
            remote="remote",
            parent="parent",
            name=name,
            size=1,
            mime_type="mime_type",
            mod_time="mod_time",
        )
        self.assertEqual(file_item.real_suffix, "cbz")


if __name__ == "__main__":
    unittest.main()
