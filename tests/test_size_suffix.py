"""
Unit test file.
"""

import unittest

from rclone_api import SizeSuffix


class RcloneSuffixSize(unittest.TestCase):
    """Test rclone functionality."""

    def test_list_remotes(self) -> None:
        size_suffix = SizeSuffix(1024)
        size_suffix = SizeSuffix("16MB")
        size_int = size_suffix.as_int()
        self.assertEqual(size_int, 16 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
