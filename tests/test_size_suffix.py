"""
Unit test file.
"""

import unittest

from rclone_api import SizeSuffix


class RcloneSuffixSize(unittest.TestCase):
    """Test rclone functionality."""

    def test_simple_suffix(self) -> None:
        size_suffix = SizeSuffix(1024)
        size_suffix = SizeSuffix("16MB")
        size_int = size_suffix.as_int()
        self.assertEqual(size_int, 16 * 1024 * 1024)

    def test_float_suffix(self) -> None:
        size_suffix = SizeSuffix("16.5MB")
        size_int = size_suffix.as_int()
        self.assertEqual(size_int, int(16.5 * 1024 * 1024))


if __name__ == "__main__":
    unittest.main()
