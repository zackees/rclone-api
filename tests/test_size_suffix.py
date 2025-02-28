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
        size_suffix = SizeSuffix("16.5M")
        size_int = size_suffix.as_int()
        self.assertEqual(size_int, int(16.5 * 1024 * 1024))
        # now assert that the string value is the same as the input
        out_str = str(size_suffix)
        self.assertEqual(out_str, "16.5M")

    def test_float_suffix_border(self) -> None:
        size_suffix = SizeSuffix("1M")
        size_int = size_suffix.as_int()
        size_int -= 1
        # now assert that the string value is the same as the input
        tmp = SizeSuffix(size_int)
        out_str = tmp.as_str()
        self.assertEqual(out_str, "1M")


if __name__ == "__main__":
    unittest.main()
