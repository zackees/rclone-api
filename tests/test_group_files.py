"""
Unit test file.
"""

import unittest

from rclone_api.group_files import group_files


class GroupFilestest(unittest.TestCase):
    """Test rclone functionality."""

    def test_simple_group_files(self) -> None:
        files = [
            "dst:Bucket/subdir/file1.txt",
            "dst:Bucket/subdir/file2.txt",
        ]
        groups: dict[str, list[str]] = group_files(files)
        self.assertEqual(len(groups), 1)
        # dst:/Bucket/subdir should be the key
        self.assertIn("dst:Bucket/subdir", groups)
        self.assertEqual(len(groups["dst:Bucket/subdir"]), 2)
        expected_files = [
            "file1.txt",
            "file2.txt",
        ]
        self.assertIn(expected_files[0], groups["dst:Bucket/subdir"])
        self.assertIn(expected_files[1], groups["dst:Bucket/subdir"])
        print("done")

    def test_different_paths(self) -> None:
        files = [
            "dst:Bucket/subdir/file1.txt",
            "dst:Bucket/subdir2/file2.txt",
        ]
        groups: dict[str, list[str]] = group_files(files)
        self.assertEqual(len(groups), 2)
        # dst:/Bucket/subdir should be the key
        self.assertIn("dst:Bucket/subdir", groups)
        self.assertEqual(len(groups["dst:Bucket/subdir"]), 1)
        expected_files = [
            "file1.txt",
        ]
        self.assertIn(expected_files[0], groups["dst:Bucket/subdir"])
        # dst:/Bucket/subdir2 should be the key
        self.assertIn("dst:Bucket/subdir2", groups)
        self.assertEqual(len(groups["dst:Bucket/subdir2"]), 1)

    def test_two_big_directories(self) -> None:
        files = [
            "dst:Bucket/subdir/file1.txt",
            "dst:Bucket/subdir/file2.txt",
            "dst:Bucket/subdir2/file3.txt",
            "dst:Bucket/subdir2/file4.txt",
        ]

        groups: dict[str, list[str]] = group_files(files)
        self.assertEqual(len(groups), 2)
        # dst:/Bucket/subdir should be the key
        self.assertIn("dst:Bucket/subdir", groups)
        self.assertEqual(len(groups["dst:Bucket/subdir"]), 2)
        expected_files = [
            "file1.txt",
            "file2.txt",
        ]
        self.assertIn(expected_files[0], groups["dst:Bucket/subdir"])
        self.assertIn(expected_files[1], groups["dst:Bucket/subdir"])
        # dst:/Bucket/subdir2 should be the key
        self.assertIn("dst:Bucket/subdir2", groups)
        self.assertEqual(len(groups["dst:Bucket/subdir2"]), 2)
        expected_files = [
            "file3.txt",
            "file4.txt",
        ]
        self.assertIn(expected_files[0], groups["dst:Bucket/subdir2"])
        self.assertIn(expected_files[1], groups["dst:Bucket/subdir2"])
        print("done")


if __name__ == "__main__":
    unittest.main()
