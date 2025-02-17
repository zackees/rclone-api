"""
Unit test file.
"""

import unittest

from rclone_api.group_files import group_files


class GroupFilestest(unittest.TestCase):
    """Test rclone functionality."""

    def test_simple_group_files(self) -> None:
        files = [
            "Bucket/subdir/file1.txt",
            "Bucket/subdir/file2.txt",
        ]
        groups: dict[str, list[str]] = group_files(files)
        self.assertEqual(len(groups), 1)
        # Bucket/subdir should be the key
        self.assertIn("Bucket/subdir", groups)
        self.assertEqual(len(groups["Bucket/subdir"]), 2)
        expected_files = [
            "file1.txt",
            "file2.txt",
        ]
        self.assertIn(expected_files[0], groups["Bucket/subdir"])
        self.assertIn(expected_files[1], groups["Bucket/subdir"])
        print("done")

    def test_different_paths(self) -> None:
        files = [
            "Bucket/subdir/file1.txt",
            "Bucket/subdir2/file2.txt",
        ]
        groups: dict[str, list[str]] = group_files(files)
        self.assertEqual(len(groups), 2)
        # Bucket/subdir should be the key
        self.assertIn("Bucket/subdir", groups)
        self.assertEqual(len(groups["Bucket/subdir"]), 1)
        expected_files = [
            "file1.txt",
        ]
        self.assertIn(expected_files[0], groups["Bucket/subdir"])
        # Bucket/subdir2 should be the key
        self.assertIn("Bucket/subdir2", groups)
        self.assertEqual(len(groups["Bucket/subdir2"]), 1)

    def test_two_big_directories(self) -> None:
        files = [
            "Bucket/subdir/file1.txt",
            "Bucket/subdir/file2.txt",
            "Bucket/subdir2/file3.txt",
            "Bucket/subdir2/file4.txt",
        ]

        groups: dict[str, list[str]] = group_files(files)
        self.assertEqual(len(groups), 2)
        # Bucket/subdir should be the key
        self.assertIn("Bucket/subdir", groups)
        self.assertEqual(len(groups["Bucket/subdir"]), 2)
        expected_files = [
            "file1.txt",
            "file2.txt",
        ]
        self.assertIn(expected_files[0], groups["Bucket/subdir"])
        self.assertIn(expected_files[1], groups["Bucket/subdir"])
        # Bucket/subdir2 should be the key
        self.assertIn("Bucket/subdir2", groups)
        self.assertEqual(len(groups["Bucket/subdir2"]), 2)
        expected_files = [
            "file3.txt",
            "file4.txt",
        ]
        self.assertIn(expected_files[0], groups["Bucket/subdir2"])
        self.assertIn(expected_files[1], groups["Bucket/subdir2"])
        print("done")

    def test_two_fine_grained(self) -> None:
        files = [
            "TorrentBooks/libgenrs_nonfiction/204000/a2b20b2c89240ce81dec16091e18113e",
            "TorrentBooks/libgenrs_nonfiction/208000/155fe185bc03048b003a8e145ed097c8",
            "TorrentBooks/libgenrs_nonfiction/208001/155fe185bc03048b003a8e145ed097c8",
            "TorrentBooks/libgenrs_nonfiction/208002/155fe185bc03048b003a8e145ed097c8",
            "TorrentBooks/libgenrs_nonfiction/2080054/155fe185bc03048b003a8e145ed097c4",
        ]
        # expect that this all goes under the same parent
        groups: dict[str, list[str]] = group_files(files)
        self.assertEqual(len(groups), 1)
        # Bucket/subdir should be the key
        self.assertIn("TorrentBooks/libgenrs_nonfiction", groups)
        self.assertEqual(len(groups["TorrentBooks/libgenrs_nonfiction"]), 5)
        expected_files = [
            "204000/a2b20b2c89240ce81dec16091e18113e",
            "208000/155fe185bc03048b003a8e145ed097c8",
        ]
        self.assertIn(expected_files[0], groups["TorrentBooks/libgenrs_nonfiction"])


if __name__ == "__main__":
    unittest.main()
