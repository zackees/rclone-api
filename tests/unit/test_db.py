"""
UUnit test file for the DB class.
"""

import os
import unittest
from pathlib import Path

from rclone_api import FileItem as DBFile
from rclone_api.db import DB

HERE = Path(__file__).parent
DB_PATH = HERE / "test.db"

os.environ["DB_PATH"] = str(DB_PATH)


class RcloneDBTests(unittest.TestCase):
    """Test DB functionality."""

    def setUp(self) -> None:
        """Set up the test."""
        sql_url = "sqlite:///" + str(DB_PATH)
        self.db = DB(sql_url)
        # self.db = DB()

    def tearDown(self) -> None:
        """Clean up after the test."""
        # Remove the database file
        self.db.close()
        if DB_PATH.exists():
            DB_PATH.unlink()

    def test_db_creation(self) -> None:
        """Test database creation."""
        self.assertTrue(DB_PATH.exists())

    def test_table(self) -> None:
        """Test table section functionality."""
        # Create a table section
        repo = self.db.get_or_create_repo("dst:TorrentBooks")

        new_files = [
            DBFile(
                remote="dst:TorrentBooks",  # ignored in db
                parent="",
                name="book1.pdf",
                size=2048,
                mime_type="application/pdf",
                mod_time="2025-03-03T12:00:00",
            ),
            DBFile(
                remote="dst:TorrentBooks",  # ignored in db
                parent="",
                name="book2.epub",
                size=1024,
                mime_type="application/epub+zip",
                mod_time="2025-03-03T12:05:00",
            ),
        ]

        repo.insert_files(new_files)
        # what happens when we do it again?
        repo.insert_files(new_files)

        # Query the data
        out_file_entries: list[DBFile] = repo.get_all_files()

        # Assert that two file entries exist
        self.assertEqual(
            len(out_file_entries),
            2,
            f"Expected 2 file entries, found {len(out_file_entries)}",
        )

        for entry in out_file_entries:
            print(entry)
            self.assertIn(entry, new_files, f"Unexpected entry: {entry}")


#
if __name__ == "__main__":
    unittest.main()
