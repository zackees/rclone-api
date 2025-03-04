"""
UUnit test file for the DB class.
"""

import unittest
from pathlib import Path

from rclone_api.db import DB, DBFile

HERE = Path(__file__).parent
DB_PATH = HERE / "test.db"


class RcloneDBTests(unittest.TestCase):
    """Test DB functionality."""

    def setUp(self) -> None:
        """Set up the test."""
        self.db = DB(str(DB_PATH))

    def tearDown(self) -> None:
        """Clean up after the test."""
        # Remove the database file
        self.db.close()
        if DB_PATH.exists():
            DB_PATH.unlink()

    def test_db_creation(self) -> None:
        """Test database creation."""
        self.assertTrue(DB_PATH.exists())

    def test_table_section(self) -> None:
        """Test table section functionality."""
        # Create a table section
        ts = self.db.get_table_section(
            "dst:TorrentBooks", "file_entries_dst_torrentbooks"
        )

        new_files = [
            DBFile(
                parent="",
                name="book1.pdf",
                size=2048,
                mime_type="application/pdf",
                mod_time="2025-03-03T12:00:00",
            ),
            DBFile(
                parent="",
                name="book2.epub",
                size=1024,
                mime_type="application/epub+zip",
                mod_time="2025-03-03T12:05:00",
            ),
        ]

        ts.insert_files(new_files)

        # Query the data
        out_file_entries: list[DBFile] = ts.get_files()

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
