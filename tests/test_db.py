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

        # Insert sample data
        # ts.insert_file(
        #     parent="",
        #     name="book1.pdf",
        #     size=2048,
        #     mime_type="application/pdf",
        #     mod_time="2025-03-03T12:00:00",
        # )
        # ts.insert_file(
        #     parent="",
        #     name="book2.epub",
        #     size=1024,
        #     mime_type="application/epub+zip",
        #     mod_time="2025-03-03T12:05:00",
        # )
        ts.insert_file(
            DBFile(
                parent="",
                name="book1.pdf",
                size=2048,
                mime_type="application/pdf",
                mod_time="2025-03-03T12:00:00",
            )
        )
        ts.insert_file(
            DBFile(
                parent="",
                name="book2.epub",
                size=1024,
                mime_type="application/epub+zip",
                mod_time="2025-03-03T12:05:00",
            )
        )

        # Query the data
        file_entries = ts.get_files()

        # Assert that two file entries exist
        self.assertEqual(
            len(file_entries), 2, f"Expected 2 file entries, found {len(file_entries)}"
        )

        # Map file names to their entries for easier verification
        entries = {entry.name: entry for entry in file_entries}  # type: ignore
        expected_entries = {
            "book1.pdf": {
                "size": 2048,
                "mime_type": "application/pdf",
                "mod_time": "2025-03-03T12:00:00",
                "parent": "",
            },
            "book2.epub": {
                "size": 1024,
                "mime_type": "application/epub+zip",
                "mod_time": "2025-03-03T12:05:00",
                "parent": "",
            },
        }

        for name, expected in expected_entries.items():
            self.assertIn(name, entries, f"Expected entry with name {name} not found.")
            entry = entries[name]
            for field, expected_value in expected.items():
                actual_value = getattr(entry, field)
                self.assertEqual(
                    actual_value,
                    expected_value,
                    f"Mismatch for {name} in field '{field}': expected {expected_value}, got {actual_value}",
                )


#
if __name__ == "__main__":
    unittest.main()
