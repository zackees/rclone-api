import os
from typing import Optional

from sqlmodel import Field, Session, SQLModel, create_engine, select

# Remove the database file if it exists to start fresh on each run.
db_file = "database.db"
if os.path.exists(db_file):
    os.remove(db_file)

# Create the engine (using SQLite in this example)
engine = create_engine(f"sqlite:///{db_file}")


# Meta table that indexes all repositories
class RepositoryMeta(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    repo_name: str
    file_table_name: str  # The dedicated table name for file entries


# Factory to dynamically create a FileEntry model with a given table name
def create_file_entry_model(table_name: str):
    class FileEntry(SQLModel, table=True):
        __tablename__ = table_name  # type: ignore # dynamically set table name
        id: Optional[int] = Field(default=None, primary_key=True)
        parent: str
        name: str
        size: int
        mime_type: str
        mod_time: str

    return FileEntry


# Create the meta table (RepositoryMeta) ahead of time
SQLModel.metadata.create_all(engine)


class TableSection:
    def __init__(self, remote_name: str, table_name: str | None = None):
        self.remote_name = remote_name
        # If table_name is not provided, derive one from the remote name.
        if table_name is None:
            table_name = (
                "file_entries_"
                + remote_name.replace(":", "_").replace(" ", "_").lower()
            )
        self.table_name = table_name

        # Check if repository exists in RepositoryMeta; if not, create a new entry.
        with Session(engine) as session:
            existing_repo = session.exec(
                select(RepositoryMeta).where(
                    RepositoryMeta.repo_name == self.remote_name
                )
            ).first()
            if not existing_repo:
                repo_meta = RepositoryMeta(
                    repo_name=self.remote_name, file_table_name=self.table_name
                )
                session.add(repo_meta)
                session.commit()

        # Dynamically create the file entry model and its table.
        self.file_entry_model = create_file_entry_model(self.table_name)
        SQLModel.metadata.create_all(engine, tables=[self.file_entry_model.__table__])  # type: ignore

    def insert_data(self) -> None:
        # Insert sample data into the dynamic table for this remote.
        with Session(engine) as session:
            file1 = self.file_entry_model(
                parent=f"/{self.remote_name}",
                name="book1.pdf",
                size=2048,
                mime_type="application/pdf",
                mod_time="2025-03-03T12:00:00",
            )
            file2 = self.file_entry_model(
                parent=f"/{self.remote_name}",
                name="book2.epub",
                size=1024,
                mime_type="application/epub+zip",
                mod_time="2025-03-03T12:05:00",
            )
            session.add(file1)
            session.add(file2)
            session.commit()

    def query(self) -> None:
        # Query the dynamic table and verify the expected file entries.
        with Session(engine) as session:
            file_entries = session.exec(select(self.file_entry_model)).all()

            # Assert that two file entries exist.
            assert (
                len(file_entries) == 2
            ), f"Expected 2 file entries, found {len(file_entries)}"

            # Map file names to their entries for easier verification.
            entries = {entry.name: entry for entry in file_entries}
            expected_entries = {
                "book1.pdf": {
                    "size": 2048,
                    "mime_type": "application/pdf",
                    "mod_time": "2025-03-03T12:00:00",
                    "parent": f"/{self.remote_name}",
                },
                "book2.epub": {
                    "size": 1024,
                    "mime_type": "application/epub+zip",
                    "mod_time": "2025-03-03T12:05:00",
                    "parent": f"/{self.remote_name}",
                },
            }

            for name, expected in expected_entries.items():
                assert name in entries, f"Expected entry with name {name} not found."
                entry = entries[name]
                for field, expected_value in expected.items():
                    actual_value = getattr(entry, field)
                    assert (
                        actual_value == expected_value
                    ), f"Mismatch for {name} in field '{field}': expected {expected_value}, got {actual_value}"
        print("unit_test passed!")


# --- Unit Test Implementation ---
def unit_test() -> None:
    ts = TableSection("dst:TorrentBooks", "file_entries_dst_torrentbooks")
    ts.insert_data()
    ts.query()


if __name__ == "__main__":
    unit_test()
