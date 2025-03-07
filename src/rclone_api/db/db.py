"""
Database module for rclone_api.
"""

import os
from threading import Lock
from typing import Optional

from sqlmodel import Session, SQLModel, create_engine, select

from rclone_api.db.models import RepositoryMeta, create_file_entry_model
from rclone_api.file import FileItem


def _to_table_name(remote_name: str) -> str:
    return (
        "files_"
        + remote_name.replace(":", "_").replace(" ", "_").replace("/", "_").lower()
    )


class DB:
    """Database class for rclone_api."""

    def __init__(self, db_path_url: str):
        """Initialize the database.

        Args:
            db_path: Path to the database file
        """
        self.db_path_url = db_path_url

        # When running multiple commands in parallel, the database connection may fail once
        # when the database is first populated.
        retries = 2
        for _ in range(retries):
            try:
                self.engine = create_engine(db_path_url)
                SQLModel.metadata.create_all(self.engine)
                break
            except Exception as e:
                print(f"Failed to connect to database. Retrying... {e}")
        else:
            raise Exception("Failed to connect to database.")
        self._cache: dict[str, DBRepo] = {}
        self._cache_lock = Lock()

    def drop_all(self) -> None:
        """Drop all tables in the database."""
        SQLModel.metadata.drop_all(self.engine)

    def close(self) -> None:
        """Close the database connection and release resources."""
        if hasattr(self, "engine") and self.engine is not None:
            self.engine.dispose()

    def add_files(self, files: list[FileItem]) -> None:
        """Add files to the database.

        Args:
            remote_name: Name of the remote
            files: List of file entries
        """

        partition: dict[str, list[FileItem]] = {}
        for file in files:
            partition.setdefault(file.remote, []).append(file)

        for remote_name, files in partition.items():
            repo = self.get_or_create_repo(remote_name)
            repo.insert_files(files)

    def query_all_files(self, remote_name: str) -> list[FileItem]:
        """Query files from the database.

        Args:
            remote_name: Name of the remote
        """
        repo = self.get_or_create_repo(remote_name)
        files = repo.get_all_files()
        out: list[FileItem] = []
        for file in files:
            out.append(file)
        return out

    def get_or_create_repo(self, remote_name: str) -> "DBRepo":
        """Get a table section for a remote.

        Args:
            remote_name: Name of the remote
            table_name: Optional table name, will be derived from remote_name if not provided

        Returns:
            DBRepo: A table section for the remote
        """
        with self._cache_lock:
            if remote_name in self._cache:
                return self._cache[remote_name]
            table_name = _to_table_name(remote_name)
            out = DBRepo(self.engine, remote_name, table_name)
            self._cache[remote_name] = out
            return out


class DBRepo:
    """Table repo remote."""

    def __init__(self, engine, remote_name: str, table_name: Optional[str] = None):
        """Initialize a table section.

        Args:
            engine: SQLAlchemy engine
            remote_name: Name of the remote
            table_name: Optional table name, will be derived from remote_name if not provided
        """
        self.engine = engine
        self.remote_name = remote_name

        # If table_name is not provided, derive one from the remote name.
        if table_name is None:
            # table_name = (
            #     "file_entries_"
            #     + remote_name.replace(":", "_").replace(" ", "_").replace("/", "_").lower()
            # )
            table_name = _to_table_name(remote_name)
        self.table_name = table_name

        # Check if repository exists in RepositoryMeta; if not, create a new entry.
        with Session(self.engine) as session:
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
        self.FileEntryModel = create_file_entry_model(self.table_name)
        SQLModel.metadata.create_all(self.engine, tables=[self.FileEntryModel.__table__])  # type: ignore

    def insert_file(self, file: FileItem) -> None:
        """Insert a file entry into the table.

        Args:
            file: File entry
        """
        return self.insert_files([file])

    def insert_files(self, files: list[FileItem]) -> None:
        """
        Insert multiple file entries into the table.

        Three bulk operations are performed:
        1. Select: Determine which files already exist.
        2. Insert: Bulk-insert new file entries.
        3. Update: Bulk-update existing file entries.

        The FileEntryModel must define a unique constraint on (path, name) and have a primary key "id".
        """
        # Step 1: Bulk select existing records.
        # get_exists() returns a set of FileItem objects (based on path_no_remote and name) that already exist.
        existing_files = self.get_exists(files)

        # Determine which files need to be updated vs. inserted.
        needs_update = existing_files
        is_new = set(files) - existing_files

        # Step 2: Bulk insert new rows.
        new_values = [
            {
                "path": file.path_no_remote,
                "name": file.name,
                "size": file.size,
                "mime_type": file.mime_type,
                "mod_time": file.mod_time,
                "suffix": file.real_suffix,
            }
            for file in is_new
        ]
        with Session(self.engine) as session:
            if new_values:
                session.bulk_insert_mappings(self.FileEntryModel, new_values)  # type: ignore
                session.commit()

        # Step 3: Bulk update existing rows.
        # First, query the database for the primary keys of rows that match the unique keys in needs_update.
        with Session(self.engine) as session:
            # Collect all unique paths from files needing update.
            update_paths = [file.path_no_remote for file in needs_update]
            # Query for existing rows matching any of these paths.
            db_entries = session.exec(
                select(self.FileEntryModel).where(
                    self.FileEntryModel.path.in_(update_paths)  # type: ignore
                )
            ).all()

            # Build a mapping from the unique key (path, name) to the primary key (id).
            id_map = {(entry.path, entry.name): entry.id for entry in db_entries}

            # Prepare bulk update mappings.
            update_values = []
            for file in needs_update:
                key = (file.path_no_remote, file.name)
                if key in id_map:
                    update_values.append(
                        {
                            "id": id_map[key],
                            "size": file.size,
                            "mime_type": file.mime_type,
                            "mod_time": file.mod_time,
                            "suffix": file.real_suffix,
                        }
                    )
            if update_values:
                session.bulk_update_mappings(self.FileEntryModel, update_values)  # type: ignore
                session.commit()

    def get_exists(self, files: list[FileItem]) -> set[FileItem]:
        """Get file entries from the table that exist among the given files.

        Args:
            files: List of file entries

        Returns:
            Set of FileItem instances whose 'path_no_remote' exists in the table.
        """
        # Extract unique paths from the input files.
        paths = {file.path_no_remote for file in files}

        with Session(self.engine) as session:
            # Execute a single query to fetch all file paths in the table that match the input paths.
            result = session.exec(
                select(self.FileEntryModel.path).where(
                    self.FileEntryModel.path.in_(paths)  # type: ignore
                )
            ).all()
            # Convert the result to a set for fast membership tests.
            existing_paths = set(result)

        # Return the set of FileItem objects that have a path in the existing_paths.
        return {file for file in files if file.path_no_remote in existing_paths}

    def get_all_files(self) -> list[FileItem]:
        """Get all files in the table.

        Returns:
            list: List of file entries
        """
        # with Session(self.engine) as session:
        #     return session.exec(select(self.FileEntryModel)).all()
        out: list[FileItem] = []
        with Session(self.engine) as session:
            query = session.exec(select(self.FileEntryModel)).all()
            for item in query:
                name = item.name  # type: ignore
                size = item.size  # type: ignore
                mime_type = item.mime_type  # type: ignore
                mod_time = item.mod_time  # type: ignore
                path = item.path  # type: ignore
                parent = os.path.dirname(path)
                if parent == "/" or parent == ".":
                    parent = ""
                o = FileItem(
                    remote=self.remote_name,
                    parent=parent,
                    name=name,
                    size=size,
                    mime_type=mime_type,
                    mod_time=mod_time,
                )
                out.append(o)
        return out
