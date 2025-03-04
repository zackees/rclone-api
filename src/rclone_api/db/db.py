"""
Database module for rclone_api.
"""

import os
from dataclasses import dataclass
from threading import Lock
from typing import Any, Optional

from dotenv import load_dotenv
from sqlmodel import Session, SQLModel, create_engine, select

from rclone_api.db.models import RepositoryMeta, create_file_entry_model


@dataclass
class DBFile:
    parent: str
    name: str
    size: int
    mime_type: str
    mod_time: str

    # test for equality
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DBFile):
            return False
        return (
            self.parent == other.parent
            and self.name == other.name
            and self.size == other.size
            and self.mime_type == other.mime_type
            and self.mod_time == other.mod_time
        )


def _db_url_from_env_or_raise() -> str:
    load_dotenv()
    db_url = os.getenv("DB_URL")
    if db_url is None:
        raise ValueError("DB_URL not set")
    return db_url


def _to_table_name(remote_name: str) -> str:
    return "file_entries_" + remote_name.replace(":", "_").replace(" ", "_").lower()


class DB:
    """Database class for rclone_api."""

    def __init__(self, db_path_url: str | None = None):
        """Initialize the database.

        Args:
            db_path: Path to the database file
        """
        db_path_url = db_path_url or _db_url_from_env_or_raise()
        assert isinstance(db_path_url, str)
        self.db_path_url = db_path_url
        self.engine = create_engine(db_path_url)

        # Create the meta table
        SQLModel.metadata.create_all(self.engine)
        self._cache: dict[str, DBRepo] = {}
        self._cache_lock = Lock()

    def close(self) -> None:
        """Close the database connection and release resources."""
        if hasattr(self, "engine") and self.engine is not None:
            self.engine.dispose()

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
            table_name = (
                "file_entries_"
                + remote_name.replace(":", "_").replace(" ", "_").lower()
            )
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

    def insert_file(self, file: DBFile) -> None:
        """Insert a file entry into the table.

        Args:
            file: File entry
        """
        with Session(self.engine) as session:
            # For efficiency, we use dynamic tables to hold each repo.
            file_entry = self.FileEntryModel(
                parent=file.parent,
                name=file.name,
                size=file.size,
                mime_type=file.mime_type,
                mod_time=file.mod_time,
            )
            session.add(file_entry)
            session.commit()

    def insert_files(self, files: list[DBFile]) -> None:
        """Insert multiple file entries into the table.

        Args:
            files: List of file entries
        """
        file_entries = [
            self.FileEntryModel(
                parent=file.parent,
                name=file.name,
                size=file.size,
                mime_type=file.mime_type,
                mod_time=file.mod_time,
            )
            for file in files
        ]
        with Session(self.engine) as session:
            session.add_all(file_entries)
            session.commit()

    def get_files(self) -> list[DBFile]:
        """Get all files in the table.

        Returns:
            list: List of file entries
        """
        # with Session(self.engine) as session:
        #     return session.exec(select(self.FileEntryModel)).all()
        out: list[DBFile] = []
        with Session(self.engine) as session:
            query = session.exec(select(self.FileEntryModel)).all()
            for item in query:
                name = item.name  # type: ignore
                size = item.size  # type: ignore
                mime_type = item.mime_type  # type: ignore
                mod_time = item.mod_time  # type: ignore
                parent = item.parent  # type: ignore
                o = DBFile(parent, name, size, mime_type, mod_time)
                out.append(o)
        return out
