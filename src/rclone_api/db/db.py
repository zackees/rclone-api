"""
Database module for rclone_api.
"""

import os
from threading import Lock
from typing import Optional

from dotenv import load_dotenv
from sqlmodel import Session, SQLModel, create_engine, select

from rclone_api.db.models import RepositoryMeta, create_file_entry_model
from rclone_api.file import FileItem


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

    def query_files(self, remote_name: str) -> list[FileItem]:
        """Query files from the database.

        Args:
            remote_name: Name of the remote
        """
        repo = self.get_or_create_repo(remote_name)
        files = repo.get_files()
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

    def insert_file(self, file: FileItem) -> None:
        """Insert a file entry into the table.

        Args:
            file: File entry
        """
        return self.insert_files([file])

    def insert_files(self, files: list[FileItem]) -> None:
        """Insert multiple file entries into the table.

        On conflict (i.e. if a file with the same path and name exists),
        update its size, mime_type, mod_time, and suffix.

        Ensure that your FileEntryModel has a unique constraint on the
        (path, name) columns.
        """
        from sqlalchemy.dialects.sqlite import insert

        with Session(self.engine) as session:
            for file in files:
                stmt = insert(self.FileEntryModel).values(
                    path=file.path_no_remote,
                    name=file.name,
                    size=file.size,
                    mime_type=file.mime_type,
                    mod_time=file.mod_time,
                    suffix=file.suffix,
                )
                # Here we update on conflict based on unique columns "path" and "name".
                stmt = stmt.on_conflict_do_update(
                    index_elements=["path"],
                    set_={
                        "size": file.size,
                        "mime_type": file.mime_type,
                        "mod_time": file.mod_time,
                        "suffix": file.suffix,
                    },
                )
                session.exec(stmt)  # type: ignore
            session.commit()

    def get_files(self) -> list[FileItem]:
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
