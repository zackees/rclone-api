"""
Database models for rclone_api.
"""

from typing import Optional, Type

from sqlmodel import Field, SQLModel


# Meta table that indexes all repositories
class RepositoryMeta(SQLModel, table=True):
    """Repository metadata table."""

    id: Optional[int] = Field(default=None, primary_key=True)
    repo_name: str
    file_table_name: str  # The dedicated table name for file entries


# Factory to dynamically create a FileEntry model with a given table name
def create_file_entry_model(table_name: str) -> Type[SQLModel]:
    """Create a file entry model with a given table name.

    Args:
        table_name: Table name

    Returns:
        Type[SQLModel]: File entry model
    """

    class FileEntry(SQLModel, table=True):
        __tablename__ = table_name  # type: ignore # dynamically set table name
        id: Optional[int] = Field(default=None, primary_key=True)
        parent: str
        name: str
        size: int
        mime_type: str
        mod_time: str

    return FileEntry
