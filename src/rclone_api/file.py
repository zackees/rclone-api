import json
from pathlib import Path

from rclone_api.rpath import RPath


class File:
    """Remote file dataclass."""

    def __init__(
        self,
        path: RPath,
    ) -> None:
        self.path = path

    @property
    def name(self) -> str:
        return self.path.name

    def read_text(self) -> str:
        """Read the file contents as bytes.

        Returns:
            bytes: The file contents

        Raises:
            RuntimeError: If no rclone instance is associated with this file
            RuntimeError: If the path represents a directory
        """
        if self.path.rclone is None:
            raise RuntimeError("No rclone instance associated with this file")
        if self.path.is_dir:
            raise RuntimeError("Cannot read a directory as bytes")

        result = self.path.rclone._run(["cat", self.path.path], check=True)
        return result.stdout

    def to_json(self) -> dict:
        """Convert the File to a JSON serializable dictionary."""
        return self.path.to_json()

    def to_string(self, include_remote: bool = True) -> str:
        """Convert the File to a string."""
        # out = str(self.path)
        remote = self.path.remote
        rest = self.path.path
        if include_remote:
            return f"{remote.name}:{rest}"
        return rest

    def relative_to(self, prefix: str) -> str:
        """Return the relative path to the other directory."""
        self_path = Path(str(self))
        rel_path = self_path.relative_to(prefix)
        return str(rel_path.as_posix())

    @property
    def size(self) -> int:
        """Get the size of the file."""
        return self.path.size

    def __str__(self) -> str:
        return str(self.path)

    def __repr__(self) -> str:
        data = self.path.to_json()
        data_str = json.dumps(data)
        return data_str
