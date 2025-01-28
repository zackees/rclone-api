import json
from pathlib import Path
from typing import Any


class RPath:
    """Remote file dataclass."""

    def __init__(
        self,
        path: str,
        name: str,
        size: int,
        mime_type: str,
        mod_time: str,
        is_dir: bool,
    ) -> None:
        from rclone_api.rclone import Rclone

        self.path = path
        self.name = name
        self.size = size
        self.mime_type = mime_type
        self.mod_time = mod_time
        self.is_dir = is_dir
        self.rclone: Rclone | None = None

    def set_rclone(self, rclone: Any) -> None:
        """Set the rclone object."""
        from rclone_api.rclone import Rclone

        assert isinstance(rclone, Rclone)
        self.rclone = rclone

    @staticmethod
    def from_dict(data: dict) -> "RPath":
        """Create a File from a dictionary."""
        return RPath(
            data["Path"],
            data["Name"],
            data["Size"],
            data["MimeType"],
            data["ModTime"],
            data["IsDir"],
            # data["IsBucket"],
        )

    @staticmethod
    def from_array(data: list[dict]) -> list["RPath"]:
        """Create a File from a dictionary."""
        out: list[RPath] = []
        for d in data:
            file: RPath = RPath.from_dict(d)
            out.append(file)
        return out

    @staticmethod
    def from_json_str(json_str: str) -> list["RPath"]:
        """Create a File from a JSON string."""
        json_obj = json.loads(json_str)
        if isinstance(json_obj, dict):
            return [RPath.from_dict(json_obj)]
        return RPath.from_array(json_obj)

    def to_json(self) -> dict:
        return {
            "Path": self.path,
            "Name": self.name,
            "Size": self.size,
            "MimeType": self.mime_type,
            "ModTime": self.mod_time,
            "IsDir": self.is_dir,
            # "IsBucket": self.is_bucket,
        }

    def read_text(self) -> str:
        """Read the file contents as bytes.

        Returns:
            bytes: The file contents

        Raises:
            RuntimeError: If no rclone instance is associated with this file
            RuntimeError: If the path represents a directory
        """
        if self.rclone is None:
            raise RuntimeError("No rclone instance associated with this file")
        if self.is_dir:
            raise RuntimeError("Cannot read a directory as bytes")

        result = self.rclone._run(["cat", self.path])
        return result.stdout

    def read(self, dest: Path) -> None:
        """Copy the remote file to a local destination path.

        Args:
            dest: Local destination path where the file will be copied

        Raises:
            RuntimeError: If no rclone instance is associated with this file
            RuntimeError: If the path represents a directory
        """
        if self.rclone is None:
            raise RuntimeError("No rclone instance associated with this file")
        if self.is_dir:
            raise RuntimeError("Cannot read a directory as a file")

        self.rclone._run(["copyto", self.path, str(dest)])

    def __str__(self) -> str:
        out = self.to_json()
        return json.dumps(out)
