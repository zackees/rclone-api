import json
from typing import Any

from rclone_api.remote import Remote


class RPath:
    """Remote file dataclass."""

    def __init__(
        self,
        remote: Remote,
        path: str,
        name: str,
        size: int,
        mime_type: str,
        mod_time: str,
        is_dir: bool,
    ) -> None:
        from rclone_api.rclone import Rclone

        if "dst:" in path:
            raise ValueError(f"Invalid path: {path}")

        self.remote = remote
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
    def from_dict(data: dict, remote: Remote) -> "RPath":
        """Create a File from a dictionary."""
        return RPath(
            remote,
            data["Path"],
            data["Name"],
            data["Size"],
            data["MimeType"],
            data["ModTime"],
            data["IsDir"],
            # data["IsBucket"],
        )

    @staticmethod
    def from_array(data: list[dict], remote: Remote) -> list["RPath"]:
        """Create a File from a dictionary."""
        out: list[RPath] = []
        for d in data:
            file: RPath = RPath.from_dict(d, remote)
            out.append(file)
        return out

    @staticmethod
    def from_json_str(json_str: str, remote: Remote) -> list["RPath"]:
        """Create a File from a JSON string."""
        json_obj = json.loads(json_str)
        if isinstance(json_obj, dict):
            return [RPath.from_dict(json_obj, remote)]
        return RPath.from_array(json_obj, remote)

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

    def __str__(self) -> str:
        return f"{self.remote.name}:{self.path}"
