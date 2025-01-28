import json
from typing import Any


class File:
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
    def from_dict(data: dict) -> "File":
        """Create a File from a dictionary."""
        return File(
            data["Path"],
            data["Name"],
            data["Size"],
            data["MimeType"],
            data["ModTime"],
            data["IsDir"],
            # data["IsBucket"],
        )

    @staticmethod
    def from_array(data: list[dict]) -> list["File"]:
        """Create a File from a dictionary."""
        out: list[File] = []
        for d in data:
            file: File = File.from_dict(d)
            out.append(file)
        return out

    @staticmethod
    def from_json_str(json_str: str) -> list["File"]:
        """Create a File from a JSON string."""
        json_obj = json.loads(json_str)
        if isinstance(json_obj, dict):
            return [File.from_dict(json_obj)]
        return File.from_array(json_obj)

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
        out = self.to_json()
        return json.dumps(out)
