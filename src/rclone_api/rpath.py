import json
from datetime import datetime
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
        from rclone_api.rclone_impl import RcloneImpl

        if path.endswith("/"):
            path = path[:-1]
        self.remote = remote
        self.path = path
        self.name = name
        self.size = size
        self.mime_type = mime_type
        self.mod_time = mod_time
        self.is_dir = is_dir
        self.rclone: RcloneImpl | None = None

    def mod_time_dt(self) -> datetime:
        """Return the modification time as a datetime object."""
        return datetime.fromisoformat(self.mod_time)

    def set_rclone(self, rclone: Any) -> None:
        """Set the rclone object."""
        from rclone_api.rclone_impl import RcloneImpl

        assert isinstance(rclone, RcloneImpl)
        self.rclone = rclone

    @staticmethod
    def from_dict(
        data: dict, remote: Remote, parent_path: str | None = None
    ) -> "RPath":
        """Create a File from a dictionary."""
        path = data["Path"]
        if parent_path is not None:
            path = f"{parent_path}/{path}"
        return RPath(
            remote,
            path,
            data["Name"],
            data["Size"],
            data["MimeType"],
            data["ModTime"],
            data["IsDir"],
            # data["IsBucket"],
        )

    @staticmethod
    def from_array(
        data: list[dict], remote: Remote, parent_path: str | None = None
    ) -> list["RPath"]:
        """Create a File from a dictionary."""
        out: list[RPath] = []
        for d in data:
            file: RPath = RPath.from_dict(d, remote, parent_path)
            out.append(file)
        return out

    @staticmethod
    def from_json_str(
        json_str: str, remote: Remote, parent_path: str | None = None
    ) -> list["RPath"]:
        """Create a File from a JSON string."""
        json_obj = json.loads(json_str)
        if isinstance(json_obj, dict):
            return [RPath.from_dict(json_obj, remote, parent_path)]
        return RPath.from_array(json_obj, remote, parent_path)

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

    def __repr__(self):
        data = self.to_json()
        data["Remote"] = self.remote.name
        return json.dumps(data)
