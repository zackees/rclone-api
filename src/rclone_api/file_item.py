import json
import warnings
from dataclasses import dataclass
from pathlib import Path

_STRING_INTERNER: dict[str, str] = {}


def _intern(s: str) -> str:
    return _STRING_INTERNER.setdefault(s, s)


# File is too complex, this is a simple dataclass that can be streamed out.
@dataclass
class FileItem:
    """Remote file dataclass."""

    remote: str
    parent: str
    name: str
    size: int
    mime_type: str
    mod_time: str

    @property
    def path(self) -> str:
        return f"{self.remote}/{self.parent}/{self.name}"

    @property
    def suffix(self) -> str:
        return self._suffix

    def __post_init__(self):
        self.parent = _intern(self.parent)
        self.mime_type = _intern(self.mime_type)
        suffix = Path(self.name).suffix
        self._suffix = _intern(suffix)

    @staticmethod
    def from_json(data: dict) -> "FileItem | None":
        try:
            path_str: str = data["Path"]
            parent_path = Path(path_str).parent.as_posix()
            name = data["Name"]
            size = data["Size"]
            mime_type = data["MimeType"]
            mod_time = data["ModTime"]

            return FileItem(
                remote="DUMMY",
                parent=parent_path,
                name=name,
                size=size,
                mime_type=mime_type,
                mod_time=mod_time,
            )
        except KeyError:
            warnings.warn(f"Invalid data: {data}")
            return None

    @staticmethod
    def from_json_str(data: str) -> "FileItem | None":
        try:
            data_dict = json.loads(data)
            return FileItem.from_json(data_dict)
        except json.JSONDecodeError:
            warnings.warn(f"Invalid JSON data: {data}")
            return None
