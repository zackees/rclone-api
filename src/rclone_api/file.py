import json
import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from rclone_api.rpath import RPath

_STRING_INTERNER: dict[str, str] = {}


def _intern(s: str) -> str:
    return _STRING_INTERNER.setdefault(s, s)


_SUFFIX_LARGEST_SIZE = len("torrents") + 2


def _suffix_clean_bad_parts(suffix: list[str]) -> list[str]:
    """Remove any bad parts from the suffix list."""
    out = []
    for part in suffix:
        if part in ["", ""]:
            continue
        if " " in part:
            # split on spaces
            continue
        if "--" in part:
            # split on --
            parts = part.split("--")
            parts = [x.strip() for x in parts if x.strip()]
            out.extend(parts)
        out.append(part)

    out, tmp = [], out
    for part in tmp:
        if len(part) > _SUFFIX_LARGEST_SIZE:
            continue
        out.append(part)
    return out


def _get_suffix(name: str, chop_compressed_suffixes: bool = True) -> str:
    # name.sql.gz -> sql.gz
    try:
        parts = name.split(".")
        if len(parts) == 1:
            return ""
        parts = _suffix_clean_bad_parts(parts)
        last_part = parts[-1]
        if chop_compressed_suffixes:
            if last_part == "gz" and len(parts) > 2:
                parts = parts[:-1]
        return ".".join(parts[-1:])
    except IndexError:
        warnings.warn(f"Invalid name: {name} for normal suffix extraction")
        suffix = Path(name).suffix
        if suffix.startswith("."):
            return suffix[1:]
        return suffix


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
    hash: str | None = None
    id: int | None = None

    @property
    def path(self) -> str:
        if self.parent == ".":
            return f"{self.remote}/{self.name}"
        else:
            return f"{self.remote}/{self.parent}/{self.name}"

    @property
    def path_no_remote(self) -> str:
        if self.parent == ".":
            return f"{self.name}"
        else:
            return f"{self.parent}/{self.name}"

    @property
    def real_suffix(self) -> str:
        return self._suffix

    def __post_init__(self):
        self.parent = _intern(self.parent)
        self.mime_type = _intern(self.mime_type)
        self.remote = _intern(self.remote)
        self._suffix = _intern(_get_suffix(self.name))

    @staticmethod
    def from_json(remote: str, data: dict) -> "FileItem | None":
        try:
            path_str: str = data["Path"]
            parent_path = Path(path_str).parent.as_posix()
            name = data["Name"]
            size = data["Size"]
            mime_type = data["MimeType"]
            mod_time = data["ModTime"]

            return FileItem(
                remote=remote,
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
    def from_json_str(remote: str, data: str) -> "FileItem | None":
        try:
            data_dict = json.loads(data)
            return FileItem.from_json(remote, data_dict)
        except json.JSONDecodeError:
            warnings.warn(f"Invalid JSON data: {data}")
            return None

    # hasher for set membership
    def __hash__(self) -> int:
        return hash(self.path_no_remote)


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

    def mod_time(self) -> str:
        return self.path.mod_time

    def mod_time_dt(self) -> datetime:
        """Return the modification time as a datetime object."""
        return self.path.mod_time_dt()

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
