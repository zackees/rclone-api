from dataclasses import dataclass

from rclone_api.dir import Dir
from rclone_api.file import File


@dataclass
class DirListing:
    """Remote file dataclass."""

    dirs: list[Dir]
    files: list[File]
