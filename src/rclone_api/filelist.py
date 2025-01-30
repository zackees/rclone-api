from dataclasses import dataclass

from rclone_api.dir import Dir
from rclone_api.dir_listing import DirListing
from rclone_api.file import File
from rclone_api.rpath import RPath


@dataclass
class FileList:
    """Remote file dataclass."""

    dirs: list[Dir]
    files: list[File]

    def _to_dir_list(self) -> list[RPath]:
        pathlist: list[RPath] = []
        for d in self.dirs:
            pathlist.append(d.path)
        for f in self.files:
            pathlist.append(f.path)
        return pathlist

    def __str__(self) -> str:
        pathlist: list[RPath] = self._to_dir_list()
        return str(DirListing(pathlist))

    def __repr__(self) -> str:
        pathlist: list[RPath] = self._to_dir_list()
        return repr(DirListing(pathlist))
