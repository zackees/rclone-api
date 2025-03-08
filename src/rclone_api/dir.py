import json
from pathlib import Path
from typing import Generator

from rclone_api.dir_listing import DirListing
from rclone_api.remote import Remote
from rclone_api.rpath import RPath
from rclone_api.types import ListingOption, Order


class Dir:
    """Remote file dataclass."""

    @property
    def remote(self) -> Remote:
        return self.path.remote

    @property
    def name(self) -> str:
        return self.path.name

    def __init__(self, path: RPath | Remote) -> None:
        """Initialize Dir with either an RPath or Remote.

        Args:
            path: Either an RPath object or a Remote object
        """
        if isinstance(path, Remote):
            # Need to create an RPath for the Remote's root
            self.path = RPath(
                remote=path,
                path=str(path),
                name=str(path),
                size=0,
                mime_type="inode/directory",
                mod_time="",
                is_dir=True,
            )
            # Ensure the RPath has the same rclone instance as the Remote
            self.path.set_rclone(path.rclone)
        else:
            self.path = path
        # self.path.set_rclone(self.path.remote.rclone)
        assert self.path.rclone is not None

    def ls(
        self,
        max_depth: int | None = None,
        glob: str | None = None,
        order: Order = Order.NORMAL,
        listing_option: ListingOption = ListingOption.ALL,
    ) -> DirListing:
        """List files and directories in the given path."""
        assert self.path.rclone is not None
        dir = Dir(self.path)
        return self.path.rclone.ls(
            dir,
            max_depth=max_depth,
            glob=glob,
            order=order,
            listing_option=listing_option,
        )

    def relative_to(self, other: "Dir") -> str:
        """Return the relative path to the other directory."""
        self_path = Path(self.path.path)
        other_path = Path(other.path.path)
        rel_path = self_path.relative_to(other_path)
        return str(rel_path.as_posix())

    def walk(
        self, breadth_first: bool, max_depth: int = -1
    ) -> Generator[DirListing, None, None]:
        """List files and directories in the given path."""
        from rclone_api.detail.walk import walk

        assert self.path.rclone is not None
        return walk(self, breadth_first=breadth_first, max_depth=max_depth)

    def to_json(self) -> dict:
        """Convert the Dir to a JSON serializable dictionary."""
        return self.path.to_json()

    def __str__(self) -> str:
        return str(self.path)

    def __repr__(self) -> str:
        data = self.path.to_json()
        data_str = json.dumps(data)
        return data_str

    def to_string(self, include_remote: bool = True) -> str:
        """Convert the File to a string."""
        out = str(self.path)
        if not include_remote:
            _, out = out.split(":", 1)
        return out

    # / operator
    def __truediv__(self, other: str) -> "Dir":
        """Join the current path with another path."""
        path = Path(self.path.path) / other
        rpath = RPath(
            self.path.remote,
            str(path.as_posix()),
            name=other,
            size=0,
            mime_type="inode/directory",
            mod_time="",
            is_dir=True,
        )
        rpath.set_rclone(self.path.rclone)
        return Dir(rpath)
