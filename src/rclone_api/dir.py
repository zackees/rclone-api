import json
from typing import Generator

from rclone_api.dir_listing import DirListing
from rclone_api.remote import Remote
from rclone_api.rpath import RPath


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

    def ls(self, max_depth: int = 0, reverse: bool = False) -> DirListing:
        """List files and directories in the given path."""
        assert self.path.rclone is not None
        dir = Dir(self.path)
        return self.path.rclone.ls(dir, max_depth=max_depth, reverse=reverse)

    def walk(
        self, breadth_first: bool, max_depth: int = -1
    ) -> Generator[DirListing, None, None]:
        """List files and directories in the given path."""
        from rclone_api.walk import walk

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
