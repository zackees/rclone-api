from typing import Generator

from rclone_api.dir_listing import DirListing
from rclone_api.remote import Remote
from rclone_api.rpath import RPath


class Dir:
    """Remote file dataclass."""

    def __init__(self, path: RPath | Remote) -> None:
        """Initialize Dir with either an RPath or Remote.

        Args:
            path: Either an RPath object or a Remote object
        """
        if isinstance(path, Remote):
            # Need to create an RPath for the Remote's root
            self.path = RPath(
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

    def ls(self, max_depth: int = 0) -> DirListing:
        """List files and directories in the given path."""
        assert self.path.rclone is not None
        return self.path.rclone.ls(self.path.path, max_depth=max_depth)

    def walk(self, max_depth: int = -1) -> Generator[DirListing, None, None]:
        """List files and directories in the given path."""
        from rclone_api.walk import walk

        assert self.path.rclone is not None
        return walk(self, max_depth=max_depth)

    def __str__(self) -> str:
        return str(self.path)
