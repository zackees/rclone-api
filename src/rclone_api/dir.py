from rclone_api.dir_listing import DirListing
from rclone_api.rpath import RPath


class Dir:
    """Remote file dataclass."""

    def __init__(self, path: RPath) -> None:
        self.path = path

    def ls(self, max_depth: int = 0) -> DirListing:
        """List files and directories in the given path."""
        assert self.path.rclone is not None
        return self.path.rclone.ls(self.path.path, max_depth=max_depth)

    def __str__(self) -> str:
        return str(self.path)
