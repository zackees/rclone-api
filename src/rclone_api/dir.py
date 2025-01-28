from rclone_api.file import File
from rclone_api.rpath import RPath


class Dir:
    """Remote file dataclass."""

    def __init__(self, path: RPath) -> None:
        self.path = path

    def ls(self) -> tuple[list["Dir"], list[File]]:
        """List files and directories in the given path."""
        cmd = ["lsjson", "--files-only", "--dirs-only", "--json", str(self.path)]
        assert self.path.rclone is not None
        cp = self.path.rclone._run(cmd)
        text = cp.stdout
        tmp: list[RPath] = RPath.from_json_str(text)
        for t in tmp:
            t.set_rclone(self.path.rclone)
        # dirs = [o for o in out if o.is_dir]
        # files = [o for o in out if not o.is_dir]
        dirs = [Dir(p) for p in tmp if p.is_dir]
        files = [File(p) for p in tmp if not p.is_dir]
        return dirs, files

    def __str__(self) -> str:
        return str(self.path)
