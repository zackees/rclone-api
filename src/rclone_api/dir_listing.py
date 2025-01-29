# from rclone_api.dir import Dir
# from rclone_api.file import File
from rclone_api.rpath import RPath


class DirListing:
    """Remote file dataclass."""

    def __init__(self, dirs_and_files: list[RPath]) -> None:
        from rclone_api.dir import Dir
        from rclone_api.file import File

        self.dirs: list[Dir] = [Dir(d) for d in dirs_and_files if d.is_dir]
        self.files: list[File] = [File(f) for f in dirs_and_files if not f.is_dir]
