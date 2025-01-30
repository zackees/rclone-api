import json

from rclone_api.rpath import RPath


class DirListing:
    """Remote file dataclass."""

    def __init__(self, dirs_and_files: list[RPath]) -> None:
        from rclone_api.dir import Dir
        from rclone_api.file import File

        self.dirs: list[Dir] = [Dir(d) for d in dirs_and_files if d.is_dir]
        self.files: list[File] = [File(f) for f in dirs_and_files if not f.is_dir]

    def __str__(self) -> str:
        n_files = len(self.files)
        n_dirs = len(self.dirs)
        msg = f"Files: {n_files}\n"
        if n_files > 0:
            for f in self.files:
                msg += f"  {f}\n"
        msg += f"Dirs: {n_dirs}\n"
        if n_dirs > 0:
            for d in self.dirs:
                msg += f"  {d}\n"
        return msg

    def __repr__(self) -> str:
        dirs: list = []
        files: list = []
        for d in self.dirs:
            dirs.append(d.path.to_json())
        for f in self.files:
            files.append(f.path.to_json())
        json_obj = {
            "dirs": dirs,
            "files": files,
        }
        return json.dumps(json_obj, indent=2)
