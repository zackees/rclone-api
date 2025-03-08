import json
import warnings

from rclone_api.rpath import RPath


def _dedupe(items: list[RPath]) -> list[RPath]:
    """Remove duplicate items from a list of RPath objects."""
    seen = set()
    unique_items = []
    for item in items:
        if item not in seen:
            seen.add(item)
            unique_items.append(item)
        else:
            warnings.warn(f"Duplicate item found: {item}, filtered out.")
    return unique_items


class DirListing:
    """Remote file dataclass."""

    def __init__(self, dirs_and_files: list[RPath]) -> None:
        from rclone_api.dir import Dir
        from rclone_api.file import File

        dirs_and_files = _dedupe(dirs_and_files)

        self.dirs: list[Dir] = [Dir(d) for d in dirs_and_files if d.is_dir]
        self.files: list[File] = [File(f) for f in dirs_and_files if not f.is_dir]

    def files_relative(self, prefix: str) -> list[str]:
        """Return a list of file paths relative to the root directory."""
        from rclone_api.file import File

        out: list[str] = []
        f: File
        for f in self.files:
            out.append(f.relative_to(prefix))
        return out

    def __str__(self) -> str:
        n_files = len(self.files)
        n_dirs = len(self.dirs)
        msg = f"\nFiles: {n_files}\n"
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
