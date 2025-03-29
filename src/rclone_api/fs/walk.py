from pathlib import Path
from typing import Generator

from rclone_api.fs.filesystem import FSPath, logger


def fs_walk(
    self: FSPath,
) -> Generator[tuple[FSPath, list[str], list[str]], None, None]:
    root_path = self
    stack: list[str] = [self.path]

    while stack:
        curr_path = stack.pop()
        curr_suffix_path = Path(curr_path).relative_to(Path(root_path.path)).as_posix()
        current_dir = root_path / curr_suffix_path
        try:
            filenames, dirnames = current_dir.ls()
        except Exception as e:
            logger.warning(f"Unable to list directory {current_dir}: {e}")
            continue

        yield current_dir, dirnames, filenames

        # Add subdirectories to stack for further traversal
        for dirname in reversed(dirnames):
            stack.append((current_dir / dirname).path)
