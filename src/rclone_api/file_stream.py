"""
Unit test file.
"""

from typing import Generator

from rclone_api.file import FileItem
from rclone_api.process import Process


class FilesStream:

    def __init__(self, path: str, process: Process) -> None:
        self.path = path
        self.process = process

    def __enter__(self) -> "FilesStream":
        self.process.__enter__()
        return self

    def __exit__(self, *exc_info):
        self.process.__exit__(*exc_info)

    def files(self) -> Generator[FileItem, None, None]:
        line: bytes
        for line in self.process.stdout:
            linestr: str = line.decode("utf-8").strip()
            if linestr.startswith("["):
                continue
            if linestr.endswith(","):
                linestr = linestr[:-1]
            if linestr.endswith("]"):
                continue
            fileitem: FileItem | None = FileItem.from_json_str(self.path, linestr)
            if fileitem is None:
                continue
            yield fileitem

    def files_paged(
        self, page_size: int = 1000
    ) -> Generator[list[FileItem], None, None]:
        page: list[FileItem] = []
        for fileitem in self.files():
            page.append(fileitem)
            if len(page) >= page_size:
                yield page
                page = []
        if len(page) > 0:
            yield page

    def __iter__(self) -> Generator[FileItem, None, None]:
        return self.files()
