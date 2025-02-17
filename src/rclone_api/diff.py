import warnings
from dataclasses import dataclass
from enum import Enum
from queue import Queue
from threading import Thread
from typing import Generator

from rclone_api.process import Process


class DiffType(Enum):
    EQUAL = "="
    MISSING_ON_SRC = (
        "-"  # means path was missing on the source, so only in the destination
    )
    MISSING_ON_DST = (
        "+"  # means path was missing on the destination, so only in the source
    )
    DIFFERENT = "*"  # means path was present in source and destination but different.
    ERROR = "!"  # means there was an error


@dataclass
class DiffItem:
    type: DiffType
    path: str
    src_prefix: str
    dst_prefix: str

    def __str__(self) -> str:
        return f"{self.type.value} {self.path}"

    def __repr__(self) -> str:
        return f"{self.type.name} {self.path}"

    def full_str(self) -> str:
        return f"{self.type.name} {self.src_prefix}/{self.path} {self.dst_prefix}/{self.path}"

    def dst_path(self) -> str:
        return f"{self.dst_prefix}/{self.path}"

    def src_path(self) -> str:
        return f"{self.src_prefix}/{self.path}"


def _classify_diff(line: str, src_slug: str, dst_slug: str) -> DiffItem | None:
    def _new(type: DiffType, path: str) -> DiffItem:
        return DiffItem(type, path, src_prefix=src_slug, dst_prefix=dst_slug)

    suffix = line[1:].strip() if len(line) > 0 else ""
    if line.startswith(DiffType.EQUAL.value):
        return _new(DiffType.EQUAL, suffix)
    if line.startswith(DiffType.MISSING_ON_SRC.value):
        return _new(DiffType.MISSING_ON_SRC, suffix)
    if line.startswith(DiffType.MISSING_ON_DST.value):
        # return DiffItem(DiffType.MISSING_ON_DST, f"{src_slug}/{suffix}")
        return _new(DiffType.MISSING_ON_DST, suffix)
    if line.startswith(DiffType.DIFFERENT.value):
        # return DiffItem(DiffType.DIFFERENT, suffix)
        return _new(DiffType.DIFFERENT, suffix)
    if line.startswith(DiffType.ERROR.value):
        # return DiffItem(DiffType.ERROR, suffix)
        return _new(DiffType.ERROR, suffix)
    return None


def _async_diff_stream_from_running_process(
    running_process: Process,
    src_slug: str,
    dst_slug: str,
    output: Queue[DiffItem | None],
) -> None:
    count = 0
    first_few_lines: list[str] = []
    check = True
    try:
        assert running_process.stdout is not None
        n_max = 10
        for line in iter(running_process.stdout.readline, b""):
            try:
                line_str = line.decode("utf-8").strip()
                if len(first_few_lines) < n_max:
                    first_few_lines.append(line_str)
                # _classify_line_type
                diff_item: DiffItem | None = _classify_diff(
                    line_str, src_slug, dst_slug
                )
                if diff_item is None:
                    # Some other output that we don't care about, debug print etc.
                    continue
                output.put(diff_item)
                count += 1
                # print(f"unhandled: {line_str}")
            except UnicodeDecodeError:
                print("UnicodeDecodeError")
                continue
        output.put(None)
    except KeyboardInterrupt:
        import _thread

        check = False

        print("KeyboardInterrupt")
        output.put(None)
        _thread.interrupt_main()
    if count == 0 and check:
        first_lines_str = "\n".join(first_few_lines)
        warning_msg = f"No output from rclone check, first few lines: {first_lines_str}"
        warnings.warn(warning_msg)
        raise ValueError(warning_msg)


def diff_stream_from_running_process(
    running_process: Process,
    src_slug: str,
    dst_slug: str,
) -> Generator[DiffItem, None, None]:
    output: Queue[DiffItem | None] = Queue()
    # process_output_to_diff_stream(running_process, src_slug, dst_slug, output)
    thread = Thread(
        target=_async_diff_stream_from_running_process,
        args=(running_process, src_slug, dst_slug, output),
        daemon=True,
    )
    thread.start()
    while True:
        item = output.get()
        if item is None:
            break
        yield item
    thread.join(timeout=5)
