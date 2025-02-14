from dataclasses import dataclass
from enum import Enum
from queue import Queue
from threading import Thread
from typing import Generator

from rclone_api.process import Process


class LineType(Enum):
    EQUAL = 1
    MISSING_ON_SRC = 2
    MISSING_ON_DST = 3


@dataclass
class QueueItem:
    line_type: LineType
    line: str


def _async_diff_stream_from_running_process(
    running_process: Process,
    src_slug: str,
    dst_slug: str,
    output: Queue[QueueItem | None],
) -> None:
    count = 0
    first_few_lines: list[str] = []
    try:
        assert running_process.stdout is not None
        n_max = 10
        for line in iter(running_process.stdout.readline, b""):
            try:
                line_str = line.decode("utf-8").strip()
                if len(first_few_lines) < n_max:
                    first_few_lines.append(line_str)
                if line_str.startswith("="):
                    output.put(QueueItem(LineType.EQUAL, line_str[1:].strip()))
                    count += 1
                    continue
                if line_str.startswith("-"):
                    slug = line_str[1:].strip()
                    # print(f"Missing on src: {slug}")
                    output.put(QueueItem(LineType.MISSING_ON_SRC, f"{dst_slug}/{slug}"))
                    count += 1
                    continue
                if line_str.startswith("+"):
                    slug = line_str[1:].strip()
                    output.put(QueueItem(LineType.MISSING_ON_DST, f"{src_slug}/{slug}"))
                    count += 1
                    continue
                # print(f"unhandled: {line_str}")
            except UnicodeDecodeError:
                print("UnicodeDecodeError")
                continue
        output.put(None)
        print("done")
    except KeyboardInterrupt:
        import _thread

        print("KeyboardInterrupt")
        output.put(None)
        _thread.interrupt_main()
    if count == 0:
        first_lines_str = "\n".join(first_few_lines)
        raise ValueError(
            f"No output from rclone check, first few lines: {first_lines_str}"
        )


def diff_stream_from_running_process(
    running_process: Process,
    src_slug: str,
    dst_slug: str,
) -> Generator[QueueItem, None, None]:
    output: Queue[QueueItem | None] = Queue()
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
