from dataclasses import dataclass
from typing import Any


def _to_args(datclass: Any) -> list[str]:
    args = []
    for key, value in datclass.__dict__.items():
        if value is not None:
            args.append(f"--{key.replace('_', '-')}")
            if value is not True:
                args.append(str(value))
    return args


class CopyFlags:
    check_first: bool | None = False
    checksum: bool | None = False
    compare_dest: list[str] | None = None
    copy_dest: list[str] | None = None
    cutoff_mode: str | None = None
    ignore_case_sync: bool | None = None
    ignore_checksum: bool | None = None
    ignore_existing: bool | None = None
    ignore_size: bool | None = None
    ignore_times: bool | None = None
    immutable: bool | None = None
    inplace: bool | None = None
    links: bool | None = None
    max_backlog: int | None = None
    max_duration: str | None = None
    max_transfer: str | None = None
    metadata: bool | None = None
    modify_window: str | None = None
    multi_thread_chunk_size: str | None = None
    multi_thread_cutoff: str | None = None
    multi_thread_streams: int | None = None
    multi_thread_write_buffer_size: str | None = None
    no_check_dest: bool | None = None
    no_traverse: bool | None = None
    no_update_dir_modtime: bool | None = None
    no_update_modtime: bool | None = None
    order_by: str | None = None
    partial_suffix: str | None = None
    refresh_times: bool | None = None
    server_side_across_configs: bool | None = None
    size_only: bool | None = None
    streaming_upload_cutoff: str | None = None
    update: bool | None = None

    def to_args(self) -> list[str]:
        return _to_args(self)


@dataclass
class Flags:
    copy: CopyFlags | None = None

    def to_args(self) -> list[str]:
        args = []
        if self.copy:
            args.extend(self.copy.to_args())
        return args
