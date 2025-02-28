from dataclasses import dataclass

from rclone_api.experimental.flags_base import BaseFlags, merge_flags
from rclone_api.types import SizeSuffix


@dataclass
class CopyFlags(BaseFlags):
    check_first: bool | None = None
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
    max_transfer: SizeSuffix | None = None
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
        return super().to_args()

    def merge(self, other: "CopyFlags") -> "CopyFlags":
        return merge_flags(CopyFlags, self, other)

    def __repr__(self):
        return super().__repr__()


@dataclass
class Flags(BaseFlags):
    copy: CopyFlags | None = None

    def to_args(self) -> list[str]:
        return super().to_args()

    def merge(self, other: "Flags") -> "Flags":
        return merge_flags(Flags, self, other)

    def __repr__(self):
        return super().__repr__()


def unit_test() -> None:
    copy_flags_a = CopyFlags(compare_dest=["a", "b"])
    copy_flags_b = CopyFlags(checksum=False)
    flags_a = copy_flags_a.merge(copy_flags_b)
    print("A:", flags_a)

    copy_flags_c = CopyFlags(checksum=True)
    copy_flags_d = CopyFlags(checksum=False)

    merged_c_d = copy_flags_c.merge(copy_flags_d)
    print("B:", merged_c_d)
    merged_d_c = copy_flags_d.merge(copy_flags_c)
    print("C:", merged_d_c)

    # now do the one with the SizeSuffix type
    copy_flags_e = CopyFlags(max_transfer=SizeSuffix("128M"))
    copy_flags_f = CopyFlags(max_transfer=SizeSuffix("256M"))
    merged_e_f = copy_flags_e.merge(copy_flags_f)
    print("D:", merged_e_f)


if __name__ == "__main__":
    unit_test()
