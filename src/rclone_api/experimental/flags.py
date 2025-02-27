from dataclasses import dataclass, fields, is_dataclass
from typing import Type, TypeVar

T = TypeVar("T")


def _merge(cls: Type[T], dataclass_a: T, dataclass_b: T) -> T:
    if not is_dataclass(dataclass_a) or not is_dataclass(dataclass_b):
        raise ValueError("Both inputs must be dataclass instances")
    if type(dataclass_a) is not type(dataclass_b):
        raise ValueError("Dataclass instances must be of the same type")

    merged_kwargs = {}
    for field in fields(dataclass_a):
        a_value = getattr(dataclass_a, field.name)
        b_value = getattr(dataclass_b, field.name)

        if is_dataclass(a_value) and is_dataclass(b_value):
            merged_kwargs[field.name] = _merge(type(a_value), a_value, b_value)
        else:
            merged_kwargs[field.name] = b_value if b_value is not None else a_value

    return cls(**merged_kwargs)


@dataclass
class BaseFlags:
    def to_args(self) -> list[str]:
        args = []
        for field in fields(self):
            value = getattr(self, field.name)
            if value is None:
                continue
            # If the field value is a nested dataclass that supports to_args, use it.
            if is_dataclass(value) and hasattr(value, "to_args"):
                to_args = getattr(value, "to_args")
                args.extend(to_args())
            elif isinstance(value, bool):
                # Only include the flag if the boolean is True.
                if value:
                    args.append(f"--{field.name.replace('_', '-')}")
            else:
                args.append(f"--{field.name.replace('_', '-')}")
                args.append(str(value))
        return args

    def merge(self, other: "BaseFlags") -> "BaseFlags":
        # Use the type of self, so merging CopyFlags returns a CopyFlags instance.
        return _merge(type(self), self, other)

    def __repr__(self):
        return str(self.to_args())


@dataclass(repr=False)
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


@dataclass(repr=False)
class Flags(BaseFlags):
    copy: CopyFlags | None = None


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


if __name__ == "__main__":
    unit_test()
