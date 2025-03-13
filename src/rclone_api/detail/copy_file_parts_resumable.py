from rclone_api.rclone_impl import RcloneImpl
from rclone_api.types import (
    PartInfo,
)


def copy_file_parts_resumable(
    self: RcloneImpl,
    src: str,  # src:/Bucket/path/myfile.large.zst
    dst_dir: str,  # dst:/Bucket/path/myfile.large.zst-parts/
    part_infos: list[PartInfo] | None = None,
    upload_threads: int = 10,
    merge_threads: int = 5,
    verbose: bool | None = None,
) -> Exception | None:
    # _upload_parts
    from rclone_api.s3.multipart.upload_parts_resumable import upload_parts_resumable
    from rclone_api.s3.multipart.upload_parts_server_side_merge import (
        s3_server_side_multi_part_merge,
    )

    if verbose is None:
        verbose = self.get_verbose()

    err: Exception | None = upload_parts_resumable(
        self=self,
        src=src,
        dst_dir=dst_dir,
        part_infos=part_infos,
        threads=upload_threads,
    )
    if isinstance(err, Exception):
        return err
    if dst_dir.endswith("/"):
        dst_dir = dst_dir[:-1]
    dst_info = f"{dst_dir}/info.json"
    err = s3_server_side_multi_part_merge(
        rclone=self, info_path=dst_info, max_workers=merge_threads, verbose=verbose
    )
    if isinstance(err, Exception):
        return err
    return None
