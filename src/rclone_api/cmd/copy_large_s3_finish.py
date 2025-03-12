import argparse
import os
from dataclasses import dataclass
from pathlib import Path

from rclone_api import Rclone
from rclone_api.detail.copy_file_parts import InfoJson
from rclone_api.s3.s3_multipart_uploader_by_copy import (
    Part,
    S3MultiPartUploader,
)

_TIMEOUT_READ = 900
_TIMEOUT_CONNECTION = 900
_MAX_WORKERS = 1  # Back blaze get's overwhelmed with 10, so I set it to 1 to be safe.


@dataclass
class Args:
    config_path: Path
    src: str  # like dst:TorrentBooks/aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst-parts/ (info.json will be located here)
    dst: str  # like dst:TorrentBooks/aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst
    verbose: bool


def list_files(rclone: Rclone, path: str):
    """List files in a remote path."""
    for dirlisting in rclone.walk(path):
        for file in dirlisting.files:
            print(file.path)


def _parse_args() -> Args:
    parser = argparse.ArgumentParser(description="List files in a remote path.")
    parser.add_argument("src", help="File to copy")
    parser.add_argument("dst", help="Destination file")
    parser.add_argument("-v", "--verbose", help="Verbose output", action="store_true")
    parser.add_argument(
        "--config", help="Path to rclone config file", type=Path, required=False
    )
    args = parser.parse_args()
    config: Path | None = args.config
    if config is None:
        config = Path("rclone.conf")
        if not config.exists():
            raise FileNotFoundError(f"Config file not found: {config}")
    assert config is not None
    out = Args(
        config_path=config,
        src=args.src,
        dst=args.dst,
        verbose=args.verbose,
    )
    return out


# def finish_multipart_upload_from_keys(
#     s3_client: BaseClient,
#     source_bucket: str,
#     parts: list[Part],
#     destination_bucket: str,
#     destination_key: str,
#     chunk_size: int,  # 5MB default
#     max_workers: int = 100,
#     retries: int = 3,
# ) -> str | Exception:
#     """
#     Finish a multipart upload by copying parts from existing S3 objects.

#     Args:
#         s3_client: Boto3 S3 client
#         source_bucket: Source bucket name
#         source_keys: List of source object keys to copy from
#         destination_bucket: Destination bucket name
#         destination_key: Destination object key
#         chunk_size: Size of each part in bytes
#         retries: Number of retry attempts
#         byte_ranges: Optional list of byte ranges corresponding to source_keys

#     Returns:
#         The URL of the completed object
#     """

#     # Create upload info
#     info = begin_upload(
#         s3_client=s3_client,
#         parts=parts,
#         destination_bucket=destination_bucket,
#         destination_key=destination_key,
#         chunk_size=chunk_size,
#         retries=retries,
#     )

#     out = do_body_work(
#         info=info,
#         source_bucket=source_bucket,
#         parts=parts,
#         max_workers=max_workers,
#         retries=retries,
#     )

#     return out


def do_finish_part(rclone: Rclone, info: InfoJson, dst: str) -> Exception | None:
    from rclone_api.s3.create import (
        BaseClient,
        S3Config,
        S3Credentials,
        create_s3_client,
    )

    s3_config = S3Config(
        verbose=False,
        timeout_read=_TIMEOUT_READ,
        timeout_connection=_TIMEOUT_CONNECTION,
    )

    s3_creds: S3Credentials = rclone.impl.get_s3_credentials(remote=dst)
    s3_client: BaseClient = create_s3_client(s3_creds=s3_creds, s3_config=s3_config)
    s3_bucket = s3_creds.bucket_name
    is_done = info.fetch_is_done()
    size = info.size
    assert is_done, f"Upload is not done: {info}"

    parts_dir = info.parts_dir
    if parts_dir.endswith("/"):
        parts_dir = parts_dir[:-1]
    source_keys = info.fetch_all_finished()
    # print(parts_dir)
    # print(source_keys)

    parts_path = parts_dir.split(s3_bucket)[1]
    if parts_path.startswith("/"):
        parts_path = parts_path[1:]

    first_part: int | None = info.first_part
    last_part: int | None = info.last_part

    assert first_part is not None
    assert last_part is not None

    def _to_s3_key(name: str | None) -> str:
        if name:
            out = f"{parts_path}/{name}"
            return out
        out = f"{parts_path}"
        return out

    parts: list[Part] = []
    part_num = 1
    for part_key in source_keys:
        s3_key = _to_s3_key(name=part_key)
        part = Part(part_number=part_num, s3_key=s3_key)
        parts.append(part)
        part_num += 1

    chunksize = info.chunksize
    assert chunksize is not None

    dst_name = info.dst_name
    dst_dir = os.path.dirname(parts_path)
    # dst_key =
    dst_key = f"{dst_dir}/{dst_name}"

    uploader: S3MultiPartUploader = S3MultiPartUploader(
        s3_client=s3_client,
        verbose=True,
    )

    from rclone_api.s3.merge_state import MergeState
    from rclone_api.s3.s3_multipart_uploader_by_copy import MultipartUploadInfo

    info_multipart: MultipartUploadInfo
    merge_state: MergeState

    info_multipart, merge_state = uploader.begin_new_upload(
        parts=parts,
        destination_bucket=s3_creds.bucket_name,
        destination_key=dst_key,
        chunk_size=chunksize.as_int(),
    )

    uploader.start_upload(
        info=info_multipart, state=merge_state, max_workers=_MAX_WORKERS
    )

    # now check if the dst now exists, if so, delete the parts folder.
    # if rclone.exists(dst):
    #     rclone.purge(parts_dir)

    if not rclone.exists(dst):
        return FileNotFoundError(f"Destination file not found: {dst}")

    write_size = rclone.size_file(dst)
    if write_size != size:
        return ValueError(f"Size mismatch: {write_size} != {size}")

    print(f"Upload complete: {dst}")
    rclone.purge(parts_dir)
    return None


def main() -> int:
    """Main entry point."""
    args = _parse_args()
    rclone = Rclone(rclone_conf=args.config_path)
    info_json = f"{args.src}/info.json".replace("//", "/")
    info = InfoJson(rclone.impl, src=None, src_info=info_json)
    loaded = info.load()
    if not loaded:
        raise FileNotFoundError(
            f"Info file not found, has the upload finished? {info_json}"
        )
    print(info)
    do_finish_part(rclone=rclone, info=info, dst=args.dst)
    return 0


if __name__ == "__main__":
    import sys

    sys.argv.append("--config")
    sys.argv.append("rclone.conf")
    sys.argv.append(
        "dst:TorrentBooks/aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst-parts/"
    )
    sys.argv.append(
        "dst:TorrentBooks/aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst"
    )
    main()
