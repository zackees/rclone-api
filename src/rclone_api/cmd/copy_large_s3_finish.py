import argparse
import os
from dataclasses import dataclass
from pathlib import Path

from rclone_api import Rclone
from rclone_api.detail.copy_file_parts import InfoJson
from rclone_api.s3.s3_multipart_uploader_by_copy import (
    DEFAULT_MAX_WORKERS,
    Part,
    S3MultiPartMerger,
)

_TIMEOUT_READ = 900
_TIMEOUT_CONNECTION = 900


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


def _begin_new_merge(
    rclone: Rclone, info: InfoJson, dst: str
) -> S3MultiPartMerger | Exception:
    from rclone_api.s3.create import (
        BaseClient,
        S3Config,
        S3Credentials,
        create_s3_client,
    )

    try:
        s3_config = S3Config(
            verbose=False,
            timeout_read=_TIMEOUT_READ,
            timeout_connection=_TIMEOUT_CONNECTION,
        )
        s3_creds: S3Credentials = rclone.impl.get_s3_credentials(remote=dst)
        s3_client: BaseClient = create_s3_client(s3_creds=s3_creds, s3_config=s3_config)
        s3_bucket = s3_creds.bucket_name
        is_done = info.fetch_is_done()
        assert is_done, f"Upload is not done: {info}"

        parts_dir = info.parts_dir
        source_keys = info.fetch_all_finished()

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
        part_num = first_part
        for part_key in source_keys:
            assert part_num <= last_part and part_num >= first_part
            s3_key = _to_s3_key(name=part_key)
            part = Part(part_number=part_num, s3_key=s3_key)
            parts.append(part)
            part_num += 1

        dst_name = info.dst_name
        dst_dir = os.path.dirname(parts_path)
        dst_key = f"{dst_dir}/{dst_name}"

        merger: S3MultiPartMerger = S3MultiPartMerger(
            s3_client=s3_client,
            verbose=True,
        )
        err = merger.begin_new_merge(
            parts=parts,
            bucket=s3_creds.bucket_name,
            dst_key=dst_key,
        )
        if isinstance(err, Exception):
            return err
        return merger
    except Exception as e:
        return e


def _finish_merge(rclone: Rclone, info: InfoJson, dst: str) -> Exception | None:
    size = info.size
    parts_dir = info.parts_dir
    if not rclone.exists(dst):
        return FileNotFoundError(f"Destination file not found: {dst}")

    write_size = rclone.size_file(dst)
    if write_size != size:
        return ValueError(f"Size mismatch: {write_size} != {size}")

    print(f"Upload complete: {dst}")
    rclone.purge(parts_dir)
    return None


def perform_merge(rclone: Rclone, info: InfoJson, dst: str) -> Exception | None:
    size = info.size
    parts_dir = info.parts_dir
    print(f"Finishing upload: {dst}")
    print(f"Parts dir: {parts_dir}")
    print(f"Size: {size}")
    merger: S3MultiPartMerger | Exception = _begin_new_merge(
        rclone=rclone, info=info, dst=dst
    )
    if isinstance(merger, Exception):
        return merger

    err = merger.merge(max_workers=DEFAULT_MAX_WORKERS)
    if isinstance(err, Exception):
        return err

    err = _finish_merge(rclone=rclone, info=info, dst=dst)
    return err


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
    perform_merge(rclone=rclone, info=info, dst=args.dst)
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
