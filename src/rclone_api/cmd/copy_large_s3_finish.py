import argparse
import json
import os
import warnings
from dataclasses import dataclass
from pathlib import Path

from rclone_api import Rclone
from rclone_api.detail.copy_file_parts import InfoJson
from rclone_api.rclone_impl import RcloneImpl
from rclone_api.s3.create import (
    S3Credentials,
)
from rclone_api.s3.merge_state import MergeState, Part
from rclone_api.s3.s3_multipart_uploader_by_copy import (
    S3MultiPartMerger,
)


@dataclass
class Args:
    config_path: Path
    src: str  # like dst:TorrentBooks/aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst-parts/ (info.json will be located here)
    verbose: bool


def list_files(rclone: Rclone, path: str):
    """List files in a remote path."""
    for dirlisting in rclone.walk(path):
        for file in dirlisting.files:
            print(file.path)


def _parse_args() -> Args:
    parser = argparse.ArgumentParser(description="List files in a remote path.")
    parser.add_argument("src", help="Directory that holds the info.json file")
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
        verbose=args.verbose,
    )
    return out


def _begin_or_resume_merge(
    rclone: RcloneImpl, info: InfoJson
) -> S3MultiPartMerger | Exception:
    try:
        dst = info.dst
        s3_creds: S3Credentials = rclone.get_s3_credentials(remote=dst)
        merger: S3MultiPartMerger = S3MultiPartMerger(
            rclone_impl=rclone,
            info=info,
            s3_creds=s3_creds,
            verbose=True,
        )

        s3_bucket = s3_creds.bucket_name
        is_done = info.fetch_is_done()
        assert is_done, f"Upload is not done: {info}"

        merge_path = _get_merge_path(info_path=info.src_info)
        merge_json_text = rclone.read_text(merge_path)
        if isinstance(merge_json_text, str):
            # Attempt to do a resume
            merge_data = json.loads(merge_json_text)
            print(merge_data)
            merge_state = MergeState.from_json(rclone_impl=rclone, json=merge_data)
            if isinstance(merge_state, MergeState):
                merger.begin_resume_merge(merge_state=merge_state)
                return merger
            warnings.warn(f"Failed to resume merge: {merge_state}, starting new merge")

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

        err = merger.begin_new_merge(
            merge_path=merge_path,
            parts=parts,
            bucket=s3_creds.bucket_name,
            dst_key=dst_key,
        )
        if isinstance(err, Exception):
            return err
        return merger
    except Exception as e:
        return e


def _get_merge_path(info_path: str) -> str:
    par_dir = os.path.dirname(info_path)
    merge_path = f"{par_dir}/merge.json"
    return merge_path


def _perform_merge(rclone: RcloneImpl, info_path: str) -> Exception | None:
    merge_path = _get_merge_path(info_path)
    info = InfoJson(rclone, src=None, src_info=info_path)
    loaded = info.load()
    if not loaded:
        return FileNotFoundError(
            f"Info file not found, has the upload finished? {info_path}"
        )
    size = info.size
    parts_dir = info.parts_dir
    print(f"Finishing upload: {info.dst}")
    print(f"Parts dir: {parts_dir}")
    print(f"Size: {size}")
    print(f"Info: {info}")
    print(f"Merge.json: {merge_path}")
    merger: S3MultiPartMerger | Exception = _begin_or_resume_merge(
        rclone=rclone, info=info
    )
    if isinstance(merger, Exception):
        return merger

    err = merger.merge()
    if isinstance(err, Exception):
        return err

    err = merger.cleanup()
    if isinstance(err, Exception):
        err
    return None


def _get_info_path(src: str) -> str:
    if src.endswith("/"):
        src = src[:-1]
    info_path = f"{src}/info.json"
    return info_path


def main() -> int:
    """Main entry point."""
    args = _parse_args()
    rclone = Rclone(rclone_conf=args.config_path)
    info_path = _get_info_path(src=args.src)
    _perform_merge(rclone=rclone.impl, info_path=info_path)
    return 0


if __name__ == "__main__":
    import sys

    sys.argv.append("--config")
    sys.argv.append("rclone.conf")
    sys.argv.append(
        "dst:TorrentBooks/aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst-parts/"
    )
    main()
