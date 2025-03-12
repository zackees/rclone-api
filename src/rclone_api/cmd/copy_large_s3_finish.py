import argparse
from dataclasses import dataclass
from pathlib import Path

from rclone_api import Rclone
from rclone_api.detail.copy_file_parts import InfoJson
from rclone_api.types import (
    PartInfo,
)

DATA_SOURCE = (
    "dst:TorrentBooks/aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst"
)


# response = client.upload_part_copy(
#     Bucket='string',
#     CopySource='string' or {'Bucket': 'string', 'Key': 'string', 'VersionId': 'string'},
#     CopySourceIfMatch='string',
#     CopySourceIfModifiedSince=datetime(2015, 1, 1),
#     CopySourceIfNoneMatch='string',
#     CopySourceIfUnmodifiedSince=datetime(2015, 1, 1),
#     CopySourceRange='string',
#     Key='string',
#     PartNumber=123,
#     UploadId='string',
#     SSECustomerAlgorithm='string',
#     SSECustomerKey='string',
#     CopySourceSSECustomerAlgorithm='string',
#     CopySourceSSECustomerKey='string',
#     RequestPayer='requester',
#     ExpectedBucketOwner='string',
#     ExpectedSourceBucketOwner='string'
# )


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
    parser.add_argument(
        "--chunk-size",
        help="Chunk size that will be read and uploaded in SizeSuffix form, too low or too high will cause issues",
        type=str,
        default="128MB",  # if this is too low or too high an s3 service
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


# from dataclasses import dataclass

# def parse_info_json(text: str) -> UploadInfo:
#     import json
#     data = json.loads(text)
#     chunk_size = data["chunksize_int"]
#     first_part = data["first_part"]
#     last_part = data["last_part"]
#     assert isinstance(chunk_size, int)
#     assert isinstance(first_part, int)
#     assert isinstance(last_part, int)
#     assert first_part <= last_part
#     parts: list[str] = []
#     fmt = "part.{:05d}_{}-{}"
#     for i in range(first_part, last_part + 1):
#         offset: int = i * chunk_size
#         end: int = (i + 1) * chunk_size
#         part = fmt.format(i, offset, end)
#         parts.append(part)
#     return UploadInfo(chunk_size=chunk_size, parts=parts)


def do_finish_part(info: InfoJson) -> None:
    all_parts: list[PartInfo] | Exception = info.compute_all_parts()
    print(all_parts)


def main() -> int:
    """Main entry point."""
    args = _parse_args()
    rclone = Rclone(rclone_conf=args.config_path)
    info_json = f"{args.src}/info.json".replace("//", "/")
    info = InfoJson(rclone.impl, src=None, src_info=info_json)
    loaded = info.load()
    assert loaded
    print(info)
    do_finish_part(info)
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
