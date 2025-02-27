import argparse
from dataclasses import dataclass
from pathlib import Path

from rclone_api import MultiUploadResult, Rclone

_1MB = 1024 * 1024


@dataclass
class Args:
    config_path: Path
    src: str
    dst: str
    chunk_size_mb: int
    read_concurrent_chunks: int
    retries: int
    save_state_json: Path


def list_files(rclone: Rclone, path: str):
    """List files in a remote path."""
    for dirlisting in rclone.walk(path):
        for file in dirlisting.files:
            print(file.path)


def _parse_args() -> Args:
    parser = argparse.ArgumentParser(description="List files in a remote path.")
    parser.add_argument("src", help="File to copy")
    parser.add_argument("dst", help="Destination file")
    parser.add_argument(
        "--config", help="Path to rclone config file", type=Path, required=True
    )
    parser.add_argument(
        "--chunk-size-mb", help="Chunk size in MB", type=int, default=256
    )
    parser.add_argument(
        "--read-concurrent-chunks",
        help="Maximum number of chunks to read",
        type=int,
        default=4,
    )
    parser.add_argument("--retries", help="Number of retries", type=int, default=3)
    parser.add_argument(
        "--resumable-json",
        help="Path to resumable JSON file",
        type=Path,
        default="resume.json",
    )

    args = parser.parse_args()
    out = Args(
        config_path=Path(args.config),
        src=args.src,
        dst=args.dst,
        chunk_size_mb=args.chunk_size_mb,
        read_concurrent_chunks=args.read_concurrent_chunks,
        retries=args.retries,
        save_state_json=args.resumable_json,
    )
    return out


def main() -> int:
    """Main entry point."""
    args = _parse_args()
    rclone = Rclone(rclone_conf=args.config_path)
    rslt: MultiUploadResult = rclone.copy_file_resumable_s3(
        src=args.src,
        dst=args.dst,
        chunk_size=args.chunk_size_mb * _1MB,
        concurrent_chunks=args.read_concurrent_chunks,
        retries=args.retries,
        save_state_json=args.save_state_json,
    )
    print(rslt)
    return 0
