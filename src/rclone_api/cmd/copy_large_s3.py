import argparse
from dataclasses import dataclass
from pathlib import Path

from rclone_api import MultiUploadResult, Rclone, SizeSuffix

_1MB = 1024 * 1024


@dataclass
class Args:
    config_path: Path
    src: str
    dst: str
    chunk_size: SizeSuffix
    threads: int
    retries: int
    save_state_json: Path
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
        "--config", help="Path to rclone config file", type=Path, required=True
    )
    parser.add_argument(
        "--chunk-size",
        help="Chunk size that will be read and uploaded in in SizeSuffix (i.e. 128M = 128 megabytes) form",
        type=str,
        default="1G",
    )
    parser.add_argument(
        "--threads",
        help="Number of threads to use per chunk",
        type=int,
        default=64,
    )
    parser.add_argument("--retries", help="Number of retries", type=int, default=3)
    parser.add_argument(
        "--resume-json",
        help="Path to resumable JSON file",
        type=Path,
        default="resume.json",
    )

    args = parser.parse_args()
    out = Args(
        config_path=Path(args.config),
        src=args.src,
        dst=args.dst,
        chunk_size=SizeSuffix(args.chunk_size),
        threads=args.threads,
        retries=args.retries,
        save_state_json=args.resume_json,
        verbose=args.verbose,
    )
    return out


def main() -> int:
    """Main entry point."""
    args = _parse_args()
    rclone = Rclone(rclone_conf=args.config_path)
    # unit_chunk = args.chunk_size / args.threads
    rslt: MultiUploadResult = rclone.copy_file_resumable_s3(
        src=args.src,
        dst=args.dst,
        chunk_size=args.chunk_size,
        threads=args.threads,
        # vfs_read_chunk_size=unit_chunk,
        # vfs_read_chunk_size_limit=args.chunk_size,
        # vfs_read_chunk_streams=args.threads,
        retries=args.retries,
        save_state_json=args.save_state_json,
        verbose=args.verbose,
    )
    print(rslt)
    return 0


if __name__ == "__main__":
    import os
    import sys

    here = Path(__file__).parent
    project_root = here.parent.parent.parent
    print(f"project_root: {project_root}")
    os.chdir(str(project_root))
    cwd = Path(__file__).parent
    print(f"cwd: {cwd}")
    sys.argv.append("--config")
    sys.argv.append("rclone.conf")
    sys.argv.append(
        "45061:aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst.torrent"
    )
    sys.argv.append(
        "dst:TorrentBooks/aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst.torrent"
    )
    main()
