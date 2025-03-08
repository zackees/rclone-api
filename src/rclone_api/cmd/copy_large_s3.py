import argparse
from dataclasses import dataclass
from pathlib import Path

from rclone_api import Rclone, SizeSuffix


@dataclass
class Args:
    config_path: Path
    src: str
    dst: str
    chunk_size: SizeSuffix
    read_threads: int
    write_threads: int
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
        "--config", help="Path to rclone config file", type=Path, required=False
    )
    parser.add_argument(
        "--chunk-size",
        help="Chunk size that will be read and uploaded in SizeSuffix form, too low or too high will cause issues",
        type=str,
        default="128MB",  # if this is too low or too high an s3 service
    )
    parser.add_argument(
        "--read-threads",
        help="Number of concurrent read threads per chunk, only one chunk will be read at a time",
        type=int,
        default=8,
    )
    parser.add_argument(
        "--write-threads",
        help="Max number of chunks to upload in parallel to the destination, each chunk is uploaded in a separate thread",
        type=int,
        default=16,
    )
    parser.add_argument("--retries", help="Number of retries", type=int, default=3)
    parser.add_argument(
        "--resume-json",
        help="Path to resumable JSON file",
        type=Path,
        default="resume.json",
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
        chunk_size=SizeSuffix(args.chunk_size),
        read_threads=args.read_threads,
        write_threads=args.write_threads,
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
    # rslt: MultiUploadResult = rclone.copy_file_resumable_s3(
    #     src=args.src,
    #     dst=args.dst,
    #     chunk_size=args.chunk_size,
    #     read_threads=args.read_threads,
    #     write_threads=args.write_threads,
    #     retries=args.retries,
    #     save_state_json=args.save_state_json,
    #     verbose=args.verbose,
    # )
    err: Exception | None = rclone.copy_file_parts(
        src=args.src,
        dst_dir=args.dst,
        # verbose=args.verbose,
    )
    if err is not None:
        print(f"Error: {err}")
        raise err
    return 0


if __name__ == "__main__":
    import sys

    # here = Path(__file__).parent
    # project_root = here.parent.parent.parent
    # print(f"project_root: {project_root}")
    # os.chdir(str(project_root))
    # cwd = Path(__file__).parent
    # print(f"cwd: {cwd}")
    sys.argv.append("--config")
    sys.argv.append("rclone.conf")
    sys.argv.append(
        "45061:aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst"
    )
    sys.argv.append(
        "dst:TorrentBooks/aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst"
    )
    main()
