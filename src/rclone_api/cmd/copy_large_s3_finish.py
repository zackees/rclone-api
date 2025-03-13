import argparse
from dataclasses import dataclass
from pathlib import Path

from rclone_api import Rclone
from rclone_api.s3.multipart.upload_parts_server_side_merge import (
    s3_server_side_multi_part_merge,
)


@dataclass
class Args:
    config_path: Path
    src: str  # like dst:TorrentBooks/aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst-parts/ (info.json will be located here)
    verbose: bool

    def __repr__(self):
        return f"Args(config_path={self.config_path}, src={self.src}, verbose={self.verbose})"

    def __str__(self):
        return repr(self)


def list_files(rclone: Rclone, path: str):
    """List files in a remote path."""
    for dirlisting in rclone.walk(path):
        for file in dirlisting.files:
            print(file.path)


def _parse_args() -> Args:
    parser = argparse.ArgumentParser(description="List files in a remote path.")
    parser.add_argument("src", help="Directory that holds the info.json file")
    parser.add_argument("--no-verbose", help="Verbose output", action="store_true")
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
        verbose=not args.no_verbose,
    )
    return out


def _get_info_path(src: str) -> str:
    if src.endswith("/"):
        src = src[:-1]
    info_path = f"{src}/info.json"
    return info_path


def main() -> int:
    """Main entry point."""
    print("Starting...")
    args = _parse_args()
    print(f"args: {args}")
    rclone = Rclone(rclone_conf=args.config_path)
    info_path = _get_info_path(src=args.src)
    s3_server_side_multi_part_merge(
        rclone=rclone.impl, info_path=info_path, max_workers=5, verbose=args.verbose
    )
    return 0


if __name__ == "__main__":
    import sys

    sys.argv.append("--config")
    sys.argv.append("rclone.conf")
    sys.argv.append(
        "dst:TorrentBooks/aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst-parts/"
    )
    main()
