import argparse
from dataclasses import dataclass
from pathlib import Path

from rclone_api import Rclone


@dataclass
class Args:
    config: Path
    path: str

    def __post_init__(self):
        if not self.config.exists():
            raise FileNotFoundError(f"Config file not found: {self.config}")


def list_files(rclone: Rclone, path: str):
    """List files in a remote path."""

    with rclone.ls_stream(path, fast_list=True) as files:
        for file_item in files:
            print(file_item.path, "", file_item.size, file_item.mod_time)


def _parse_args() -> Args:
    parser = argparse.ArgumentParser(description="List files in a remote path.")
    parser.add_argument(
        "--config", help="Path to rclone config file", type=Path, default="rclone.conf"
    )
    parser.add_argument("path", help="Remote path to list")
    tmp = parser.parse_args()
    return Args(config=tmp.config, path=tmp.path)


def main() -> int:
    """Main entry point."""
    args = _parse_args()
    path = args.path
    rclone = Rclone(Path(args.config))
    list_files(rclone, path)
    return 0


if __name__ == "__main__":
    import sys

    cwd = Path(".").absolute()
    print(f"cwd: {cwd}")
    sys.argv.append("dst:TorrentBooks")
    main()
