import argparse
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from rclone_api import Rclone

# load_dotenv()


# DB_URL = "sqlite:///data.db"

# os.environ["DB_URL"] = "sqlite:///data.db"


def _db_url_from_env_or_raise() -> str:
    load_dotenv(Path(".env"))
    db_url = os.getenv("DB_URL")
    if db_url is None:
        raise ValueError("DB_URL not set")
    return db_url


@dataclass
class Args:
    config: Path
    path: str
    db_url: str

    def __post_init__(self):
        if not self.config.exists():
            raise FileNotFoundError(f"Config file not found: {self.config}")


def fill_db(rclone: Rclone, path: str):
    """List files in a remote path."""
    # db = DB(_db_url_from_env_or_raise())
    db_url = _db_url_from_env_or_raise()
    rclone.save_to_db(src=path, db_url=db_url, fast_list=True)


def _parse_args() -> Args:
    parser = argparse.ArgumentParser(description="List files in a remote path.")
    parser.add_argument(
        "--config", help="Path to rclone config file", type=Path, default="rclone.conf"
    )
    parser.add_argument("--db", help="Database URL", type=str, default=None)
    parser.add_argument("path", help="Remote path to list")
    tmp = parser.parse_args()
    return Args(
        config=tmp.config,
        path=tmp.path,
        db_url=tmp.db if tmp.db is not None else _db_url_from_env_or_raise(),
    )


def main() -> int:
    """Main entry point."""
    args = _parse_args()
    path = args.path
    rclone = Rclone(Path(args.config))
    fill_db(rclone, path)
    return 0


if __name__ == "__main__":
    import sys

    cwd = Path(".").absolute()
    print(f"cwd: {cwd}")
    sys.argv.append("dst:TorrentBooks/meta")
    main()
