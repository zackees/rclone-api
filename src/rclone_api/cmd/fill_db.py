import argparse
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from rclone_api import Rclone
from rclone_api.db import DB

load_dotenv()


# DB_URL = "sqlite:///data.db"

os.environ["DB_URL"] = "sqlite:///data.db"


@dataclass
class Args:
    config: Path
    path: str

    def __post_init__(self):
        if not self.config.exists():
            raise FileNotFoundError(f"Config file not found: {self.config}")


#
# method) def ls_stream_files_paged(
#     path: str,
#     max_depth: int = -1,
#     fast_list: bool = False,
#     page_size: int = 1000
# ) -> Generator[list[FileItem], None, None]
#


def list_files(rclone: Rclone, path: str):
    """List files in a remote path."""
    db = DB()

    # with closing(
    #     rclone.ls_stream_files_paged(path, fast_list=True, page_size=100)
    # ) as pages:
    #     for files_page in pages:
    #         # print(file_item.path, "", file_item.size, file_item.mod_time)
    #         # for file_item in files_page:
    #         # print(file_item.path, "", file_item.size, file_item.mod_time)
    #         db.add_files(files_page)
    #         break

    with rclone.ls_stream(path, fast_list=True) as stream:
        for page in stream.files_paged(page_size=100):
            for file_item in page:
                print(file_item.path, "", file_item.size, file_item.mod_time)
                db.add_files([file_item])

    # now query
    print("Querying")
    files = db.query_files(path)
    for file in files:
        print(file.path, "", file.size, file.mod_time)
    print()


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
