"""
Unit test file.
"""

import argparse
import os
import shutil
import time
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path

import psutil
from dotenv import load_dotenv

from rclone_api import Config, Rclone, SizeSuffix
from rclone_api.mount_read_chunker import MultiMountFileChunker
from rclone_api.types import FilePart

os.environ["RCLONE_API_VERBOSE"] = "1"


@dataclass
class Args:
    direct_io: bool
    num: int
    size: SizeSuffix | None


@dataclass
class Credentials:
    BUCKET_KEY_SECRET: str
    BUCKET_KEY_PUBLIC: str
    BUCKET_NAME: str
    SRC_SFTP_HOST: str
    SRC_SFTP_USER: str
    SRC_SFTP_PORT: str
    SRC_SFTP_PASS: str
    BUCKET_URL: str


def _generate_rclone_config() -> tuple[Config, Credentials]:

    cwd = Path.cwd()
    env_path = cwd / ".env"
    assert (
        env_path.exists()
    ), "this test requires that the secret .env file exists with the credentials"
    load_dotenv(env_path, verbose=True)
    print(f"Current working directory: {cwd}")

    # assert that .env exists for this test
    assert os.path.exists(
        ".env"
    ), "this test requires that the secret .env file exists with the credentials"

    # BUCKET_NAME = os.getenv("BUCKET_NAME", "TorrentBooks")  # Default if not in .env

    # Load additional environment variables
    BUCKET_KEY_SECRET = os.getenv("BUCKET_KEY_SECRET")
    BUCKET_KEY_PUBLIC = os.getenv("BUCKET_KEY_PUBLIC")
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    SRC_SFTP_HOST = os.getenv("SRC_SFTP_HOST")
    SRC_SFTP_USER = os.getenv("SRC_SFTP_USER")
    SRC_SFTP_PORT = os.getenv("SRC_SFTP_PORT")
    SRC_SFTP_PASS = os.getenv("SRC_SFTP_PASS")
    # BUCKET_URL = os.getenv("BUCKET_URL")
    BUCKET_URL = "sfo3.digitaloceanspaces.com"

    config_text = f"""
[dst]
type = s3
provider = DigitalOcean
access_key_id = {BUCKET_KEY_PUBLIC}
secret_access_key = {BUCKET_KEY_SECRET}
endpoint = {BUCKET_URL}
bucket = {BUCKET_NAME}

[src]
type = sftp
host = {SRC_SFTP_HOST}
user = {SRC_SFTP_USER}
port = {SRC_SFTP_PORT}
pass = {SRC_SFTP_PASS}

"""
    # print("Config text:")
    # print(config_text)
    # _CONFIG_PATH.write_text(config_text, encoding="utf-8")
    # print(f"Config file written to: {_CONFIG_PATH}")

    creds = Credentials(
        BUCKET_KEY_SECRET=str(BUCKET_KEY_SECRET),
        BUCKET_KEY_PUBLIC=str(BUCKET_KEY_PUBLIC),
        BUCKET_NAME=str(BUCKET_NAME),
        SRC_SFTP_HOST=str(SRC_SFTP_HOST),
        SRC_SFTP_USER=str(SRC_SFTP_USER),
        SRC_SFTP_PORT=str(SRC_SFTP_PORT),
        SRC_SFTP_PASS=str(SRC_SFTP_PASS),
        BUCKET_URL=str(BUCKET_URL),
    )

    return Config(config_text), creds


def _run_profile(
    rclone: Rclone,
    src_file: str,
    transfers: int,
    offset: SizeSuffix,
    size: SizeSuffix,
    log_dir: Path,
    num: int,
    direct_io: bool,
) -> None:

    mount_log = log_dir / f"mount_{SizeSuffix(size)}_threads_{transfers}.log"
    print("\n\n")
    print("#" * 80)
    print(f"# Started test download of {SizeSuffix(size)} with {transfers} transfers")
    print("#" * 80)

    chunk_size = size

    filechunker: MultiMountFileChunker = rclone.get_multi_mount_file_chunker(
        src=src_file,
        chunk_size=chunk_size,
        threads=transfers,
        direct_io=direct_io,
        mount_log=mount_log,
    )
    bytes_count = 0

    futures: list[Future[FilePart]] = []
    for i in range(num):
        offset = SizeSuffix(i * chunk_size.as_int()) + offset
        future = filechunker.fetch(offset.as_int(), size.as_int(), "TEST OBJECT")
        futures.append(future)

    # dry run to warm up the mounts, then read a different byte range.
    for future in futures:
        filepart_or_err = future.result()
        if isinstance(filepart_or_err, Exception):
            assert False, f"Error: {filepart_or_err}"
        filepart_or_err.dispose()
    futures.clear()

    start = time.time()
    net_io_start = psutil.net_io_counters()

    offset = SizeSuffix("1G")

    for i in range(num):
        offset = SizeSuffix(i * chunk_size.as_int()) + offset
        future = filechunker.fetch(offset.as_int(), size.as_int(), "TEST OBJECT")
        futures.append(future)

    for future in futures:
        bytes_or_err = future.result()
        if isinstance(bytes_or_err, Exception):
            assert False, f"Error: {bytes_or_err}"
        bytes_count += bytes_or_err.n_bytes()

    diff = (time.time() - start) / num
    net_io_end = psutil.net_io_counters()
    # self.assertEqual(len(bytes_or_err), size)
    # assert (
    #     bytes_count == SizeSuffix(size * num).as_int()
    # ), f"Length: {SizeSuffix(bytes_count)} != {SizeSuffix(size* num)}"

    if bytes_count != SizeSuffix(size * num).as_int():
        print("\n######################## ERROR ########################")
        print(f"Error: Length: {SizeSuffix(bytes_count)} != {SizeSuffix(size* num)}")
        print(f"  Bytes count: {bytes_count}")
        print(f"  Size: {SizeSuffix(size * num)}")
        print(f"  num: {num}")
        print("########################################################\n")

    # print io stats
    bytes_sent = (net_io_end.bytes_sent - net_io_start.bytes_sent) // num
    bytes_recv = (net_io_end.bytes_recv - net_io_start.bytes_recv) // num
    packets_sent = (net_io_end.packets_sent - net_io_start.packets_sent) // num
    efficiency = size.as_int() / (bytes_recv)
    efficiency_100 = efficiency * 100
    efficiency_str = f"{efficiency_100:.2f}"

    bytes_send_suffix = SizeSuffix(bytes_sent)
    bytes_recv_suffix = SizeSuffix(bytes_recv)
    range_size = SizeSuffix(size)

    print(f"\nFinished downloading {range_size} with {transfers} transfers")
    print("Net IO stats:")
    print(f"Bytes sent: {bytes_send_suffix}")
    print(f"Bytes received: {bytes_recv_suffix}")
    print(f"Packets sent: {packets_sent}")
    print(f"Efficiency: {efficiency_str}%")
    print(f"Time: {diff:.1f} seconds")


def test_profile_copy_bytes(
    args: Args,
    rclone: Rclone,
    offset: SizeSuffix,
    transfer_list: list[int] | None,
    mount_root_path: Path,
    size: SizeSuffix | None,
    num: int,
) -> None:

    if size:
        sizes = [size.as_int()]
    else:
        sizes = [
            1024 * 1024 * 1,
            # 1024 * 1024 * 2,
            1024 * 1024 * 4,
            # 1024 * 1024 * 8,
            1024 * 1024 * 16,
            # 1024 * 1024 * 32,
            1024 * 1024 * 64,
            # 1024 * 1024 * 128,
            1024 * 1024 * 256,
        ]
    # transfer_list = [1, 2, 4, 8, 16]
    transfer_list = transfer_list or [1, 2, 4]

    # src_file = "dst:rclone-api-unit-test/zachs_video/internaly_ai_alignment.mp4"
    # sftp mount
    src_file = "src:aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst"

    for sz in sizes:
        for transfers in transfer_list:
            _run_profile(
                rclone=rclone,
                src_file=src_file,
                transfers=transfers,
                offset=offset,
                size=SizeSuffix(sz),
                direct_io=args.direct_io,
                log_dir=mount_root_path,
                num=num,
            )
    print("done")


def _parse_args() -> Args:
    parser = argparse.ArgumentParser(description="Profile copy_bytes")
    parser.add_argument("--direct-io", help="Use direct IO", action="store_true")
    parser.add_argument("-n", "--num", help="Number of workers", type=int, default=1)
    parser.add_argument(
        "--size", help="Size of the file to download", type=SizeSuffix, default=None
    )
    args = parser.parse_args()
    return Args(direct_io=args.direct_io, num=args.num, size=args.size)


_SHOW_CREDS = False


def main() -> None:
    """Main entry point."""
    print("Running test_profile_copy_bytes")
    config, creds = _generate_rclone_config()
    if _SHOW_CREDS:
        print("Config:")
        print(config)
        print("Credentials:")
        print(creds)
    rclone = Rclone(config)

    mount_root_path = Path("rclone_logs") / "mount"
    if mount_root_path.exists():
        shutil.rmtree(mount_root_path)

    args = _parse_args()
    transfer_list = None
    # parallel_workers = args.num

    def task(
        offset: SizeSuffix,
        args=args,
        rclone=rclone,
        transfer_list=transfer_list,
        mount_root_path=mount_root_path,
    ):
        return test_profile_copy_bytes(
            args=args,
            rclone=rclone,
            offset=offset,
            mount_root_path=mount_root_path,
            transfer_list=transfer_list,
            size=args.size,
            num=args.num,
        )

    task(offset=SizeSuffix(0))

    # with ThreadPoolExecutor(max_workers=parallel_workers) as _:
    #     tasks: list[Future] = []
    #     for i in range(parallel_workers):
    #         offset = SizeSuffix(i * 1024 * 1024 * 256)
    #         future = ThreadPoolExecutor().submit(lambda: task(offset=offset))
    #         tasks.append(future)


if __name__ == "__main__":
    import sys

    sys.argv.append("--size")
    sys.argv.append("16MB")
    main()
